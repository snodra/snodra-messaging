package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.handler.SubscriptionAdmission
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.SecurityContextElement
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder

/**
 * Handles a [Request] the hub forwarded to this instance: validates the forwarded JWT, builds the [MessageContext] and
 * calls the local command or query handler. Every outcome, including a rejected token, becomes a [Reply].
 *
 * While the handler runs, the Spring [org.springframework.security.core.context.SecurityContext] holds a
 * [KrakenForwardedAuthentication] (or no authentication for an anonymous caller), carried across suspensions and
 * thread switches by a coroutine context element, so nested sends from the handler re-forward the same token.
 * Threads the handler starts itself do not inherit it; such code should read [MessageContext] instead. The request's
 * correlation id travels the same way ([CorrelationContext]), so messages sent while handling carry it.
 *
 * [dispatchStream] does the same for a streaming request (`mode = STREAMING`, P4b): the handler's flow is collected and
 * each element handed to the connection as a part; the outcome is null or the error that ends the stream.
 *
 * [dispatchSubscription] admits a subscription request (`mode = SUBSCRIPTION`, P4c); the initial result of a subscription
 * is an ordinary unary request (its id ends in `#0`) answered by [dispatch].
 */
class InboundRequestDispatcher(
    private val invoker: HandlerInvoker,
    private val identities: ForwardedIdentityResolver,
    @Suppress("unused") private val codec: JsonCodec,
) {
    private val log = LoggerFactory.getLogger(InboundRequestDispatcher::class.java)

    suspend fun dispatch(req: Request): Reply {
        val result = when (val caller = callerOf(req)) {
            is Caller.Rejected -> caller.failure
            is Caller.Resolved -> caller.run(req) { payload, ctx, identity ->
                when (req.kind) {
                    // `<id>#0` is the initial request of a subscription (P4c): the hub's id scheme, the only marker it leaves.
                    RequestKind.QUERY -> invoker.invokeQuery(req.type, payload, ctx, identity, subscriptionInitial = req.id.endsWith("#0"))
                    RequestKind.COMMAND -> invoker.invokeCommand(req.type, payload, ctx, identity)
                    else -> InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "unsupported request kind ${req.kind}")
                }
            }
        }
        return when (result) {
            is InvocationResult.Success ->
                Reply.newBuilder().setRequestId(req.id).setPayload(ByteString.copyFrom(result.payload)).build()
            is InvocationResult.Failure -> errorReply(req.id, result.code, result.message, result.details)
        }
    }

    /**
     * Serves a streaming request (see [StreamingInbound]): the handler's flow runs under the same security and
     * correlation context as a unary handler, each element goes to [emit] as a part, and the result is null (the
     * flow completed) or the `Error` that ends the stream.
     */
    suspend fun dispatchStream(req: Request, emit: suspend (ByteArray) -> Unit): Error? {
        val failure = when (val caller = callerOf(req)) {
            is Caller.Rejected -> caller.failure
            is Caller.Resolved -> caller.run(req) { payload, ctx, identity -> invoker.invokeStream(req.type, payload, ctx, identity, emit) }
        }
        return failure?.let {
            Error.newBuilder().setRequestId(req.id).setCode(it.code).setMessage(it.message).setDetails(ByteString.copyFrom(it.details)).build()
        }
    }

    /**
     * Admits a subscription request (`mode = SUBSCRIPTION`, P4c, see [SubscriptionInbound]): the forwarded token is
     * validated and the handler's pre-checks run as for a unary request; the decoded query goes to [register], whose
     * error (the instance's subscription limit) or null is the result. Nothing is invoked: the initial result is answered
     * by the separate `<id>#0` request.
     */
    suspend fun dispatchSubscription(req: Request, register: (query: Any) -> Error?): Error? {
        val failure = when (val caller = callerOf(req)) {
            is Caller.Rejected -> caller.failure
            is Caller.Resolved -> when (val admission = invoker.admitSubscription(req.type, req.payload.toByteArray(), caller.ctx, caller.identity)) {
                is SubscriptionAdmission.Rejected -> admission.failure
                is SubscriptionAdmission.Accepted -> return register(admission.query)
            }
        }
        return Error.newBuilder().setRequestId(req.id).setCode(failure.code).setMessage(failure.message).setDetails(ByteString.copyFrom(failure.details)).build()
    }

    private sealed interface Caller {
        class Rejected(val failure: InvocationResult.Failure) : Caller
        class Resolved(val ctx: MessageContext, val identity: Identity?, val securityContext: SecurityContext) : Caller {
            /** Runs [block] with the caller's identity in the Spring security context and its correlation id in the coroutine context. */
            suspend fun <T> run(req: Request, block: suspend (ByteArray, MessageContext, Identity?) -> T): T =
                withContext(SecurityContextElement(securityContext) + CorrelationContext(ctx.correlationId)) {
                    block(req.payload.toByteArray(), ctx, identity)
                }
        }
    }

    /** Validates the forwarded token and builds the [MessageContext] of [req]. */
    private fun callerOf(req: Request): Caller {
        val headers = req.headersMap.toMap()
        val authorization = headers["authorization"]
        val identity = try {
            identities.resolve(authorization)
        } catch (e: UnauthenticatedException) {
            log.debug("Rejected {} request {}: {}", req.type, req.id, e.message)
            return Caller.Rejected(InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated"))
        }
        val ctx = MessageContext(
            messageId = req.id,
            type = req.type,
            correlationId = headers["correlationId"] ?: req.id,
            userId = identity?.userId,
            roles = identity?.roles ?: emptySet(),
            authorization = authorization,
            headers = headers,
        )
        // The resolved identity (verified or trusted) is visible to the handler and re-forwarded by nested sends.
        val securityContext = SecurityContextHolder.createEmptyContext().apply {
            if (identity != null) authentication = identity.authentication
        }
        return Caller.Resolved(ctx, identity, securityContext)
    }

    private fun errorReply(id: String, code: ErrorCode, message: String, details: ByteArray): Reply =
        Reply.newBuilder()
            .setRequestId(id)
            .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message).setDetails(ByteString.copyFrom(details)))
            .build()
}
