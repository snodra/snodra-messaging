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
import io.damoreira.kraken.messaging.security.ForwardedJwtAuthenticator
import io.damoreira.kraken.messaging.security.KrakenForwardedAuthentication
import io.damoreira.kraken.messaging.security.SecurityContextElement
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.security.core.context.SecurityContextHolder

/**
 * Handles a [Request] the hub forwarded to this instance: validates the forwarded JWT, builds the [MessageContext] and
 * calls the local command or query handler. Every outcome, including a rejected token, becomes a [Reply].
 *
 * While the handler runs, the Spring [org.springframework.security.core.context.SecurityContext] holds a
 * [KrakenForwardedAuthentication] (or no authentication for an anonymous caller), carried across suspensions and
 * thread switches by a coroutine context element, so nested sends from the handler re-forward the same token.
 * Threads the handler starts itself do not inherit it; such code should read [MessageContext] instead.
 */
class InboundRequestDispatcher(
    private val invoker: HandlerInvoker,
    private val authenticator: ForwardedJwtAuthenticator,
    @Suppress("unused") private val codec: JsonCodec,
) {
    private val log = LoggerFactory.getLogger(InboundRequestDispatcher::class.java)

    suspend fun dispatch(req: Request): Reply {
        val headers = req.headersMap.toMap()
        val authorization = headers["authorization"]
        val principal = try {
            authenticator.authenticate(authorization)
        } catch (e: UnauthenticatedException) {
            log.debug("Rejected {} request {}: {}", req.type, req.id, e.message)
            return errorReply(req.id, ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated", ByteArray(0))
        }
        val ctx = MessageContext(
            messageId = req.id,
            type = req.type,
            correlationId = headers["correlationId"] ?: req.id,
            userId = principal.userId,
            roles = principal.roles,
            authorization = authorization,
            headers = headers,
        )
        val securityContext = SecurityContextHolder.createEmptyContext().apply {
            val token = authorization?.let(ForwardedJwtAuthenticator::bearerToken)
            if (token != null) authentication = KrakenForwardedAuthentication(token, principal)
        }
        val payload = req.payload.toByteArray()
        val result = withContext(SecurityContextElement(securityContext)) {
            when (req.kind) {
                RequestKind.QUERY -> invoker.invokeQuery(req.type, payload, ctx)
                RequestKind.COMMAND -> invoker.invokeCommand(req.type, payload, ctx)
                else -> InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "unsupported request kind ${req.kind}")
            }
        }
        return when (result) {
            is InvocationResult.Success ->
                Reply.newBuilder().setRequestId(req.id).setPayload(ByteString.copyFrom(result.payload)).build()
            is InvocationResult.Failure -> errorReply(req.id, result.code, result.message, result.details)
        }
    }

    private fun errorReply(id: String, code: ErrorCode, message: String, details: ByteArray): Reply =
        Reply.newBuilder()
            .setRequestId(id)
            .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message).setDetails(ByteString.copyFrom(details)))
            .build()
}
