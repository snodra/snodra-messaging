package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.ExternalCallFailedException
import io.damoreira.kraken.messaging.external.ExternalCallUnknownOutcomeException
import io.damoreira.kraken.messaging.external.ExternalFailureMarkers
import io.damoreira.kraken.messaging.external.IdempotencyInterceptor
import io.damoreira.kraken.messaging.external.RetryableExternalException
import io.damoreira.kraken.messaging.security.Identity
import jakarta.validation.ConstraintViolationException
import kotlinx.coroutines.flow.Flow
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.SmartInitializingSingleton
import kotlin.coroutines.cancellation.CancellationException

sealed interface InvocationResult {
    data class Success(val payload: ByteArray) : InvocationResult
    data class Failure(val code: ErrorCode, val message: String, val details: ByteArray = ByteArray(0)) : InvocationResult
}

/** The outcome of admitting a subscription (`mode = SUBSCRIPTION`, P4c) on this instance: the decoded query, or why not. */
sealed interface SubscriptionAdmission {
    /** The query decoded as the handler's message class; the subscription may be registered. */
    class Accepted(val query: Any) : SubscriptionAdmission

    /** Refused before anything is registered: the `ReplyEnd{error}` to send. */
    class Rejected(val failure: InvocationResult.Failure) : SubscriptionAdmission
}

/**
 * Decodes a payload, calls the registered handler and maps the outcome (or exception) to an [InvocationResult].
 *
 * `IllegalArgumentException` and `jakarta.validation.ConstraintViolationException` become `HANDLER_ERROR` with the plain
 * exception message and `details` = JSON array of violation messages (`"<propertyPath>: <message>"`, or just
 * `<message>` when the path is empty, sorted; `[]` when none). Other throwables become
 * `HANDLER_ERROR` with `"<SimpleName>: <message>"`. A handler's `@RequiresAuthentication` is checked before it runs
 * (`UNAUTHENTICATED` / `PERMISSION_DENIED`); a thrown `AccessDeniedException` (ours or Spring's) is `PERMISSION_DENIED`.
 * Stack traces are never included.
 *
 * A streaming query handler (`StreamingQuery<R>` -> `Flow<R>`, P4b) is run by [invokeStream]: every element is encoded
 * and handed to the caller as it is emitted (the caller suspends the collection while the hub granted no credit), and an
 * exception from the flow maps exactly like a unary handler's. The same handler reached by a unary request (mode
 * `UNARY`, [invokeQuery]) is `INVALID_REQUEST`: its flow cannot be a single reply.
 *
 * A subscription query handler (`SubscriptionQuery<I, U>` -> `I`, P4c) answers only the initial request of a
 * subscription ([invokeQuery] with `subscriptionInitial`, the hub's `<id>#0`); a plain unary request for its type is
 * `INVALID_REQUEST` ("send it with QueryBus.subscribe"). [admitSubscription] runs the same pre-checks for the
 * subscription request itself (`mode = SUBSCRIPTION`) and hands back the decoded query, which the connection keeps to
 * match the emitter's updates against.
 */
class HandlerInvoker(
    private val registry: HandlerRegistry,
    private val codec: JsonCodec,
    /** Runs the handlers annotated with `@IdempotentHandler`; without one such a handler fails when called. */
    private val idempotency: IdempotencyInterceptor? = null,
) : SmartInitializingSingleton {
    private val log = LoggerFactory.getLogger(HandlerInvoker::class.java)

    /** Once every bean exists (so every handler is registered): an `@IdempotentHandler` with no interceptor fails the startup, not the first call. */
    override fun afterSingletonsInstantiated() = verifyIdempotency()

    /**
     * Fails with an [IllegalStateException] naming the handler when a registered handler is an `@IdempotentHandler` and this
     * invoker has no [IdempotencyInterceptor]: such a handler would never run (it fails when called), so the service must not
     * start. The interceptor exists when a class in the application's packages has such a method; set
     * `kraken.messaging.external.calls.enabled=true` when the adapter lives elsewhere (and not `false` while one exists).
     */
    fun verifyIdempotency() {
        if (idempotency != null) return
        val handler = registry.idempotentHandlers().firstOrNull() ?: return
        throw IllegalStateException(
            "Handler ${handler.label} is an @IdempotentHandler but this service has no idempotency support (no IdempotencyInterceptor): " +
                "it would fail when called. The interceptor exists when a class in the application's packages has an @IdempotentHandler method; " +
                "set kraken.messaging.external.calls.enabled=true when the adapter lives elsewhere, and do not set it to false while one exists",
        )
    }

    /** [identity] is the resolved caller (null = anonymous); it is checked against the handler's `@RequiresAuthentication`. */
    suspend fun invokeCommand(type: String, payload: ByteArray, ctx: MessageContext, identity: Identity?): InvocationResult =
        invoke(type, registry.commandHandler(type), payload, ctx, identity)

    /**
     * [subscriptionInitial] (P4c): the request is the initial request of a subscription (`<id>#0`), so a subscription
     * query handler answers it; without it such a handler is `INVALID_REQUEST`. Other handlers are invoked either way.
     */
    suspend fun invokeQuery(
        type: String,
        payload: ByteArray,
        ctx: MessageContext,
        identity: Identity?,
        subscriptionInitial: Boolean = false,
    ): InvocationResult = invoke(type, registry.queryHandler(type), payload, ctx, identity, subscriptionInitial)

    /**
     * Admits a subscription request (`mode = SUBSCRIPTION`, P4c) for [type]: the handler must be a subscription query
     * handler (else `INVALID_REQUEST`), `@RequiresAuthentication` is checked as for a unary handler, and [payload] is
     * decoded as the handler's message class (`INVALID_REQUEST` when it does not). Nothing is invoked.
     */
    fun admitSubscription(type: String, payload: ByteArray, ctx: MessageContext, identity: Identity?): SubscriptionAdmission {
        val handler = registry.queryHandler(type)
            ?: return SubscriptionAdmission.Rejected(InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "no subscription handler for $type on this instance"))
        if (!handler.subscription) {
            return SubscriptionAdmission.Rejected(InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "handler ${handler.label} for $type is not a subscription query handler (it answers mode UNARY)"))
        }
        return when (val prepared = prepare(type, handler, payload, ctx, identity)) {
            is Prepared.Rejected -> SubscriptionAdmission.Rejected(prepared.failure)
            is Prepared.Ready -> SubscriptionAdmission.Accepted(prepared.message)
        }
    }

    /**
     * Calls every handler of [type] in order, only those of [processor] when it is given (ADR 0022), and in a [replay] (P6b)
     * none marked `@DisallowReplay`; a failing handler does not stop the others, the first failure is rethrown.
     */
    suspend fun invokeEvent(type: String, payload: ByteArray, ctx: MessageContext, processor: String? = null, replay: Boolean = false) {
        var first: Throwable? = null
        val handlers = (if (processor == null) registry.eventHandlers(type) else registry.eventHandlers(type, processor))
            .filter { !replay || !it.disallowReplay }
        for (handler in handlers) {
            try {
                call(handler, codec.decode(payload, handler.messageClass), ctx)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (first == null) first = e else if (e !== first) first.addSuppressed(e)
            }
        }
        first?.let { throw it }
    }

    /**
     * Runs the streaming query handler of [type] (P4b): decodes [payload], checks `@RequiresAuthentication` as for a
     * unary handler, then collects the handler's `Flow<R>`, encoding each element and handing it to [onPart] in order.
     * Returns null when the flow completed, or the failure that ends the stream: the pre-checks' (`NO_HANDLER`,
     * `INVALID_REQUEST` for a bad payload or a handler that is not a streaming one, `UNAUTHENTICATED`,
     * `PERMISSION_DENIED`) or the flow's exception mapped like a unary handler's. Cancellation (the hub's `Cancel`)
     * propagates and stops the collection.
     */
    suspend fun invokeStream(
        type: String,
        payload: ByteArray,
        ctx: MessageContext,
        identity: Identity?,
        onPart: suspend (ByteArray) -> Unit,
    ): InvocationResult.Failure? {
        val handler = registry.queryHandler(type)
        if (handler != null && !handler.streaming) {
            return InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "handler ${handler.label} for $type is not a streaming query handler (it answers mode UNARY)")
        }
        return when (val prepared = prepare(type, handler, payload, ctx, identity)) {
            is Prepared.Rejected -> prepared.failure
            is Prepared.Ready -> try {
                @Suppress("UNCHECKED_CAST")
                val flow = handler!!.invoke(prepared.message, ctx) as Flow<Any?>
                flow.collect { element -> onPart(codec.encode(element ?: Unit)) }
                null
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                failureOf(e)
            }
        }
    }

    /** Runs [handler], through the idempotency interceptor when it is an `@IdempotentHandler`. */
    private suspend fun call(handler: HandlerMethod, message: Any, ctx: MessageContext): Any? {
        if (handler.idempotency == null) return handler.invoke(message, ctx)
        val interceptor = idempotency ?: throw IllegalStateException(
            "Handler ${handler.label} is an @IdempotentHandler but this service has no idempotency support (no IdempotencyInterceptor); it is not run",
        )
        return interceptor.execute(handler, message, ctx) { handler.invoke(message, ctx) }
    }

    private sealed interface Prepared {
        class Ready(val message: Any) : Prepared
        class Rejected(val failure: InvocationResult.Failure) : Prepared
    }

    /** The checks before a handler runs: it exists, the caller may call it, the payload decodes, the expression grants. */
    private fun prepare(type: String, handler: HandlerMethod?, payload: ByteArray, ctx: MessageContext, identity: Identity?): Prepared {
        if (handler == null) return Prepared.Rejected(InvocationResult.Failure(ErrorCode.NO_HANDLER, "no handler for type $type"))
        if (handler.authorization != null && identity == null) {
            // Before decoding, so an anonymous caller cannot probe payload shapes of protected handlers.
            return Prepared.Rejected(rejected(handler, ErrorCode.UNAUTHENTICATED, "handler ${handler.label} requires authentication"))
        }
        val message = try {
            codec.decode(payload, handler.messageClass)
        } catch (e: InvalidMessageException) {
            return Prepared.Rejected(InvocationResult.Failure(ErrorCode.INVALID_REQUEST, e.message ?: "Cannot decode payload as $type"))
        }
        return when (val decision = handler.authorization?.check(identity, message, ctx, handler.label)) {
            null, AuthorizationDecision.Granted -> Prepared.Ready(message)
            is AuthorizationDecision.Unauthenticated -> Prepared.Rejected(rejected(handler, ErrorCode.UNAUTHENTICATED, decision.message))
            is AuthorizationDecision.Denied -> Prepared.Rejected(rejected(handler, ErrorCode.PERMISSION_DENIED, decision.message))
        }
    }

    private suspend fun invoke(
        type: String,
        handler: HandlerMethod?,
        payload: ByteArray,
        ctx: MessageContext,
        identity: Identity?,
        subscriptionInitial: Boolean = false,
    ): InvocationResult {
        if (handler != null && handler.streaming) {
            return InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "$type is a streaming query: send it with QueryBus.stream (mode STREAMING)")
        }
        if (handler != null && handler.subscription && !subscriptionInitial) {
            return InvocationResult.Failure(ErrorCode.INVALID_REQUEST, "$type is a subscription query: send it with QueryBus.subscribe")
        }
        val message = when (val prepared = prepare(type, handler, payload, ctx, identity)) {
            is Prepared.Rejected -> return prepared.failure
            is Prepared.Ready -> prepared.message
        }
        return try {
            InvocationResult.Success(codec.encode(call(handler!!, message, ctx) ?: Unit))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            failureOf(e)
        }
    }

    /** The failure a handler's (or its flow's) exception maps to; see the class comment. */
    private fun failureOf(e: Throwable): InvocationResult.Failure = when (e) {
        is UnauthenticatedException -> InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated")
        is AccessDeniedException -> InvocationResult.Failure(ErrorCode.PERMISSION_DENIED, e.message ?: "access denied")
        is org.springframework.security.access.AccessDeniedException -> InvocationResult.Failure(ErrorCode.PERMISSION_DENIED, e.message ?: "access denied")
        is org.springframework.security.core.AuthenticationException -> InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated")
        // An adapter asks for a later attempt: the marker in the details tells the saga relay (also across the hub).
        is RetryableExternalException -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e), ExternalFailureMarkers.RETRYABLE)
        is ExternalCallUnknownOutcomeException -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, e.message ?: "unknown outcome", ExternalFailureMarkers.UNKNOWN_OUTCOME)
        is ExternalCallFailedException -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, e.message ?: "failed earlier")
        is IllegalArgumentException -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, e.message ?: e::class.simpleName.orEmpty(), violations(e))
        else -> when {
            isConstraintViolation(e) -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, e.message ?: e::class.simpleName.orEmpty(), violations(e))
            else -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e))
        }
    }

    private fun rejected(handler: HandlerMethod, code: ErrorCode, message: String): InvocationResult.Failure {
        log.debug("handler {} rejected request: {} {}", handler.label, code, message)
        return InvocationResult.Failure(code, message)
    }

    private fun describe(e: Throwable) = "${e::class.simpleName}: ${e.message}"

    private fun isConstraintViolation(e: Throwable) = ValidationSupport.available && ValidationSupport.isViolation(e)

    private fun violations(e: Throwable): ByteArray =
        codec.encode(if (isConstraintViolation(e)) ValidationSupport.messages(e) else emptyList<String>())
}

/** Keeps the optional jakarta.validation-api off the class-loading path when it is not on the runtime classpath. */
private object ValidationSupport {
    val available: Boolean = try {
        Class.forName("jakarta.validation.ConstraintViolationException")
        true
    } catch (_: ClassNotFoundException) {
        false
    } catch (_: LinkageError) {
        false
    }

    fun isViolation(e: Throwable) = e is ConstraintViolationException

    fun messages(e: Throwable): List<String> =
        (e as ConstraintViolationException).constraintViolations.map { it.propertyPath.toString().let { path -> if (path.isEmpty()) it.message else "$path: ${it.message}" } }.sorted()
}
