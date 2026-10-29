package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.security.Identity
import jakarta.validation.ConstraintViolationException
import org.slf4j.LoggerFactory
import kotlin.coroutines.cancellation.CancellationException

sealed interface InvocationResult {
    data class Success(val payload: ByteArray) : InvocationResult
    data class Failure(val code: ErrorCode, val message: String, val details: ByteArray = ByteArray(0)) : InvocationResult
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
 */
class HandlerInvoker(private val registry: HandlerRegistry, private val codec: JsonCodec) {
    private val log = LoggerFactory.getLogger(HandlerInvoker::class.java)

    /** [identity] is the resolved caller (null = anonymous); it is checked against the handler's `@RequiresAuthentication`. */
    suspend fun invokeCommand(type: String, payload: ByteArray, ctx: MessageContext, identity: Identity?): InvocationResult =
        invoke(type, registry.commandHandler(type), payload, ctx, identity)

    suspend fun invokeQuery(type: String, payload: ByteArray, ctx: MessageContext, identity: Identity?): InvocationResult =
        invoke(type, registry.queryHandler(type), payload, ctx, identity)

    /** Calls every handler of [type] in order; a failing handler does not stop the others, the first failure is rethrown. */
    suspend fun invokeEvent(type: String, payload: ByteArray, ctx: MessageContext) {
        var first: Throwable? = null
        for (handler in registry.eventHandlers(type)) {
            try {
                handler.invoke(codec.decode(payload, handler.messageClass), ctx)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (first == null) first = e else if (e !== first) first.addSuppressed(e)
            }
        }
        first?.let { throw it }
    }

    private suspend fun invoke(
        type: String,
        handler: HandlerMethod?,
        payload: ByteArray,
        ctx: MessageContext,
        identity: Identity?,
    ): InvocationResult {
        if (handler == null) return InvocationResult.Failure(ErrorCode.NO_HANDLER, "no handler for type $type")
        if (handler.authorization != null && identity == null) {
            // Before decoding, so an anonymous caller cannot probe payload shapes of protected handlers.
            return rejected(handler, ErrorCode.UNAUTHENTICATED, "handler ${handler.label} requires authentication")
        }
        val message = try {
            codec.decode(payload, handler.messageClass)
        } catch (e: InvalidMessageException) {
            return InvocationResult.Failure(ErrorCode.INVALID_REQUEST, e.message ?: "Cannot decode payload as $type")
        }
        when (val decision = handler.authorization?.check(identity, message, ctx, handler.label)) {
            null, AuthorizationDecision.Granted -> Unit
            is AuthorizationDecision.Unauthenticated -> return rejected(handler, ErrorCode.UNAUTHENTICATED, decision.message)
            is AuthorizationDecision.Denied -> return rejected(handler, ErrorCode.PERMISSION_DENIED, decision.message)
        }
        return try {
            InvocationResult.Success(codec.encode(handler.invoke(message, ctx) ?: Unit))
        } catch (e: CancellationException) {
            throw e
        } catch (e: UnauthenticatedException) {
            InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated")
        } catch (e: AccessDeniedException) {
            InvocationResult.Failure(ErrorCode.PERMISSION_DENIED, e.message ?: "access denied")
        } catch (e: org.springframework.security.access.AccessDeniedException) {
            InvocationResult.Failure(ErrorCode.PERMISSION_DENIED, e.message ?: "access denied")
        } catch (e: org.springframework.security.core.AuthenticationException) {
            InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated")
        } catch (e: Throwable) {
            when {
                e is IllegalArgumentException || isConstraintViolation(e) ->
                    InvocationResult.Failure(ErrorCode.HANDLER_ERROR, e.message ?: e::class.simpleName.orEmpty(), violations(e))
                else -> InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e))
            }
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
