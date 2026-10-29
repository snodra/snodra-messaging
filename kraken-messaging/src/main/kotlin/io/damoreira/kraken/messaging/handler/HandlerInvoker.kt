package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import jakarta.validation.ConstraintViolationException
import kotlin.coroutines.cancellation.CancellationException

sealed interface InvocationResult {
    data class Success(val payload: ByteArray) : InvocationResult
    data class Failure(val code: ErrorCode, val message: String, val details: ByteArray = ByteArray(0)) : InvocationResult
}

/** Decodes a payload, calls the registered handler and maps the outcome (or exception) to an [InvocationResult]. */
class HandlerInvoker(private val registry: HandlerRegistry, private val codec: JsonCodec) {

    suspend fun invokeCommand(type: String, payload: ByteArray, ctx: MessageContext): InvocationResult =
        invoke(type, registry.commandHandler(type), payload, ctx)

    suspend fun invokeQuery(type: String, payload: ByteArray, ctx: MessageContext): InvocationResult =
        invoke(type, registry.queryHandler(type), payload, ctx)

    /** Calls every handler of [type] in order; a failing handler does not stop the others, the first failure is rethrown. */
    suspend fun invokeEvent(type: String, payload: ByteArray, ctx: MessageContext) {
        var first: Throwable? = null
        for (handler in registry.eventHandlers(type)) {
            try {
                handler.invoke(codec.decode(payload, handler.messageClass), ctx)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (first == null) first = e else first.addSuppressed(e)
            }
        }
        first?.let { throw it }
    }

    private suspend fun invoke(type: String, handler: HandlerMethod?, payload: ByteArray, ctx: MessageContext): InvocationResult {
        if (handler == null) return InvocationResult.Failure(ErrorCode.NO_HANDLER, "no handler for type $type")
        val message = try {
            codec.decode(payload, handler.messageClass)
        } catch (e: InvalidMessageException) {
            return InvocationResult.Failure(ErrorCode.INVALID_REQUEST, e.message ?: "Cannot decode payload as $type")
        }
        return try {
            InvocationResult.Success(codec.encode(handler.invoke(message, ctx) ?: Unit))
        } catch (e: CancellationException) {
            throw e
        } catch (e: UnauthenticatedException) {
            InvocationResult.Failure(ErrorCode.UNAUTHENTICATED, e.message ?: "unauthenticated")
        } catch (e: IllegalArgumentException) {
            InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e), violations(e))
        } catch (e: Throwable) {
            if (isConstraintViolation(e)) {
                InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e), violations(e))
            } else {
                InvocationResult.Failure(ErrorCode.HANDLER_ERROR, describe(e))
            }
        }
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
        (e as ConstraintViolationException).constraintViolations.map { "${it.propertyPath}: ${it.message}" }.sorted()
}
