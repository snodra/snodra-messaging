package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.external.ExternalFailureMarkers
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.hub.HubErrors
import io.damoreira.kraken.messaging.saga.SagaCommandFailed.Reason
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import java.util.UUID

/**
 * Sends a command it knows only by wire type name and JSON bytes: the saga relay has no Kotlin class for a stored
 * command, and a saga never reads a command's reply. Kept apart from the public `CommandBus`. A call carries no caller:
 * no user token and no identity, so a handler with `@RequiresAuthentication` refuses it (S1 plan, spec "identity").
 * The correlation id comes from the calling coroutine's `CorrelationContext`.
 */
interface RawCommandSender {
    suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions = SendOptions()): RawOutcome
}

/** What [RawCommandSender.sendRaw] reports, classified for the relay (the spec's retry taxonomy). */
sealed interface RawOutcome {
    /** The handler ran (or the idempotent adapter replayed its stored reply). */
    data object Success : RawOutcome

    /** A failure a later attempt may cure; the relay backs off and retries. */
    data class Transient(val message: String) : RawOutcome

    /** A failure no retry cures: it becomes a `SagaCommandFailed` with [reason] at once. */
    data class Permanent(val reason: SagaCommandFailed.Reason, val message: String) : RawOutcome
}

/** Classifies the failure of a raw send into a [RawOutcome] by the spec's retry taxonomy. */
object RawFailures {
    fun classify(error: Throwable): RawOutcome {
        val message = error.message ?: error::class.simpleName.orEmpty()
        return when (error) {
            // The handler did not run, or may not have: the saga relay's idempotency key makes a retry safe.
            is HubUnavailableException, is MessageTimeoutException, is HandlerDisconnectedException,
            is HubConnectionLostException, is HandlerBusyException -> RawOutcome.Transient(message)
            is NoHandlerException -> RawOutcome.Permanent(Reason.NO_HANDLER, message)
            is UnauthenticatedException, is AccessDeniedException -> RawOutcome.Permanent(Reason.DENIED, message)
            is InvalidMessageException -> RawOutcome.Permanent(Reason.INVALID, message)
            is RemoteHandlerException -> when {
                ExternalFailureMarkers.isRetryable(error.details) -> RawOutcome.Transient(message)
                ExternalFailureMarkers.isUnknownOutcome(error.details) -> RawOutcome.Permanent(Reason.UNKNOWN_OUTCOME, message)
                else -> RawOutcome.Permanent(Reason.FAILED, message)
            }
            is MessagingException -> RawOutcome.Permanent(Reason.FAILED, message)
            // Not a messaging failure: something unexpected in the sender; retried within the attempt limit.
            else -> RawOutcome.Transient("${error::class.simpleName}: $message")
        }
    }
}

/**
 * Sends to the handlers of this service only, through the invoker with no caller identity: what a service without a hub
 * does (an unknown type is `NO_HANDLER`), and what [io.damoreira.kraken.messaging.hub.HubBus] does for a local handler.
 */
class LocalRawCommandSender(private val invoker: HandlerInvoker) : RawCommandSender {
    override suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions): RawOutcome {
        val correlationId = CorrelationContext.resolve(currentCoroutineContext())
        val ctx = MessageContext(
            messageId = UUID.randomUUID().toString(),
            type = type,
            correlationId = correlationId,
            userId = null,
            roles = emptySet(),
            authorization = null,
            headers = options.headers + ("correlationId" to correlationId),
        )
        // The caller's coroutine and thread, so nothing of a thread-bound transaction is lost; no identity on purpose.
        return when (val result = withContext(CorrelationContext(correlationId)) { invoker.invokeCommand(type, payload, ctx, null) }) {
            is InvocationResult.Success -> RawOutcome.Success
            is InvocationResult.Failure -> RawFailures.classify(HubErrors.toException(type, result.code, result.message, result.details))
        }
    }
}
