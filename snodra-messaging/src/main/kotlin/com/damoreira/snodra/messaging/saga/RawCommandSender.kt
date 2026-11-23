package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.HandlerBusyException
import com.damoreira.snodra.messaging.HandlerDisconnectedException
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.HubUnavailableException
import com.damoreira.snodra.messaging.InvalidMessageException
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.MessageTimeoutException
import com.damoreira.snodra.messaging.MessagingException
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.RemoteHandlerException
import com.damoreira.snodra.messaging.SendOptions
import com.damoreira.snodra.messaging.UnauthenticatedException
import com.damoreira.snodra.messaging.external.ExternalFailureMarkers
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.InvocationResult
import com.damoreira.snodra.messaging.hub.HubErrors
import com.damoreira.snodra.messaging.saga.SagaCommandFailed.Reason
import com.damoreira.snodra.messaging.tracing.CorrelationContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.UUID

/**
 * Sends a command it knows only by wire type name and JSON bytes: the saga relay has no Kotlin class for a stored
 * command, and a saga never reads a command's reply. Kept apart from the public `CommandBus`. A call carries no caller:
 * no user token and no identity, so a handler with `@RequiresAuthentication` refuses it (S1 plan, spec "identity").
 * The correlation id comes from the calling coroutine's `CorrelationContext`.
 */
interface RawCommandSender {
    suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions = SendOptions()): RawOutcome

    /**
     * How long the relay waits for [sendRaw] of [type] before it abandons the call and sends it again later with the same
     * idempotency key, given its `sagas.relay.send-timeout` [sendTimeout]. A local handler has no other
     * bound: [sendTimeout]. Through the hub never less than the longest the call can take (the wait for a hub, moves to other hubs
     * included, plus the last hop's guard past the hub's deadline: `HubTransport.callBound`) plus [HUB_CALL_GUARD], so the relay
     * never sends a command again while the hub may still be running the first call.
     */
    fun sendTimeoutFor(type: String, sendTimeout: Duration): Duration = sendTimeout

    companion object {
        /**
         * Added to the longest a call through the hub can take (`HubTransport.callBound`, which already counts the wait for a hub
         * and the client's own guard): room for scheduling, so the relay's limit always falls after the call's own end.
         */
        val HUB_CALL_GUARD: Duration = Duration.ofSeconds(10)
    }
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
 * does (an unknown type is `NO_HANDLER`), and what [com.damoreira.snodra.messaging.hub.HubBus] does for a local handler.
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
        // The caller's trace too (a saga's command keeps the trace of its cause).
        val trace = CorrelationContext(correlationId, CorrelationContext.currentTraceparent(currentCoroutineContext()))
        return when (val result = withContext(trace) { invoker.invokeCommand(type, payload, ctx, null) }) {
            is InvocationResult.Success -> RawOutcome.Success
            is InvocationResult.Failure -> RawFailures.classify(HubErrors.toException(type, result.code, result.message, result.details))
        }
    }
}
