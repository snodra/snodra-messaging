package io.damoreira.kraken.messaging.metrics

import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.DuplicateHandlerException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageKind
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnauthenticatedException
import java.time.Instant

/**
 * What the outbox relay, the event processors and the hub client report (P9, ADR 0026): implemented on Micrometer when a
 * registry exists ([MicrometerMessagingMetrics]), else [NoopMessagingMetrics]. The hooks run on the hot paths, so an
 * implementation only counts in memory. Every label value is a processor, a registered type (or [OTHER]), a kind or an
 * outcome from a fixed set, never an id. Methods have empty defaults so more can be added without breaking implementations.
 */
interface MessagingMetrics {
    /** The final outcome of one record in one event processor, `kraken_events_consumed_total{processor,type,outcome}`. */
    enum class Consumed {
        /** The dispatch succeeded: stage 1 ran (and, in `default`, the saga stage). */
        HANDLED,

        /** The processor's stage-1 marker already existed, or the record's own dead letter did (a redelivery after a crash). */
        DUPLICATE,

        /** The processor does not handle the type. */
        SKIPPED,

        /** Given up without a dead letter: mode `log`, or only the saga stage failed. */
        FAILED,

        /** Handed to the dead letters (mode `collection` or `topic`). */
        PARKED,
        ;

        val label: String = name.lowercase()
    }

    /** One record of [type] (a type [processor] handles, else [OTHER]) ended with [outcome] in [processor]. */
    fun eventConsumed(processor: String, type: String, outcome: Consumed) = Unit

    /** One stage-1 run of [processor] for a consumed record of [type] took [nanos], `kraken_event_handler_duration_seconds`. */
    fun eventHandled(processor: String, type: String, nanos: Long) = Unit

    /** One outbox send attempt, `kraken_outbox_published_total{outcome}`: [ok] when the broker acknowledged it. */
    fun outboxPublished(ok: Boolean) = Unit

    /** The `createdAt` of the oldest unpublished outbox entry as the relay last sampled it, whoever holds it (null: none). */
    fun outboxOldest(createdAt: Instant?) = Unit

    /** The unpublished (not sent, not given up) outbox documents, as the relay last counted them. */
    fun outboxPending(count: Long) = Unit

    /** Whether [outboxPending] and [outboxOldest] are wanted: the relay samples the outbox in the database only then. */
    val countsOutbox: Boolean get() = false

    /** One unary command or query call of [type] through the hub ended with [outcome] after [nanos], retries included. */
    fun hubRequest(kind: MessageKind, type: String, outcome: HubOutcome, nanos: Long) = Unit

    companion object {
        /** The `type` label of a record whose type the processor does not handle (or that has none): keeps the label set bounded. */
        const val OTHER = "other"
    }
}

/** No metrics: used when no `MeterRegistry` bean exists. */
object NoopMessagingMetrics : MessagingMetrics

/**
 * How a call through the hub ended, the `outcome` label of `kraken_hub_client_request_duration_seconds`: the hub's own
 * outcome values ([OK] to [DENIED]) and two of the client ([UNAVAILABLE], [ERROR]).
 */
enum class HubOutcome {
    OK,
    NO_HANDLER,
    TIMEOUT,
    HANDLER_DISCONNECTED,
    HANDLER_ERROR,

    /** The hub refused the request, or the reply did not decode. */
    INVALID_REQUEST,
    RESOURCE_EXHAUSTED,
    CANCELLED,

    /** Unauthenticated or permission denied. */
    DENIED,

    /** No live hub took the request, or the hub stream broke before the answer. */
    UNAVAILABLE,

    /** Anything else. */
    ERROR,
    ;

    val label: String = name.lowercase()

    companion object {
        /** The outcome of a call that threw [e]. */
        fun of(e: Throwable): HubOutcome = when (e) {
            is NoHandlerException -> NO_HANDLER
            is MessageTimeoutException -> TIMEOUT
            is HandlerDisconnectedException -> HANDLER_DISCONNECTED
            is RemoteHandlerException -> HANDLER_ERROR // also a code this library does not map, as the Rust library
            is InvalidMessageException -> INVALID_REQUEST
            is HandlerBusyException -> RESOURCE_EXHAUSTED
            is StreamCancelledException -> CANCELLED
            is UnauthenticatedException, is AccessDeniedException -> DENIED
            is HubUnavailableException, is HubConnectionLostException -> UNAVAILABLE
            is DuplicateHandlerException, is HubRegistrationRejectedException -> ERROR
            else -> ERROR
        }
    }
}
