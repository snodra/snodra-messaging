package io.damoreira.kraken.messaging.metrics

import io.damoreira.kraken.messaging.MessageKind
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/** Records every hook call of [MessagingMetrics], for the tests of the components that report to it. */
class RecordingMessagingMetrics : MessagingMetrics {
    data class Consumed(val processor: String, val type: String, val outcome: MessagingMetrics.Consumed)
    data class Handled(val processor: String, val type: String, val nanos: Long)
    data class HubCall(val kind: MessageKind, val type: String, val outcome: HubOutcome, val nanos: Long)

    val consumed = CopyOnWriteArrayList<Consumed>()
    val handled = CopyOnWriteArrayList<Handled>()
    val published = CopyOnWriteArrayList<Boolean>()
    val oldest = CopyOnWriteArrayList<Instant?>()
    val pending = CopyOnWriteArrayList<Long>()
    val hubCalls = CopyOnWriteArrayList<HubCall>()

    override val countsOutbox: Boolean get() = true

    override fun eventConsumed(processor: String, type: String, outcome: MessagingMetrics.Consumed) {
        consumed += Consumed(processor, type, outcome)
    }

    override fun eventHandled(processor: String, type: String, nanos: Long) {
        handled += Handled(processor, type, nanos)
    }

    override fun outboxPublished(ok: Boolean) {
        published += ok
    }

    override fun outboxOldest(createdAt: Instant?) {
        oldest += createdAt
    }

    override fun outboxPending(count: Long) {
        pending += count
    }

    override fun hubRequest(kind: MessageKind, type: String, outcome: HubOutcome, nanos: Long) {
        hubCalls += HubCall(kind, type, outcome, nanos)
    }

    /** The outcomes recorded for [processor], in order. */
    fun outcomes(processor: String): List<Pair<String, MessagingMetrics.Consumed>> =
        consumed.filter { it.processor == processor }.map { it.type to it.outcome }
}
