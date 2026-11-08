package io.damoreira.kraken.messaging.metrics

import io.damoreira.kraken.messaging.MessageKind
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * The messaging meters of P9 (ADR 0026) on a Micrometer [registry]. Prometheus shows them with `_` for `.`, the unit
 * appended and `_total` on counters (the series the dashboards query, identical to the Rust library's):
 *
 * - `kraken.outbox.published{outcome}` (counter, `ok | failed`, one per send attempt), and the gauges `kraken.outbox.pending`
 *   and `kraken.outbox.oldest.age` (seconds, computed when observed from the oldest document the relay's latest poll saw, 0
 *   when none), registered when the relay first reports;
 * - `kraken.events.consumed{processor,type,outcome}` (counter, see [MessagingMetrics.Consumed]);
 * - `kraken.event.handler.duration{processor,type}` and `kraken.hub.client.request.duration{kind,type,outcome}`: timers
 *   exported as histograms with the explicit [DURATION_BUCKETS] (the hub's request duration boundaries). Their unit is the
 *   registry's base time unit: seconds for Prometheus and the simple registry, and for OTLP because the library makes
 *   `management.otlp.metrics.export.base-time-unit` default to `seconds` ([MetricsDefaults]), so the series is
 *   `..._seconds_bucket` as the Rust library's. (A distribution summary in seconds cannot do it: Micrometer's fixed-boundary
 *   histograms round a recorded value up to a whole number.)
 *
 * Every hook only counts in memory: the export thread reads atomics, never the database.
 */
class MicrometerMessagingMetrics(private val registry: MeterRegistry, private val clock: Clock = Clock.systemUTC()) : MessagingMetrics {
    private val pending = AtomicLong()
    private val oldest = AtomicReference<Instant?>(null)
    private val outboxRegistered = AtomicBoolean(false)
    private val published = Counter.builder("kraken.outbox.published").description("Outbox send attempts by outcome").withRegistry(registry)
    private val consumed = Counter.builder("kraken.events.consumed").description("Records consumed per event processor, by final outcome").withRegistry(registry)
    private val handled = seconds("kraken.event.handler.duration", "Stage-1 handling time of a consumed record")
    private val calls = seconds("kraken.hub.client.request.duration", "Unary command and query calls through the hub, retries included")

    override val countsOutbox: Boolean get() = true

    override fun outboxPublished(ok: Boolean) = published.withTags("outcome", if (ok) "ok" else "failed").increment()

    override fun outboxOldest(createdAt: Instant?) {
        registerOutbox()
        oldest.set(createdAt)
    }

    override fun outboxPending(count: Long) {
        registerOutbox()
        pending.set(count)
    }

    override fun eventConsumed(processor: String, type: String, outcome: MessagingMetrics.Consumed) =
        consumed.withTags("processor", processor, "type", type, "outcome", outcome.label).increment()

    override fun eventHandled(processor: String, type: String, nanos: Long) =
        handled.withTags("processor", processor, "type", type).record(nanos, TimeUnit.NANOSECONDS)

    override fun hubRequest(kind: MessageKind, type: String, outcome: HubOutcome, nanos: Long) =
        calls.withTags("kind", kind.name.lowercase(), "type", type, "outcome", outcome.label).record(nanos, TimeUnit.NANOSECONDS)

    /** The outbox gauges, and both outcomes of the send counter at 0, the first time the relay reports. */
    private fun registerOutbox() {
        if (!outboxRegistered.compareAndSet(false, true)) return
        Gauge.builder("kraken.outbox.pending", pending) { it.get().toDouble() }
            .description("Outbox documents not yet published (and not given up on)")
            .register(registry)
        Gauge.builder("kraken.outbox.oldest.age", oldest) { ref -> ref.get()?.let { (clock.millis() - it.toEpochMilli()).coerceAtLeast(0) / 1000.0 } ?: 0.0 }
            .description("Age of the oldest unpublished outbox document, 0 when none")
            .baseUnit(SECONDS)
            .register(registry)
        for (outcome in listOf("ok", "failed")) published.withTags("outcome", outcome)
    }

    /** A timer with the explicit [DURATION_BUCKETS] and nothing else (no percentile buckets). */
    private fun seconds(name: String, description: String) =
        Timer.builder(name).description(description).serviceLevelObjectives(*BUCKETS)
            .minimumExpectedValue(BUCKETS.first()).maximumExpectedValue(BUCKETS.last()).withRegistry(registry)

    companion object {
        private const val SECONDS = "seconds"

        /** The histogram bucket boundaries in seconds, the hub's `kraken_hub_request_duration_seconds` ones (and the Rust library's). */
        val DURATION_BUCKETS = doubleArrayOf(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

        private val BUCKETS = DURATION_BUCKETS.map { Duration.ofNanos((it * 1e9).toLong()) }.toTypedArray()
    }
}
