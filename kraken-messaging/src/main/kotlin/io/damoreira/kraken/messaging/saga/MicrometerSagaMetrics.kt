package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.store.SagaStores
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * The saga metrics of the spec's Observability list on a Micrometer [registry] (Prometheus shows them with `_` for `.` and
 * `_total` on counters; every tag value is a type or deadline name, an outcome or a reason, never an id):
 *
 * - `kraken.sagas.started{type}`, `kraken.sagas.ended{type}` (counters);
 * - `kraken.saga.events{type,outcome}`, outcome `handled | no_match | conflict | parked` (counter);
 * - `kraken.deadlines.fired{name}` (counter) and `kraken.deadlines.due{name}`, the gauge of deadlines past their due time,
 *   set by every scheduler poll and back to 0 when none is overdue;
 * - `kraken.external.calls{handler,outcome}`, outcome `succeeded | replayed | failed | retryable | unknown` (counter);
 * - `kraken.saga.command.failures{type,reason}` (counter, added by the S1 plan): a `SagaCommandFailed` nobody handled; `type`
 *   is the failed command's wire type name;
 * - the gauges `kraken.sagas.active{type}` and `kraken.saga.dispatch.pending`, which no runtime hook moves: a
 *   [SagaGaugeSampler] counts them in the database every few seconds and calls [activeSagas] and [dispatchPending].
 *
 * [sagaTypes] are the saga types of this service: their `active` gauge exists (at 0) from the start.
 */
class MicrometerSagaMetrics(private val registry: MeterRegistry, sagaTypes: Collection<String> = emptyList()) : SagaMetrics {
    private val overdue = ConcurrentHashMap<String, AtomicLong>()
    private val active = ConcurrentHashMap<String, AtomicLong>()
    private val pending = AtomicLong()

    init {
        Gauge.builder("kraken.saga.dispatch.pending", pending) { it.get().toDouble() }
            .description("Commands and events saga handlers recorded that the relay has not dispatched yet (and not given up on)")
            .register(registry)
        sagaTypes.forEach { gaugeOf(active, "kraken.sagas.active", "type", it, "Live (not ended) saga instances") }
    }

    override fun sagaStarted(type: String) = registry.counter("kraken.sagas.started", "type", type).increment()

    override fun sagaEnded(type: String) = registry.counter("kraken.sagas.ended", "type", type).increment()

    override fun event(type: String, outcome: SagaMetrics.Outcome) =
        registry.counter("kraken.saga.events", "type", type, "outcome", outcome.name.lowercase()).increment()

    override fun deadlineFired(name: String) = registry.counter("kraken.deadlines.fired", "name", name).increment()

    override fun deadlinesOverdue(byName: Map<String, Long>) = set(overdue, "kraken.deadlines.due", "name", byName, "Scheduled deadlines past their due time")

    override fun externalCall(handler: String, outcome: SagaMetrics.ExternalOutcome) =
        registry.counter("kraken.external.calls", "handler", handler, "outcome", outcome.name.lowercase()).increment()

    override fun commandFailureUnhandled(commandType: String, reason: SagaCommandFailed.Reason) =
        registry.counter("kraken.saga.command.failures", "type", commandType, "reason", reason.name).increment()

    /** Sets `kraken.sagas.active` per type from [byType]; a type seen before and absent now goes to 0. */
    fun activeSagas(byType: Map<String, Long>) = set(active, "kraken.sagas.active", "type", byType, "Live (not ended) saga instances")

    /** Sets `kraken.saga.dispatch.pending`. */
    fun dispatchPending(count: Long) = pending.set(count)

    @Synchronized
    private fun set(gauges: ConcurrentHashMap<String, AtomicLong>, name: String, tagKey: String, values: Map<String, Long>, description: String) {
        for ((tag, value) in values) gaugeOf(gauges, name, tagKey, tag, description).set(value)
        for ((tag, gauge) in gauges) if (tag !in values) gauge.set(0)
    }

    private fun gaugeOf(gauges: ConcurrentHashMap<String, AtomicLong>, name: String, tagKey: String, tag: String, description: String): AtomicLong =
        gauges.computeIfAbsent(tag) { t ->
            AtomicLong().also { Gauge.builder(name, it) { v -> v.get().toDouble() }.tag(tagKey, t).description(description).register(registry) }
        }
}

/**
 * Counts what the store knows and no hook reports, every [interval] on a daemon thread (and on [sampleOnce]): the live (not
 * ended) instances per saga type ([io.damoreira.kraken.messaging.store.SagaRepository.countLiveByType]) and the pending
 * dispatch entries ([io.damoreira.kraken.messaging.store.SagaDispatchStore.pendingDispatches]). A failed sample is logged
 * and keeps the last values. Starts with the application, in the relay's phase.
 */
class SagaGaugeSampler(
    private val sagas: SagaStores,
    private val metrics: MicrometerSagaMetrics,
    private val interval: Duration = Duration.ofSeconds(15),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(SagaGaugeSampler::class.java)
    private var executor: ScheduledExecutorService? = null

    init {
        require(!interval.isZero && !interval.isNegative) { "interval must be positive, was $interval" }
    }

    /** One sample of both gauges. */
    fun sampleOnce() {
        try {
            metrics.activeSagas(sagas.sagas.countLiveByType())
            metrics.dispatchPending(sagas.dispatch.pendingDispatches())
        } catch (e: Exception) {
            log.warn("Sampling the saga gauges failed, keeping the last values: {}", e.toString())
        }
    }

    @Synchronized
    override fun start() {
        if (executor != null) return
        executor = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "kraken-saga-gauges").apply { isDaemon = true } }
            .also { it.scheduleWithFixedDelay(::sampleOnce, 0, interval.toMillis(), TimeUnit.MILLISECONDS) }
    }

    @Synchronized
    override fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    @Synchronized
    override fun isRunning(): Boolean = executor != null
    override fun getPhase(): Int = SagaDispatchRelay.PHASE
}
