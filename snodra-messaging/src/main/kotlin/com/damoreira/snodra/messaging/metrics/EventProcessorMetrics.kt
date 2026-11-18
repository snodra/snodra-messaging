package com.damoreira.snodra.messaging.metrics

import com.damoreira.snodra.server.v1.ProcessorState
import com.damoreira.snodra.messaging.events.EventConsumer
import com.damoreira.snodra.messaging.events.ProcessorStatusSource
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * The event processors' gauges (P9, ADR 0026), from the status this instance reports to the hub ([source]), sampled every
 * [interval] on a daemon thread (and on [sampleOnce]); the export only reads the sampled values. The status is built from
 * memory and what the subscriptions report, and asks its own background thread to refresh the dead-letter counts, so sampling it
 * also keeps those fresh when no hub is connected. One set per started processor, as Prometheus shows them:
 *
 * - `snodra_processor_state{processor,state}`: `starting | running | paused | failing | stopped`, 1 for the current one;
 * - `snodra_dead_letters{processor}`: the sampled letter count (mode `collection`, else 0, as the status);
 * - `snodra_processor_lag{processor,segment}`: per segment assigned here whose lag is known (an unknown lag has no
 *   series: 0 means caught up). A segment that leaves the assignment (revoked, or its processor gone) reads 0 until an
 *   export has read it, then its series goes away: Prometheus keeps a series that stops at its last value for minutes (no
 *   staleness over OTLP), which next to the new owner's would read as a lag nobody has.
 */
class EventProcessorMetrics(
    registry: MeterRegistry,
    private val source: ProcessorStatusSource,
    private val interval: Duration = Duration.ofSeconds(5),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(EventProcessorMetrics::class.java)
    private val lag = MultiGauge.builder("snodra.processor.lag").description("Records behind the head per assigned segment").register(registry)
    private val state = MultiGauge.builder("snodra.processor.state").description("1 for the processor's current state").register(registry)
    private val deadLetters = MultiGauge.builder("snodra.dead.letters").description("Dead letters of the processor (mode collection)").register(registry)
    private var executor: ScheduledExecutorService? = null
    private val sampling = Any()

    /** The segments assigned at the last sample, by tags; guarded by [sampling]. */
    private var assigned = emptySet<Tags>()

    /** Segments that left the assignment, read as 0 until an export read them; guarded by [sampling]. */
    private val departed = HashMap<Tags, Departed>()

    private class Departed {
        @Volatile var read = false
    }

    init {
        require(!interval.isZero && !interval.isNegative) { "interval must be positive, was $interval" }
    }

    /** One sample of every gauge. */
    fun sampleOnce() = synchronized(sampling) {
        try {
            val statuses = source.processorStatuses()
            fun tags(processor: String, segment: String) = Tags.of("processor", processor, "segment", segment)
            val now = statuses.flatMap { s -> s.segmentsList.map { tags(s.name, it.segment) } }.toSet()
            departed.values.removeIf { it.read }
            departed.keys.removeAll(now)
            (assigned - now).forEach { departed.getOrPut(it) { Departed() } }
            assigned = now
            lag.register(
                statuses.flatMap { s -> s.segmentsList.filter { it.lag >= 0 }.map { MultiGauge.Row.of(tags(s.name, it.segment), it.lag) } } +
                    departed.map { (tags, d) -> MultiGauge.Row.of(tags, d) { it.read = true; 0.0 } },
                true,
            )
            state.register(
                statuses.flatMap { s -> STATES.map { (value, label) -> MultiGauge.Row.of(Tags.of("processor", s.name, "state", label), if (s.state == value) 1 else 0) } },
                true,
            )
            deadLetters.register(statuses.map { MultiGauge.Row.of(Tags.of("processor", it.name), it.deadLetters) }, true)
        } catch (e: Exception) {
            log.warn("Sampling the processor gauges failed, keeping the last values: {}", e.toString())
        }
    }

    @Synchronized
    override fun start() {
        if (executor != null) return
        executor = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "snodra-processor-metrics").apply { isDaemon = true } }
            .also { it.scheduleWithFixedDelay(::sampleOnce, 0, interval.toMillis(), TimeUnit.MILLISECONDS) }
    }

    @Synchronized
    override fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    @Synchronized
    override fun isRunning(): Boolean = executor != null

    /** With the event processors. */
    override fun getPhase(): Int = EventConsumer.PHASE

    private companion object {
        /** The states of the `state` label, as the hub's `snodra_server_processor_state` names them. */
        val STATES = listOf(
            ProcessorState.PROCESSOR_STARTING, ProcessorState.PROCESSOR_RUNNING, ProcessorState.PROCESSOR_PAUSED,
            ProcessorState.PROCESSOR_FAILING, ProcessorState.PROCESSOR_STOPPED,
        ).map { it to it.name.removePrefix("PROCESSOR_").lowercase() }
    }
}
