package io.damoreira.kraken.messaging.metrics

import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.hub.v1.SegmentStatus
import io.damoreira.kraken.messaging.events.ProcessorStatusSource
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** `kraken_processor_lag`, `kraken_processor_state` and `kraken_dead_letters` from the processors' status (P9, ADR 0026). */
class EventProcessorMetricsTest {
    private val registry = SimpleMeterRegistry()

    @Volatile private var statuses: List<ProcessorStatus> = emptyList()
    private val calls = AtomicInteger()
    private val source = ProcessorStatusSource { calls.incrementAndGet(); statuses }
    private val metrics = EventProcessorMetrics(registry, source, Duration.ofMillis(50))

    private fun status(name: String, state: ProcessorState, deadLetters: Long = 0, vararg lags: Pair<Int, Long>) =
        ProcessorStatus.newBuilder().setName(name).setState(state).setDeadLetters(deadLetters)
            .addAllSegments(lags.map { (p, lag) -> SegmentStatus.newBuilder().setSegment(p.toString()).setLag(lag).build() })
            .build()

    private fun gauge(name: String, vararg tags: String) = registry.find(name).tags(*tags).gauge()?.value()

    @Test fun `every started processor has its state, dead letters and the lag of each segment with a known lag`() {
        statuses = listOf(
            status("default", ProcessorState.PROCESSOR_RUNNING, 0, 0 to 3L, 1 to 0L),
            status("audit", ProcessorState.PROCESSOR_FAILING, 4, 0 to -1L, 2 to 7L),
        )
        metrics.sampleOnce()

        assertEquals(3.0, gauge("kraken.processor.lag", "processor", "default", "segment", "0"))
        assertEquals(0.0, gauge("kraken.processor.lag", "processor", "default", "segment", "1"))
        assertEquals(7.0, gauge("kraken.processor.lag", "processor", "audit", "segment", "2"))
        assertNull(gauge("kraken.processor.lag", "processor", "audit", "segment", "0"), "an unknown lag (-1) is not exported")
        assertEquals(0.0, gauge("kraken.dead.letters", "processor", "default"))
        assertEquals(4.0, gauge("kraken.dead.letters", "processor", "audit"))
        for (state in listOf("starting", "running", "paused", "failing", "stopped")) {
            assertEquals(if (state == "running") 1.0 else 0.0, gauge("kraken.processor.state", "processor", "default", "state", state), state)
            assertEquals(if (state == "failing") 1.0 else 0.0, gauge("kraken.processor.state", "processor", "audit", "state", state), state)
        }
        assertEquals(10, registry.find("kraken.processor.state").gauges().size, "five states per processor, nothing else")
    }

    @Test fun `a revoked segment's lag reads 0 until an export read it, then goes away, and a new state replaces the old one`() {
        statuses = listOf(status("default", ProcessorState.PROCESSOR_RUNNING, 0, 0 to 3L, 1 to 5L, 2 to 4L))
        metrics.sampleOnce()
        statuses = listOf(status("default", ProcessorState.PROCESSOR_PAUSED, 2, 1 to 6L, 2 to -1L))
        metrics.sampleOnce()
        metrics.sampleOnce() // nobody read the 0 yet: it stays

        // Prometheus keeps a series that stops at its last value (no staleness over OTLP): 0, not 3, next to the new owner's.
        assertEquals(0.0, gauge("kraken.processor.lag", "processor", "default", "segment", "0"), "revoked: 0 until read")
        assertEquals(6.0, gauge("kraken.processor.lag", "processor", "default", "segment", "1"))
        assertNull(gauge("kraken.processor.lag", "processor", "default", "segment", "2"), "assigned, lag unknown: absent, not 0")
        assertEquals(1.0, gauge("kraken.processor.state", "processor", "default", "state", "paused"))
        assertEquals(0.0, gauge("kraken.processor.state", "processor", "default", "state", "running"))
        assertEquals(2.0, gauge("kraken.dead.letters", "processor", "default"))

        metrics.sampleOnce()
        assertNull(gauge("kraken.processor.lag", "processor", "default", "segment", "0"), "read once: gone")
        statuses = emptyList()
        metrics.sampleOnce()
        assertEquals(0.0, gauge("kraken.processor.lag", "processor", "default", "segment", "1"), "a processor gone: its segments read 0")
        assertEquals(0.0, gauge("kraken.processor.lag", "processor", "default", "segment", "2"))
        metrics.sampleOnce()
        assertTrue(registry.find("kraken.processor.lag").gauges().isEmpty())
    }

    @Test fun `the meters are sampled on their own thread while running, never by the export`() {
        statuses = listOf(status("default", ProcessorState.PROCESSOR_STARTING))
        metrics.start()
        try {
            await atMost Duration.ofSeconds(5) until { calls.get() >= 3 }
            val before = calls.get()
            repeat(10) { registry.find("kraken.processor.state").gauges().forEach { it.value() } }
            assertTrue(calls.get() - before <= 1, "reading the gauges does not ask the processors")
            assertEquals(1.0, gauge("kraken.processor.state", "processor", "default", "state", "starting"))
        } finally {
            metrics.stop()
        }
        val after = calls.get()
        Thread.sleep(200)
        assertEquals(after, calls.get(), "stopped")
    }
}
