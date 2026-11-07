package io.damoreira.kraken.messaging.metrics

import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageKind
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.saga.SettableClock
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The meters of the outbox relay, the event processors and the hub client on a [SimpleMeterRegistry] (P9, ADR 0026). */
class MicrometerMessagingMetricsTest {
    private val registry = SimpleMeterRegistry()
    private val clock = SettableClock(Instant.parse("2026-11-07T10:00:00Z"))
    private val metrics = MicrometerMessagingMetrics(registry, clock)

    @Test fun `outbox send attempts count by outcome`() {
        metrics.outboxPublished(ok = true)
        metrics.outboxPublished(ok = true)
        metrics.outboxPublished(ok = false)
        assertEquals(2.0, registry.get("kraken.outbox.published").tag("outcome", "ok").counter().count())
        assertEquals(1.0, registry.get("kraken.outbox.published").tag("outcome", "failed").counter().count())
    }

    @Test fun `the outbox gauges exist once the relay reports, the age is computed when observed and 0 without a pending document`() {
        assertNull(registry.find("kraken.outbox.pending").gauge(), "no relay, no outbox gauges")
        assertNull(registry.find("kraken.outbox.oldest.age").gauge())

        metrics.outboxPending(3)
        metrics.outboxOldest(clock.now.minusSeconds(4))
        val age = registry.get("kraken.outbox.oldest.age").gauge()
        assertEquals("seconds", age.id.baseUnit)
        assertEquals(3.0, registry.get("kraken.outbox.pending").gauge().value())
        assertEquals(4.0, age.value())
        clock.now = clock.now.plusMillis(1500)
        assertEquals(5.5, age.value(), "the age grows between polls")
        // Both outcomes of the send counter exist from the start, at 0.
        assertEquals(0.0, registry.get("kraken.outbox.published").tag("outcome", "failed").counter().count())

        metrics.outboxOldest(null)
        metrics.outboxPending(0)
        assertEquals(0.0, age.value())
        assertEquals(0.0, registry.get("kraken.outbox.pending").gauge().value())
    }

    @Test fun `consumed records count per processor, type and outcome`() {
        metrics.eventConsumed("default", "orders.OrderPlaced", MessagingMetrics.Consumed.HANDLED)
        metrics.eventConsumed("default", "orders.OrderPlaced", MessagingMetrics.Consumed.HANDLED)
        metrics.eventConsumed("audit", MessagingMetrics.OTHER, MessagingMetrics.Consumed.SKIPPED)
        assertEquals(2.0, registry.get("kraken.events.consumed").tags("processor", "default", "type", "orders.OrderPlaced", "outcome", "handled").counter().count())
        assertEquals(1.0, registry.get("kraken.events.consumed").tags("processor", "audit", "type", "other", "outcome", "skipped").counter().count())
        assertEquals(
            listOf("handled", "duplicate", "skipped", "failed", "parked"),
            MessagingMetrics.Consumed.entries.map { it.label },
        )
    }

    @Test fun `handler durations are a timer histogram with the shared bucket boundaries`() {
        metrics.eventHandled("default", "orders.OrderPlaced", Duration.ofMillis(30).toNanos())
        metrics.eventHandled("default", "orders.OrderPlaced", Duration.ofMillis(70).toNanos())
        val timer = registry.get("kraken.event.handler.duration").tags("processor", "default", "type", "orders.OrderPlaced").timer()
        assertEquals("seconds", timer.id.baseUnit)
        assertEquals(2, timer.count())
        assertEquals(0.1, timer.totalTime(TimeUnit.SECONDS), 1e-9)
        val buckets = timer.takeSnapshot().histogramCounts()
        assertEquals(MicrometerMessagingMetrics.DURATION_BUCKETS.toList(), buckets.map { it.bucket(TimeUnit.SECONDS) })
        assertEquals(0.0, buckets.single { it.bucket(TimeUnit.SECONDS) == 0.025 }.count())
        assertEquals(1.0, buckets.single { it.bucket(TimeUnit.SECONDS) == 0.05 }.count())
        assertEquals(2.0, buckets.single { it.bucket(TimeUnit.SECONDS) == 0.1 }.count())
    }

    @Test fun `hub calls are a timer histogram per kind, type and outcome`() {
        metrics.hubRequest(MessageKind.COMMAND, "orders.PlaceOrder", HubOutcome.OK, Duration.ofMillis(20).toNanos())
        metrics.hubRequest(MessageKind.QUERY, "orders.GetOrder", HubOutcome.TIMEOUT, Duration.ofSeconds(2).toNanos())
        val ok = registry.get("kraken.hub.client.request.duration").tags("kind", "command", "type", "orders.PlaceOrder", "outcome", "ok").timer()
        assertEquals(0.02, ok.totalTime(TimeUnit.SECONDS), 1e-9)
        assertEquals(MicrometerMessagingMetrics.DURATION_BUCKETS.toList(), ok.takeSnapshot().histogramCounts().map { it.bucket(TimeUnit.SECONDS) })
        assertEquals(1.0, ok.takeSnapshot().histogramCounts().single { it.bucket(TimeUnit.SECONDS) == 0.025 }.count())
        assertEquals(1, registry.get("kraken.hub.client.request.duration").tags("kind", "query", "outcome", "timeout").timer().count())
    }

    @Test fun `the bucket boundaries are the hub's request duration boundaries`() {
        assertEquals(
            listOf(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
            MicrometerMessagingMetrics.DURATION_BUCKETS.toList(),
        )
    }

    @Test fun `only the Micrometer metrics ask the relay to count the outbox`() {
        assertTrue(metrics.countsOutbox)
        assertEquals(false, NoopMessagingMetrics.countsOutbox)
    }

    @Test fun `a failed call maps to the hub's outcome values, plus unavailable and error`() {
        val cases = mapOf(
            NoHandlerException("t") to "no_handler",
            MessageTimeoutException("t") to "timeout",
            HandlerDisconnectedException("t") to "handler_disconnected",
            RemoteHandlerException("t", "boom", ByteArray(0)) to "handler_error",
            RemoteHandlerException("t", "odd", ByteArray(0), "SOMETHING_NEW") to "error",
            InvalidMessageException("bad") to "invalid_request",
            HandlerBusyException("t") to "resource_exhausted",
            StreamCancelledException("too long") to "cancelled",
            UnauthenticatedException("no") to "denied",
            AccessDeniedException("no") to "denied",
            HubUnavailableException("none") to "unavailable",
            HubConnectionLostException("lost") to "unavailable",
            IllegalStateException("?") to "error",
        )
        for ((e, label) in cases) assertEquals(label, HubOutcome.of(e).label, e::class.simpleName)
        assertEquals("ok", HubOutcome.OK.label)
    }
}
