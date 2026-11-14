package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.store.MessageStore
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The processor's loop over a subscription (ADR 0027), on a scripted event store: a failed subscription is subscribed again
 * after a backoff, a pause that comes after a delivery hands it out again, what the subscription reports reaches the status.
 * Records of a type the processor does not handle are acknowledged without touching the message store.
 */
class EventConsumerSubscriptionTest {
    private val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(RecordingHandlers()) }
    private val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, io.damoreira.kraken.messaging.codec.JsonCodec()), mockk<MessageStore>(relaxed = true))
    private var consumer: EventConsumer? = null

    @AfterEach fun tearDown() {
        consumer?.stop()
    }

    private fun start(events: ScriptedEventStore, resubscribe: () -> Resubscribe = { Resubscribe() }) =
        EventConsumer(events, SubscriptionSpec("svc.default", "kraken.events"), dispatcher, maxAttempts = 1, resubscribe = resubscribe)
            .also { consumer = it; it.start() }

    private fun awaitSubscriptions(events: ScriptedEventStore, n: Int) =
        await().atMost(Duration.ofSeconds(10)).until { events.subscriptions.size >= n }

    @Test
    fun aFailedSubscriptionIsSubscribedAgainAfterABackoff() {
        val events = ScriptedEventStore()
        val c = start(events)
        awaitSubscriptions(events, 1)
        val first = events.subscriptions[0]
        first.assign("0")
        first.deliver("0", "0", "test.Other")
        await().atMost(Duration.ofSeconds(10)).until { first.acks == listOf("0" to "0") }

        first.fail("the consumer cannot go on")
        await().atMost(Duration.ofSeconds(10)).until { c.status().lastError == "subscription: event store: the consumer cannot go on" }
        assertEquals(ProcessorState.PROCESSOR_FAILING, c.status().state, "the failure shows on the status")
        await().atMost(Duration.ofSeconds(10)).until { first.closed }
        awaitSubscriptions(events, 2)
        val waited = Duration.ofNanos(events.subscribedAt[1] - events.subscribedAt[0])
        assertTrue(waited >= Duration.ofMillis(900), "subscribed again after the backoff (1 s), not in a hot loop: $waited")

        val second = events.subscriptions[1]
        second.assign("0")
        second.deliver("0", "1", "test.Other")
        await().atMost(Duration.ofSeconds(10)).until { second.acks == listOf("0" to "1") }
        c.stop()
        assertTrue(second.closed, "a stop closes the subscription")
        assertEquals(2, events.subscriptions.size, "no subscription after the stop")
    }

    @Test
    fun aSubscribeThatFailsIsTriedAgainAndThePauseIsKept() {
        val events = ScriptedEventStore()
        events.failSubscribe.set(1)
        val c = EventConsumer(
            events, SubscriptionSpec("svc.default", "kraken.events"), dispatcher, maxAttempts = 1,
            resubscribe = { Resubscribe(min = Duration.ofMillis(50)) },
        ).also { consumer = it }
        c.pause()
        c.start()
        awaitSubscriptions(events, 1)
        assertTrue(events.subscriptions[0].paused, "the pause reaches the new subscription before its first next")
        await().atMost(Duration.ofSeconds(5)).until { c.status().state == ProcessorState.PROCESSOR_PAUSED }
    }

    @Test
    fun aRecordHandedOutBeforeThePauseTookEffectIsHandedOutAgain() {
        val events = ScriptedEventStore()
        val c = start(events)
        awaitSubscriptions(events, 1)
        val sub = events.subscriptions[0]
        sub.assign("0")
        await().atMost(Duration.ofSeconds(10)).until { c.status().segmentsCount == 1 }
        c.pause()
        assertTrue(sub.paused)
        sub.deliver("0", "4", "test.Other") // a store that handed it out before the pause reached it
        await().atMost(Duration.ofSeconds(10)).until { sub.redelivered == listOf("0" to "4") }
        assertEquals(emptyList(), sub.acks, "not handled while paused")
        c.resume()
        assertTrue(!sub.paused)
    }

    @Test
    fun theStatusReportsTheEventStoreItsCapabilitiesAndWhatTheSubscriptionRecoveredFrom() {
        val long = "x".repeat(70)
        val events = ScriptedEventStore(name = "é".repeat(40), capabilities = setOf(Capability.LAG, Capability.REPLAY))
        val c = start(events)
        awaitSubscriptions(events, 1)
        val sub = events.subscriptions[0]
        sub.assign("10", "9", "a", long.repeat(2))
        await().atMost(Duration.ofSeconds(10)).until { c.status().segmentsCount > 0 }
        val status = c.status()
        assertEquals("é".repeat(32), status.eventStore, "cut at 64 UTF-8 bytes")
        assertEquals(listOf("lag", "replay"), status.capabilitiesList)
        assertEquals(listOf("9", "10", "a"), status.segmentsList.map { it.segment }, "segment order; a name over 128 bytes is left out")
        assertEquals("svc.default", status.subscription)
        assertEquals("kraken.events", status.source)
        assertEquals(ProcessorState.PROCESSOR_RUNNING, status.state)

        sub.errors += "rebalance: could not read the committed offsets"
        await().atMost(Duration.ofSeconds(10)).until { c.status().lastError == "rebalance: could not read the committed offsets" }
        assertEquals(ProcessorState.PROCESSOR_FAILING, c.status().state)
    }

    @Test
    fun theBackoffDoublesWhileSubscriptionsFailBeforeDeliveringAndStartsOverAfterADelivery() = runBlocking {
        val waits = CopyOnWriteArrayList<Duration>()
        val ends = ArrayDeque(
            listOf<Resubscribe.Ended>(
                Resubscribe.Ended.Failed(EventStoreException.Failed("1"), delivered = false),
                Resubscribe.Ended.Failed(EventStoreException.Failed("2"), delivered = false), // an assignment alone: still doubling
                Resubscribe.Ended.Failed(EventStoreException.Failed("3"), delivered = false),
                Resubscribe.Ended.Failed(EventStoreException.Failed("4"), delivered = true), // it delivered: back to the minimum
                Resubscribe.Ended.Failed(EventStoreException.Failed("5"), delivered = false),
                Resubscribe.Ended.Stopped,
            ),
        )
        val subscribed = CopyOnWriteArrayList<Subscription>()
        Resubscribe(min = Duration.ofMillis(10), max = Duration.ofMillis(35)).run(
            subscribe = { ScriptedEventStore.Scripted(SubscriptionSpec("s", "src")).also { subscribed += it } },
            read = { ends.removeFirst() },
            failed = { _, wait -> waits += wait },
        )
        assertEquals(listOf(10L, 20L, 35L, 10L, 20L), waits.map { it.toMillis() })
        assertTrue(subscribed.all { (it as ScriptedEventStore.Scripted).closed }, "every subscription closed")

        // A subscription that lived the stable period counts as a good one, delivered or not.
        val stable = CopyOnWriteArrayList<Duration>()
        var n = 0
        Resubscribe(min = Duration.ofMillis(10), max = Duration.ofMillis(100), stable = Duration.ofMillis(50)).run(
            subscribe = { ScriptedEventStore.Scripted(SubscriptionSpec("s", "src")) },
            read = {
                n++
                when (n) {
                    1, 2 -> Resubscribe.Ended.Failed(EventStoreException.Failed("quick"), delivered = false)
                    3 -> { kotlinx.coroutines.delay(80); Resubscribe.Ended.Failed(EventStoreException.Failed("lived"), delivered = false) }
                    else -> Resubscribe.Ended.Stopped
                }
            },
            failed = { _, wait -> stable += wait },
        )
        assertEquals(listOf(10L, 20L, 10L), stable.map { it.toMillis() })
    }
}
