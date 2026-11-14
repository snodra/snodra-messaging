package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.store.MessageStore
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The processor's loop over a subscription (ADR 0027), on a scripted event store: a failed subscription is subscribed again
 * after a backoff, a pause that comes after a delivery hands it out again, what the subscription reports reaches the status.
 * Records of a type the processor does not handle are acknowledged without touching the message store.
 */
class EventConsumerSubscriptionTest {
    private val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(RecordingHandlers()) }
    private val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, JsonCodec()), mockk<MessageStore>(relaxed = true))
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
                    3 -> { delay(80); Resubscribe.Ended.Failed(EventStoreException.Failed("lived"), delivered = false) }
                    else -> Resubscribe.Ended.Stopped
                }
            },
            failed = { _, wait -> stable += wait },
        )
        assertEquals(listOf(10L, 20L, 10L), stable.map { it.toMillis() })
    }

    /** A message store whose markers always record (the handlers run) and whose transactions do nothing. */
    private fun markingStore(): MessageStore = mockk(relaxed = true) {
        coEvery { processedEvents.record(any()) } returns true
    }

    private fun happened(id: String): ByteArray = JsonCodec().encode(Happened(id, Instant.EPOCH))

    @Test
    fun aStopDuringSubscribeClosesTheSubscriptionItBuilt() {
        val events = ScriptedEventStore()
        events.subscribeGate = CompletableDeferred()
        val c = start(events)
        await().atMost(Duration.ofSeconds(10)).until { events.subscriptions.size == 1 } // built, subscribe() not returned yet
        val stopped = CompletableFuture.runAsync { c.stop() }
        Thread.sleep(200) // the stop is waiting for the subscribe in flight
        events.subscribeGate!!.complete(Unit)
        stopped.get(5, TimeUnit.SECONDS)
        assertTrue(events.subscriptions.single().closed, "the subscription built while the processor stopped is closed, not left in its group")
    }

    @Test
    fun aSubscriptionBuiltDuringAStopIsClosedWithoutReadingWhatItHasReady() {
        val events = ScriptedEventStore()
        events.subscribeGate = CompletableDeferred()
        val c = start(events)
        await().atMost(Duration.ofSeconds(10)).until { events.subscriptions.size == 1 } // built, subscribe() not returned yet
        val built = events.subscriptions.single()
        built.assign("0")
        built.deliver("0", "0", "test.Other") // ready: next() would hand it out without suspending
        val stopped = CompletableFuture.runAsync { c.stop() }
        Thread.sleep(200) // the stop is waiting for the subscribe in flight
        events.subscribeGate!!.complete(Unit)
        stopped.get(5, TimeUnit.SECONDS)
        assertTrue(built.closed, "the subscription built while the processor stopped is closed")
        assertEquals(emptyList<Pair<String, String>>(), built.acks, "nothing of it was read: the stop surfaced before the first next()")
    }

    @Test
    fun aRecordWhoseDeadLetterCouldNotBeWrittenRunsItsHandlersAgainAfterAResubscribe() {
        val events = ScriptedEventStore(capabilities = setOf(Capability.LAG, Capability.NATIVE_DEAD_LETTER))
        val handlers = RecordingHandlers().also { it.failAlways.set("p-1") }
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handlers) }
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, JsonCodec()), markingStore())
        consumer = EventConsumer(
            events, SubscriptionSpec("svc.default", "kraken.events"), dispatcher, maxAttempts = 1, backOffMs = 50,
            deadLetters = DeadLetterSink.EventStore(events), resubscribe = { Resubscribe(min = Duration.ofMillis(50)) },
        ).also { it.start() }
        events.failPublish = true
        awaitSubscriptions(events, 1)
        val first = events.subscriptions[0]
        first.assign("0")
        first.deliver("0", "0", "test.Happened", id = "p-1", payload = happened("p-1"))
        await().atMost(Duration.ofSeconds(10)).until { first.redelivered.isNotEmpty() }
        assertEquals(1, handlers.seen.size)

        first.fail("the consumer cannot go on")
        awaitSubscriptions(events, 2)
        events.failPublish = false
        val second = events.subscriptions[1]
        second.assign("0")
        second.deliver("0", "0", "test.Happened", id = "p-1", payload = happened("p-1"))
        await().atMost(Duration.ofSeconds(10)).until { second.acks == listOf("0" to "0") }
        assertEquals(2, handlers.seen.size, "a new subscription starts afresh: the handlers run again, as on another instance")
        assertEquals(listOf("kraken.events.dlt"), events.published.map { it.source })
    }

    @Test
    fun aReaderEndedByAnErrorShowsStoppedWithWhy() {
        val events = ScriptedEventStore()
        val c = start(events)
        awaitSubscriptions(events, 1)
        val sub = events.subscriptions[0]
        sub.assign("0")
        await().atMost(Duration.ofSeconds(10)).until { c.status().state == ProcessorState.PROCESSOR_RUNNING }
        sub.failWith(StackOverflowError("deep"))
        await().atMost(Duration.ofSeconds(10)).until { c.status().state == ProcessorState.PROCESSOR_STOPPED }
        assertEquals("reader stopped: StackOverflowError: deep", c.status().lastError)
        assertTrue(sub.closed)
    }

    /** A handler that waits until released. */
    class BlockingHandlers {
        val release = CountDownLatch(1)
        val seen = CopyOnWriteArrayList<String>()
        @EventHandler fun on(event: Happened) {
            seen += event.id
            release.await(30, TimeUnit.SECONDS)
        }
    }

    @Test
    fun aStopThatTimesOutNeverLetsASecondReaderStartBesideTheFirst() {
        val events = ScriptedEventStore()
        val handlers = BlockingHandlers()
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handlers) }
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, JsonCodec()), markingStore())
        val c = EventConsumer(
            events, SubscriptionSpec("svc.default", "kraken.events"), dispatcher, maxAttempts = 1, stopTimeout = Duration.ofMillis(300),
        ).also { consumer = it; it.start() }
        awaitSubscriptions(events, 1)
        events.subscriptions[0].assign("0")
        events.subscriptions[0].deliver("0", "0", "test.Happened", id = "b-1", payload = happened("b-1"))
        await().atMost(Duration.ofSeconds(10)).until { handlers.seen.size == 1 }

        val began = System.nanoTime()
        c.stop()
        assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofSeconds(3), "the stop gave up after its timeout")
        assertTrue(runCatching { c.start() }.exceptionOrNull() is IllegalStateException, "no second reader while the first runs a handler")
        assertEquals(1, events.subscriptions.size)
        assertTrue(!c.isRunning)

        handlers.release.countDown()
        await().atMost(Duration.ofSeconds(10)).until { events.subscriptions[0].closed }
        c.start()
        awaitSubscriptions(events, 2)
    }

    @Test
    fun aStartFromTheControlPlaneIsRefusedAtOnceWhileThePreviousReaderRuns() {
        val events = ScriptedEventStore()
        val handlers = BlockingHandlers()
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handlers) }
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, JsonCodec()), markingStore())
        val c = EventConsumer(
            events, SubscriptionSpec("svc.default", "kraken.events"), dispatcher, maxAttempts = 1, stopTimeout = Duration.ofSeconds(2),
        ).also { consumer = it; it.start() }
        awaitSubscriptions(events, 1)
        events.subscriptions[0].assign("0")
        events.subscriptions[0].deliver("0", "0", "test.Happened", id = "b-1", payload = happened("b-1"))
        await().atMost(Duration.ofSeconds(10)).until { handlers.seen.size == 1 }
        c.stop() // gives up after its 2 s: the reader still runs the handler

        val began = System.nanoTime()
        val refused = runCatching { c.start(waitForPrevious = false) }.exceptionOrNull()
        val took = Duration.ofNanos(System.nanoTime() - began)
        assertTrue(refused is IllegalStateException, "no second reader while the first runs a handler: $refused")
        assertTrue(took < Duration.ofMillis(500), "refused at once, without waiting for the previous reader: $took")
        assertTrue(!c.isRunning)

        handlers.release.countDown()
        await().atMost(Duration.ofSeconds(10)).until { events.subscriptions[0].closed }
        c.start(waitForPrevious = false)
        awaitSubscriptions(events, 2)
    }

    @Test
    fun aDeliveryHandedBackBecauseOfAPauseDoesNotResetTheBackoff() {
        val events = ScriptedEventStore()
        val c = start(events) { Resubscribe(min = Duration.ofMillis(300), max = Duration.ofSeconds(5)) }
        c.pause()
        for (n in 1..2) {
            awaitSubscriptions(events, n)
            val sub = events.subscriptions[n - 1]
            sub.assign("0")
            sub.deliver("0", "$n", "test.Other")
            await().atMost(Duration.ofSeconds(10)).until { sub.redelivered.isNotEmpty() }
            sub.fail("broken $n")
        }
        awaitSubscriptions(events, 3)
        val second = Duration.ofNanos(events.subscribedAt[2] - events.subscribedAt[1])
        assertTrue(second >= Duration.ofMillis(550), "the wait doubled (300 ms, then 600 ms): $second")
    }
}
