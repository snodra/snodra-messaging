package io.damoreira.kraken.messaging.test.eventstore

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.MAX_POSITION_BYTES
import io.damoreira.kraken.messaging.eventstore.MAX_SEGMENT_BYTES
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.eventstore.deadLetterSource
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.TreeSet
import java.util.UUID

/**
 * The conformance kit of the event store (ADR 0027): the behaviour every event-store add-on must have, a third party's
 * included. An add-on runs it by subclassing this class in its own tests:
 *
 * ```kotlin
 * class KafkaEventStoreConformanceTest : EventStoreConformanceTest() {
 *     override val source = "kraken.conformance"              // prepared with two segments and its .dlt
 *     override fun eventStore(): EventStore = KafkaEventStore(settingsFor(kafka))
 *     override fun messageStore(): MessageStore = mongoStore  // any message store add-on, its schema in place
 * }
 * ```
 *
 * - **The [source]** has at least two segments (the competing-instances check spreads them over two instances), and when the
 *   event store declares [Capability.NATIVE_DEAD_LETTER] its [deadLetterSource] exists too. The kit only appends to them, and
 *   must be their only publisher while it runs (the lag check reads the whole source).
 * - **Each call of [eventStore]** is one service instance: a store reading the same event store (an in-process store returns
 *   the same one each time).
 * - **[messageStore]** keeps the processed-event markers of the at-least-once check; only that check calls it.
 * - Each check is a test of its own, on subscription names of its own; a new name starts at the earliest position, so each
 *   check reads what the earlier ones appended and looks only at its own events (by their `id` header). A check of a
 *   capability the event store does not declare is skipped (one checks that it says so instead).
 * - The checks wait at most [wait] (a minute) for what the event store does in the background (an assignment, a lag refresh)
 *   and for every call they await: a publish, a subscribe, a `next` that does not give up when cancelled, a close or a
 *   [io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin] call that does not answer fails the check naming the step
 *   (each runs on a coroutine of its own, so a stuck one is left behind instead of hanging the test). A check closes the
 *   subscriptions it left open when it fails.
 */
abstract class EventStoreConformanceTest {
    /** The prepared source (see the class docs). */
    abstract val source: String

    /** One service instance of the event store under test, per call. */
    abstract fun eventStore(): EventStore

    /** The message store of [redeliveredAfterCommitIsSkipped], its schema in place. */
    abstract fun messageStore(): MessageStore

    /** How long a check waits for the event store (see the class docs); a slower event store's test may raise it. */
    protected open val wait: Duration = Duration.ofSeconds(60)

    private val readers = mutableListOf<Reader>()

    /** The check running, named in the failures of [bounded]. */
    private var check = ""

    /** Where the kit's calls of the event store run: a call that never answers is left behind there. */
    private val calls = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @AfterEach
    fun closeOpenSubscriptions() = runBlocking {
        for (reader in readers) runCatching { reader.close() } // a check that failed on a close fails once
        calls.cancel()
    }

    /** Runs the check [name]. */
    private fun check(name: String, block: suspend CoroutineScope.() -> Unit) = runBlocking {
        check = name
        block()
    }

    // ---------------------------------------------------------------------------------------------------------- the contract

    /** The seven headers, any other header, the key (or none) and the payload arrive as published. */
    @Test
    fun headersRoundTrip() = check("headersRoundTrip") {
        val events = eventStore()
        val keyed = event(source, fresh("key"), extra = mapOf("x-conformance" to "kept as given"))
        val unkeyed = event(source, null)
        publishAll(events, listOf(keyed, unkeyed))

        val reader = join(events, spec())
        val ids = ids(listOf(keyed, unkeyed))
        reader.until("both events delivered") { it.hasAll(ids) }
        for (sent in listOf(keyed, unkeyed)) {
            val got = reader.first(sent.id)
            for (name in EventHeaders.ALL) assertTrue(name in got.headers, "header $name delivered")
            for ((name, value) in sent.headers) assertEquals(value, got.headers[name], "header $name")
            assertEquals(sent.key, got.key, "the key")
            assertEquals(String(sent.payload, Charsets.UTF_8), String(got.payload, Charsets.UTF_8), "the payload")
            assertTrue(got.timestampMs > 0, "an append time: ${got.timestampMs}")
        }
        reader.close()
    }

    /** Events of one key arrive in publish order, all from one segment, each once; a segment's positions increase by [EventStore.compare]. */
    @Test
    fun sameKeyArrivesInPublishOrder() = check("sameKeyArrivesInPublishOrder") {
        val events = eventStore()
        val keys = (0 until 4).map { fresh("key") }
        val sent = (0 until 10).flatMap { keys.map { key -> event(source, key) } }
        publishAll(events, sent)

        val reader = join(events, spec())
        val all = ids(sent)
        reader.until("every event delivered") { it.hasAll(all) }
        val got = reader.mine(all)
        assertEquals(sent.size, got.size, "each event once")
        for (key in keys) {
            val published = sent.filter { it.key == key }.map { it.id }
            val delivered = got.filter { it.key == key }
            assertEquals(published, delivered.map(::idOf), "key $key: publish order")
            val segments = delivered.map { it.segment }.toSet()
            assertEquals(1, segments.size, "key $key: one segment, got $segments")
        }
        val last = HashMap<String, String>()
        for (d in got) {
            val before = last.put(d.segment, d.position) ?: continue
            assertTrue(events.compare(before, d.position) < 0, "segment ${d.segment}: $before before ${d.position}")
        }
        reader.close()
    }

    /** Two subscription names (two processors) each receive every event. */
    @Test
    fun everyProcessorGetsEveryEvent() = check("everyProcessorGetsEveryEvent") {
        val events = eventStore()
        val sent = (0 until 10).map { event(source, fresh("key")) }
        publishAll(events, sent)

        val a = join(events, spec())
        val b = join(events, spec())
        val all = ids(sent)
        bothUntil(a, b, "both processors get every event") { x, y -> x.hasAll(all) && y.hasAll(all) }
        a.close()
        b.close()
    }

    /** Two instances of one subscription split its segments (adding the second spreads them) and share its events: each event goes to one of them, and both get some. */
    @Test
    fun instancesShareAProcessor() = check("instancesShareAProcessor") {
        val first = eventStore()
        val second = eventStore()
        val spec = spec()
        val a = join(first, spec)
        a.until("the first instance holds segments") { it.assigned.isNotEmpty() }
        val b = join(second, spec)
        bothUntil(a, b, "both instances hold segments, none held by both") { x, y ->
            x.assigned.isNotEmpty() && y.assigned.isNotEmpty() && x.assigned.none { it in y.assigned }
        }

        val sent = (0 until 40).map { event(source, fresh("key")) }
        publishAll(first, sent)
        val all = ids(sent)
        bothUntil(a, b, "every event delivered") { x, y -> all.all { x.has(it) || y.has(it) } }
        val toA = a.mine(all).map(::idOf).toSet()
        val toB = b.mine(all).map(::idOf).toSet()
        assertTrue(toA.isNotEmpty() && toB.isNotEmpty(), "both instances get events: ${toA.size} and ${toB.size}")
        assertEquals(emptySet<String>(), toA intersect toB, "delivered to both instances")
        a.close()
        b.close()
    }

    /**
     * What a subscription did not acknowledge comes again: to the next subscription of the name after a close, and on a live
     * one after [Subscription.redeliver]; what it acknowledged does not.
     */
    @Test
    fun unackedDeliveryIsRedelivered() = check("unackedDeliveryIsRedelivered") {
        val events = eventStore()
        val spec = spec()
        val key = fresh("key")
        val sent = (0 until 3).map { event(source, key) }
        val all = ids(sent)
        val unacked = all.drop(1)
        publishAll(events, sent)

        val first = join(events, spec)
        first.hold(unacked)
        first.until("the three events delivered") { it.hasAll(all) }
        first.close()

        val second = join(eventStore(), spec)
        second.hold(unacked)
        second.until("the unacknowledged events again") { it.hasAll(unacked) }
        assertEquals(unacked, second.idsInOrder(all), "after a close: the unacknowledged ones, in order, not the acknowledged one")

        second.redeliver(second.first(unacked[0]))
        second.until("the unacknowledged events once more") { it.mine(unacked).size == 4 }
        assertEquals(unacked + unacked, second.idsInOrder(all), "after redeliver: from the redelivered one on, in order")
        second.ack(second.first(unacked[1]))
        second.close()
    }

    /**
     * A crash after the handler's transaction committed and before the acknowledgement: the event comes again, its
     * processed-event marker (recorded in that transaction, its id built from the delivered `id` header as the core builds it)
     * makes it a duplicate, and once acknowledged it does not come again. The core's own marker path over an event store is
     * proven by the add-on's and the core's suites; this check pins what the event store owes it.
     */
    @Test
    fun redeliveredAfterCommitIsSkipped() = check("redeliveredAfterCommitIsSkipped") {
        val store = messageStore()
        val events = eventStore()
        val spec = spec()
        val key = fresh("key")
        val handled = event(source, key)
        publishAll(events, listOf(handled))
        val only = listOf(handled.id)
        // Built from the delivered `id` header, as the core builds it.
        fun marker(d: Delivery) = ProcessedMarker("processor:${spec.name}:${idOf(d)}", EVENT_TYPE, Instant.now(), processor = spec.name)

        val first = join(events, spec)
        first.hold(only)
        first.until("the event delivered") { it.hasAll(only) }
        val firstMarker = marker(first.first(handled.id))
        assertTrue(inTransaction(store) { store.processedEvents.record(firstMarker) }, "the first handling records its marker")
        first.close() // the crash: committed, never acknowledged

        val second = join(eventStore(), spec)
        second.hold(only)
        second.until("the event again after the crash") { it.hasAll(only) }
        val again = second.first(handled.id)
        val recorded = inTransaction(store, rollback = true) { store.processedEvents.record(marker(again)) }
        assertFalse(recorded, "the redelivered event is a duplicate")
        second.ack(again)
        second.close()

        val fence = event(source, key)
        publishAll(events, listOf(fence))
        val third = join(eventStore(), spec)
        third.until("the next event of the key") { it.has(fence.id) }
        assertFalse(third.has(handled.id), "the skipped event, acknowledged, does not come again")
        third.close()
    }

    /** A paused subscription delivers nothing (paused before its first `next` too) while its assignment goes on; resumed, it delivers from where it stopped, each event once, in order. */
    @Test
    fun pausedSubscriptionReceivesNothingAndResumesInPlace() = check("pausedSubscriptionReceivesNothingAndResumesInPlace") {
        val events = eventStore()
        val key = fresh("key")
        val before = (0 until 3).map { event(source, key) }
        val during = (0 until 5).map { event(source, key) }
        publishAll(events, before)

        val reader = join(events, spec())
        reader.subscription.pause()
        reader.pump(QUIET_MS)
        assertEquals(0, reader.seen.size, "paused from the start: nothing delivered")
        reader.subscription.resume()
        reader.until("the events before the pause") { it.hasAll(ids(before)) }

        reader.subscription.pause()
        val delivered = reader.seen.size
        publishAll(events, during)
        reader.pump(QUIET_MS)
        assertEquals(delivered, reader.seen.size, "nothing delivered while paused")
        reader.subscription.resume()
        reader.until("the events published while paused") { it.hasAll(ids(during)) }
        val all = ids(before) + ids(during)
        assertEquals(all, reader.idsInOrder(all), "each once, in order")
        reader.close()
    }

    // ---------------------------------------------------------------------------------------------------------- capabilities

    /**
     * `replay`: while an instance reads the subscription, its positions do not move; with none, a reset to earliest replays
     * what it had acknowledged. The plan of each target moves the segments as [io.damoreira.kraken.messaging.eventstore.ReplayRange] says.
     */
    @Test
    fun replayFromEarliestRedeliversHistory() = check("replayFromEarliestRedeliversHistory") {
        val events = eventStore()
        assumeTrue(events.supports(Capability.REPLAY), "the event store does not declare replay")
        val spec = spec()
        val sent = (0 until 3).map { event(source, fresh("key")) }
        val all = ids(sent)
        publishAll(events, sent)

        val first = join(events, spec)
        first.until("the events delivered") { it.hasAll(all) }
        val delivered = first.mine(all)
        val admin = events.admin(spec)
        var deadline = System.nanoTime() + wait.toNanos()
        while (bounded("admin members") { admin.members() }.isEmpty()) {
            if (System.nanoTime() > deadline) fail<Unit>("the reading instance is a member within ${wait.seconds} s")
            first.pump(SLICE_MS)
        }
        val planned = bounded("admin plan Earliest") { admin.plan(ResetTarget.Earliest) }
        val refused = runCatching { bounded("admin apply") { admin.apply(planned) } }.exceptionOrNull()
        assertInstanceOf(EventStoreException.SubscribersActive::class.java, refused, "no move while an instance reads: $refused")
        first.close()
        deadline = System.nanoTime() + wait.toNanos()
        while (true) {
            val members = bounded("admin members") { admin.members() }
            if (members.isEmpty()) break
            if (System.nanoTime() > deadline) fail<Unit>("members after close: $members")
            delay(SLICE_MS)
        }

        fun before(a: String, b: String) = events.compare(a, b) < 0
        val earliest = bounded("admin plan Earliest") { admin.plan(ResetTarget.Earliest) }
        for (d in delivered) {
            assertTrue(
                earliest.any { r ->
                    r.segment == d.segment && r.source == source && r.until.isNotEmpty() && !before(d.position, r.from) && before(d.position, r.until)
                },
                "earliest replays ${d.position} of segment ${d.segment}: $earliest",
            )
        }
        val latest = bounded("admin plan Latest") { admin.plan(ResetTarget.Latest) }
        for (d in delivered) assertTrue(latest.any { it.segment == d.segment }, "latest moves segment ${d.segment}: $latest")
        assertTrue(latest.all { it.until.isEmpty() || !before(it.from, it.until) }, "latest replays nothing: $latest")
        val oldest = delivered.first()
        val at = bounded("admin plan Timestamp") { admin.plan(ResetTarget.Timestamp(oldest.timestampMs)) }
        assertTrue(
            at.any { it.segment == oldest.segment && !before(oldest.position, it.from) },
            "a time moves to the first position at or after it: $at",
        )
        val newest = delivered.maxOf { it.timestampMs }
        val past = bounded("admin plan Timestamp") { admin.plan(ResetTarget.Timestamp(newest + 1)) }
        for (d in delivered) {
            assertTrue(
                past.any { it.segment == d.segment && before(d.position, it.from) },
                "a time after every event of segment ${d.segment} moves past ${d.position}: $past",
            )
        }
        val positions = bounded("admin plan Positions") { admin.plan(ResetTarget.Positions(mapOf(oldest.segment to oldest.position))) }
        assertEquals(listOf(oldest.segment to oldest.position), positions.map { it.segment to it.from }, "positions move the listed segments only")
        // A segment the source lacks, and a position the segment does not have (far past any head of an event store with
        // decimal positions, not a position at all for the others).
        for (target in listOf(
            ResetTarget.Positions(mapOf("conformance-no-such-segment" to oldest.position)),
            ResetTarget.Positions(mapOf(oldest.segment to Long.MAX_VALUE.toString())),
        )) {
            val invalid = runCatching { bounded("admin plan Positions") { admin.plan(target) } }.exceptionOrNull()
            assertInstanceOf(EventStoreException.Invalid::class.java, invalid, "$target is invalid: $invalid")
        }

        bounded("admin apply") { admin.apply(earliest) }
        val second = join(eventStore(), spec)
        second.until("the acknowledged events replayed") { it.hasAll(all) }
        second.close()
    }

    /** Without `replay`: the reset side of a subscription is refused as unsupported. */
    @Test
    fun adminWithoutReplayIsUnsupported() = check("adminWithoutReplayIsUnsupported") {
        val events = eventStore()
        assumeFalse(events.supports(Capability.REPLAY), "the event store declares replay")
        val refused = runCatching { events.admin(spec()) }.exceptionOrNull()
        assertInstanceOf(EventStoreException.Unsupported::class.java, refused, "admin without replay: $refused")
        assertEquals(Capability.REPLAY, (refused as EventStoreException.Unsupported).capability)
    }

    /** Without `lag`: no segment reports a lag. */
    @Test
    fun lagUnknownWithoutCapability() = check("lagUnknownWithoutCapability") {
        val events = eventStore()
        assumeFalse(events.supports(Capability.LAG), "the event store declares lag")
        val sent = event(source, fresh("key"))
        publishAll(events, listOf(sent))
        val reader = join(events, spec())
        reader.until("the event delivered") { it.has(sent.id) }
        val segments = reader.subscription.segments()
        assertTrue(segments.isNotEmpty(), "segments() lists the assignment")
        assertTrue(segments.all { it.lag == null }, "no lag without the capability: $segments")
        reader.close()
    }

    /** `lag`: each segment's lag counts what the subscription has not acknowledged. */
    @Test
    fun lagCountsUnacked() = check("lagCountsUnacked") {
        val events = eventStore()
        assumeTrue(events.supports(Capability.LAG), "the event store does not declare lag")
        val reader = join(events, spec())
        reader.until("the subscription holds segments") { it.assigned.isNotEmpty() }
        // Every assigned segment gets an acknowledged event first, so its lag counts from a committed position (with nothing
        // committed it counts from the subscription's position, which the unacknowledged deliveries below move too).
        var deadline = System.nanoTime() + wait.toNanos()
        while (!reader.assigned.all { segment -> reader.seen.any { it.segment == segment } }) {
            if (System.nanoTime() > deadline) fail<Unit>("an acknowledged event on every assigned segment within ${wait.seconds} s")
            val primed = (0 until 10).map { event(source, fresh("key")) }
            publishAll(events, primed)
            reader.until("the priming events delivered") { it.hasAll(ids(primed)) }
        }
        deadline = System.nanoTime() + wait.toNanos()
        while (true) {
            reader.until("everything acknowledged, lag 0") { it.assigned.isNotEmpty() && it.knownLag() == 0L }
            val delivered = reader.seen.size
            reader.pump(QUIET_MS)
            if (reader.seen.size == delivered && reader.knownLag() == 0L) break
            if (System.nanoTime() > deadline) fail<Unit>("the source drained within ${wait.seconds} s")
        }
        assertEquals(reader.assigned.toList(), reader.subscription.segments().map { it.segment }, "segments() lists the assignment, sorted")

        val sent = (0 until 5).map { event(source, fresh("key")) }
        val all = ids(sent)
        reader.hold(all)
        publishAll(events, sent)
        reader.until("the events delivered") { it.hasAll(all) }
        val held = reader.mine(all).map { it.segment }
        reader.until("the lag counts the 5 unacknowledged events") { it.knownLag(held) == 5L }
        for (d in reader.mine(all)) reader.ack(d)
        reader.until("the lag back to 0 once acknowledged") { it.knownLag(held) == 0L }
        reader.close()
    }

    /** `native-dead-letter`: the dead-letter source takes a letter (its `kraken-dlt-*` headers included) and delivers it as published. */
    @Test
    fun deadLetterSourceReceives() = check("deadLetterSourceReceives") {
        val events = eventStore()
        assumeTrue(events.supports(Capability.NATIVE_DEAD_LETTER), "the event store does not declare native-dead-letter")
        val dlt = deadLetterSource(source)
        val letter = event(
            dlt, fresh("key"),
            extra = mapOf("kraken-dlt-processor" to "conformance", "kraken-dlt-reason" to "FAILED", "kraken-dlt-error" to "the handler failed"),
        )
        publishAll(events, listOf(letter))
        val reader = join(events, spec().copy(source = dlt))
        reader.until("the letter delivered from the dead-letter source") { it.has(letter.id) }
        val got = reader.first(letter.id)
        for ((name, value) in letter.headers) assertEquals(value, got.headers[name], "header $name")
        assertEquals(letter.key, got.key, "the key")
        assertEquals(String(letter.payload, Charsets.UTF_8), String(got.payload, Charsets.UTF_8), "the payload")
        reader.close()
    }

    // --------------------------------------------------------------------------------------------------------------- helpers

    /** A subscription name nothing has used. */
    private fun spec() = SubscriptionSpec("conformance.${UUID.randomUUID().toString().replace("-", "")}", source)

    private suspend fun join(events: EventStore, spec: SubscriptionSpec): Reader {
        val subscription = try {
            bounded("subscribe ${spec.name}") { events.subscribe(spec) }
        } catch (e: EventStoreException) {
            throw AssertionError("subscribe ${spec.name}: ${e.message}", e)
        }
        return Reader(spec.name, subscription).also { readers += it }
    }

    private suspend fun publishAll(events: EventStore, sent: List<OutgoingEvent>) {
        for (e in sent) {
            try {
                bounded("publish ${e.id}") { events.publish(e) }
            } catch (x: EventStoreException) {
                throw AssertionError("publish ${e.id}: ${x.message}", x)
            }
        }
    }

    /** [call]'s outcome, run on a coroutine of its own; fails naming the check and [step] when it does not answer within [wait]. */
    private suspend fun <T> bounded(step: String, call: suspend () -> T): T {
        val running = calls.async { call() }
        val outcome = withTimeoutOrNull(wait.toMillis()) { Result.success(running.await()) }
        if (outcome != null) return outcome.getOrThrow()
        running.cancel()
        fail<Nothing>("check `$check`: $step: no answer within ${wait.seconds} s")
    }

    /** Reads both until [done], at most a minute. */
    private suspend fun bothUntil(a: Reader, b: Reader, what: String, done: (Reader, Reader) -> Boolean) {
        val deadline = System.nanoTime() + wait.toNanos()
        while (!done(a, b)) {
            if (System.nanoTime() > deadline) {
                fail<Unit>("$what within ${wait.seconds} s (assigned ${a.assigned} and ${b.assigned}, ${a.seen.size} and ${b.seen.size} deliveries)")
            }
            coroutineScope {
                launch { a.pump(SLICE_MS) }
                launch { b.pump(SLICE_MS) }
            }
        }
    }

    /** Runs [block] in a transaction of [store] on this thread, as the core does: committed, or rolled back when [rollback]. */
    private fun <T> inTransaction(store: MessageStore, rollback: Boolean = false, block: suspend () -> T): T {
        var result: Result<T>? = null
        TransactionTemplate(store.transactions).executeWithoutResult { status ->
            result = Result.success(runBlocking { block() })
            if (rollback) status.setRollbackOnly()
        }
        return result!!.getOrThrow()
    }

    /**
     * One subscription and what it gave: its assignment and its deliveries, in order. Every delivery is acknowledged as it
     * comes but the held ones; every one is checked against the assignment and the segment and position bounds.
     */
    private inner class Reader(val name: String, private var open: Subscription?) {
        val assigned = TreeSet<String>()
        val seen = mutableListOf<Delivery>()
        private val held = HashSet<String>()

        val subscription: Subscription get() = open ?: error("$name is closed")

        /** The events with these ids are not acknowledged as they come. */
        fun hold(ids: List<String>) {
            held += ids
        }

        fun ack(d: Delivery) = try {
            subscription.ack(d.segment, d.position)
        } catch (e: EventStoreException) {
            throw AssertionError("$name: ack ${d.segment} ${d.position}: ${e.message}", e)
        }

        fun redeliver(d: Delivery) = try {
            subscription.redeliver(d.segment, d.position)
        } catch (e: EventStoreException) {
            throw AssertionError("$name: redeliver ${d.segment} ${d.position}: ${e.message}", e)
        }

        /** Takes what the subscription gives for [sliceMs]. */
        suspend fun pump(sliceMs: Long) {
            val deadline = System.nanoTime() + sliceMs * 1_000_000
            while (true) {
                val left = (deadline - System.nanoTime()) / 1_000_000
                if (left <= 0) return
                val event = try {
                    next(left) ?: return
                } catch (e: EventStoreException) {
                    throw AssertionError("$name: next: ${e.message}", e)
                }
                take(event)
            }
        }

        /**
         * The next event within [timeoutMs], or null. `next` runs on a coroutine of its own, cancelled when the time is up (it
         * loses nothing then, by its contract); one that ignores the cancellation fails the check after [wait].
         */
        private suspend fun next(timeoutMs: Long): SubscriptionEvent? {
            val running = calls.async { subscription.next() }
            withTimeoutOrNull(timeoutMs) { running.join() }
            running.cancel()
            bounded("next of $name, cancelled") { running.join() }
            return when (val error = running.getCompletionExceptionOrNull()) {
                null -> running.getCompleted()
                is CancellationException -> null
                else -> throw error
            }
        }

        private fun take(event: SubscriptionEvent) {
            when (event) {
                is SubscriptionEvent.Assigned -> for (segment in event.segments) {
                    assertTrue(
                        segment.isNotEmpty() && segment.toByteArray(Charsets.UTF_8).size <= MAX_SEGMENT_BYTES,
                        "$name: a segment name of 1 to $MAX_SEGMENT_BYTES bytes: \"$segment\"",
                    )
                    assigned += segment
                }
                is SubscriptionEvent.Revoked -> assigned -= event.segments.toSet()
                is Delivery -> {
                    assertTrue(event.segment in assigned, "$name: a delivery of segment ${event.segment} it does not hold ($assigned)")
                    assertTrue(
                        event.position.isNotEmpty() && event.position.toByteArray(Charsets.UTF_8).size <= MAX_POSITION_BYTES,
                        "$name: a position of 1 to $MAX_POSITION_BYTES bytes: \"${event.position}\"",
                    )
                    if (idOf(event) !in held) ack(event)
                    seen += event
                }
            }
        }

        /** Reads until [done], at most a minute. */
        suspend fun until(what: String, done: (Reader) -> Boolean) {
            val deadline = System.nanoTime() + wait.toNanos()
            while (!done(this)) {
                if (System.nanoTime() > deadline) fail<Unit>("$name: $what within ${wait.seconds} s (assigned $assigned, ${seen.size} deliveries)")
                pump(SLICE_MS)
            }
        }

        fun has(id: String) = seen.any { idOf(it) == id }

        fun hasAll(ids: List<String>) = ids.all(::has)

        /** The deliveries of the events with these ids, in order. */
        fun mine(ids: List<String>): List<Delivery> = ids.toSet().let { wanted -> seen.filter { idOf(it) in wanted } }

        fun idsInOrder(ids: List<String>): List<String> = mine(ids).map(::idOf)

        /** The first delivery of the event [id]. */
        fun first(id: String): Delivery = seen.firstOrNull { idOf(it) == id } ?: fail("$name: $id delivered")

        /** The lag of the assigned segments whose lag is known, summed; null when none is known or one of [required] is not. */
        fun knownLag(required: List<String> = emptyList()): Long? {
            val segments = subscription.segments()
            if (required.any { r -> segments.none { it.segment == r && it.lag != null } }) return null
            val known = segments.mapNotNull { it.lag }
            return if (known.isEmpty()) null else known.sum()
        }

        /** Leaves the subscription; idempotent. */
        suspend fun close() {
            val subscription = open ?: return
            open = null
            bounded("close $name") { subscription.close() }
        }
    }

    private companion object {
        /** How long a check watches a subscription that must deliver nothing. */
        const val QUIET_MS = 3_000L

        /** One turn of reading a subscription. */
        const val SLICE_MS = 250L

        /** The type of the kit's events. */
        const val EVENT_TYPE = "conformance.Event"

        val timestamps: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

        fun fresh(what: String) = "$what-${UUID.randomUUID().toString().replace("-", "")}"

        /** An event of the kit to [source]: the seven headers (and [extra]), a fresh id, a JSON object payload. */
        fun event(source: String, key: String?, extra: Map<String, String> = emptyMap()): OutgoingEvent {
            val id = UUID.randomUUID().toString()
            val trace = UUID.randomUUID().toString().replace("-", "")
            val span = UUID.randomUUID().toString().replace("-", "").take(16)
            val headers = mapOf(
                EventHeaders.TYPE to EVENT_TYPE,
                EventHeaders.ID to id,
                EventHeaders.CORRELATION_ID to fresh("correlation"),
                EventHeaders.TIMESTAMP to timestamps.format(Instant.now()),
                EventHeaders.SOURCE to "conformance",
                EventHeaders.USER_ID to "conformance-user",
                EventHeaders.TRACEPARENT to "00-$trace-$span-01",
            ) + extra
            return OutgoingEvent(source, id, EVENT_TYPE, key, headers, """{"id":"$id","amount":42}""".toByteArray())
        }

        fun ids(sent: List<OutgoingEvent>) = sent.map { it.id }

        fun idOf(d: Delivery): String = d.headers[EventHeaders.ID].orEmpty()
    }
}
