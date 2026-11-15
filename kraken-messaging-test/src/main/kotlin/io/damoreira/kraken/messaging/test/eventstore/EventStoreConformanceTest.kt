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
import io.damoreira.kraken.messaging.eventstore.SegmentOrder
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
import org.junit.jupiter.api.BeforeEach
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
 *   and for every call they await: [eventStore] and [messageStore], a publish, a subscribe, a `next` that does not give up
 *   when cancelled, a close, a [io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin] call or a message store call that
 *   does not answer fails the check naming the step (each runs on a coroutine of its own, so a stuck one is left behind
 *   instead of hanging the test). A check closes the subscriptions it left open when it fails.
 * - **`next` is cancelled** by the kit every time a slice of reading is up, as the core cancels it on its shutdown and timers
 *   (see [io.damoreira.kraken.messaging.eventstore.Subscription.next]). One that loses an event it was about to return when it
 *   is cancelled shows here as a delivery that never comes: the check names the events it waited for.
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

    /** Where the kit's calls of the event store run: a call that never answers is left behind there. One per test (a subclass may share its instance between tests). */
    private var calls = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @BeforeEach
    fun startCheck() {
        calls = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        readers.clear()
    }

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
        val events = newEventStore()
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
        val events = newEventStore()
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
        val events = newEventStore()
        val sent = (0 until 10).map { event(source, fresh("key")) }
        publishAll(events, sent)

        val a = join(events, spec())
        val b = join(events, spec())
        val all = ids(sent)
        bothUntil(a, b, "both processors get every event") { x, y -> x.hasAll(all) && y.hasAll(all) }
        a.close()
        b.close()
    }

    /**
     * Two instances of one subscription split its segments (adding the second spreads them, every revocation closed by an
     * assignment, possibly empty) and share its events: each event goes to one of them, and both get some.
     */
    @Test
    fun instancesShareAProcessor() = check("instancesShareAProcessor") {
        val first = newEventStore()
        val second = newEventStore()
        val spec = spec()
        val a = join(first, spec)
        a.until("the first instance holds segments") { it.assigned.isNotEmpty() }
        val b = join(second, spec)
        bothUntil(a, b, "both instances hold segments, none held by both, every revocation closed by an assignment") { x, y ->
            x.assigned.isNotEmpty() && y.assigned.isNotEmpty() && x.assigned.none { it in y.assigned } && !x.changing && !y.changing
        }
        // What the second instance took over, the first was told to give up (whether the event store then assigns the rest
        // again or leaves it, the first's assignment ends as `assigned` says: Revoked, then Assigned, in the order given).
        assertTrue(b.assigned.all { it in a.revoked }, "the first instance was told the segments ${b.assigned} went away (revoked ${a.revoked})")

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
     * Instances join one subscription one at a time, one more than its segments, each reading the whole time: every newcomer's
     * first event is an [SubscriptionEvent.Assigned] (an empty one when it holds nothing, as the last one may), within
     * [Subscription.FIRST_ASSIGNMENT_WITHIN] of joining, so an instance that joined and holds nothing is a member, idle, and never
     * reads `STARTING`.
     */
    @Test
    fun aJoiningInstanceAnnouncesItsFirstAssignment() = check("aJoiningInstanceAnnouncesItsFirstAssignment") {
        val spec = spec()
        val readers = mutableListOf(join(newEventStore(), spec))
        readers[0].until("the first instance holds segments") { it.assigned.isNotEmpty() }
        readers[0].pump(QUIET_MS)
        val segments = readers[0].assigned.size
        val within = Subscription.FIRST_ASSIGNMENT_WITHIN
        while (readers.size <= segments) {
            val newcomer = join(newEventStore(), spec)
            readers += newcomer
            val deadline = System.nanoTime() + within.toNanos()
            while (newcomer.first == null) {
                if (System.nanoTime() > deadline) {
                    fail<Unit>(
                        "instance ${readers.size} of ${segments + 1} (assigned ${readers.map { it.assigned }}): its first assignment, " +
                            "possibly empty, within ${within.seconds} s of joining",
                    )
                }
                pumpAll(readers)
            }
            assertEquals("Assigned", newcomer.first, "instance ${readers.size}: the first event of a new subscription is an Assigned")
        }
        for (reader in readers) reader.close()
    }

    /**
     * More instances than segments: instances join one at a time. After each join every instance is read until a revocation
     * comes (or a window passes: the newcomer may be left idle); then the segments must be spread (none held twice, every one
     * held, and once there are two instances no former holder — an instance that held segments before the join — holds every
     * segment; how the segments spread beyond that is the event store's, a third party's assignor is not constrained) with every
     * instance's last assignment change closed (a [SubscriptionEvent.Revoked] followed by an [SubscriptionEvent.Assigned], an
     * empty one for an instance left without segments). The instances join until one that held segments holds none (or there are
     * twice as many instances as segments, two more; a run that reaches that cap without such an instance logs it, the check then
     * proved less): that instance saw its revocation closed by an empty assignment, so the core reports it running, idle, never
     * starting.
     *
     * After each join the check watches for the change the join may cause: until a revocation somewhere, or until the newcomer
     * saw its first change (its first `Assigned`, empty when it is left idle: [aJoiningInstanceAnnouncesItsFirstAssignment]) and
     * a slice then passed with no event at any instance, at most a window of [JOIN_WINDOW_MS].
     */
    @Test
    fun anInstanceLeftWithoutSegmentsClosesItsChange() = check("anInstanceLeftWithoutSegmentsClosesItsChange") {
        val spec = spec()
        val readers = mutableListOf(join(newEventStore(), spec))
        readers[0].until("the first instance holds segments") { it.assigned.isNotEmpty() }
        // Alone, it holds every segment (in time: an assignment may come in parts).
        readers[0].pump(QUIET_MS)
        val all = TreeSet(readers[0].assigned)
        var formerHolders: List<Reader> = readers.toList()
        fun settled(): Boolean {
            val held = readers.flatMap { it.assigned }
            return held.size == held.toSet().size && held.toSet() == all && readers.none { it.changing } &&
                (readers.size < 2 || formerHolders.none { it.assigned.containsAll(all) })
        }
        var emptied = false
        while (!emptied && readers.size < 2 * all.size + 2) {
            val revocationsBefore = readers.sumOf { it.revocations }
            formerHolders = readers.filter { it.assigned.isNotEmpty() }
            val newcomer = join(newEventStore(), spec)
            readers += newcomer
            // The join may move segments (a revocation somewhere) or leave the newcomer idle: read everyone until a revocation, at
            // most a window; once the newcomer saw its first change and a slice passes with nothing anywhere, it is over.
            val windowEnd = System.nanoTime() + JOIN_WINDOW_MS * 1_000_000
            while (readers.sumOf { it.revocations } == revocationsBefore && System.nanoTime() < windowEnd) {
                val taken = readers.sumOf { it.taken }
                pumpAll(readers)
                if (newcomer.changes > 0 && readers.sumOf { it.taken } == taken) break
            }
            allUntil(readers, "the segments spread, every revocation closed by an assignment") { settled() }
            emptied = readers.any { it.assigned.isEmpty() && it.revocations > 0 }
        }
        allUntil(readers, "the segments spread, every revocation closed by an assignment") { settled() }
        assertTrue(readers.any { it.assigned.isEmpty() }, "more instances than segments leave one without segments")
        if (!emptied) {
            System.getLogger(EventStoreConformanceTest::class.java.name).log(
                System.Logger.Level.INFO,
                "check `$check`: ${readers.size} instances on ${all.size} segments and no instance that held segments was left without any " +
                    "(the event store keeps its first instances' segments): only the joins' revocations were checked",
            )
        }
        for (reader in readers) reader.close()
    }

    /**
     * What a subscription did not acknowledge comes again: to the next subscription of the name after a close, and on a live
     * one after [Subscription.redeliver]; what it acknowledged does not.
     */
    @Test
    fun unackedDeliveryIsRedelivered() = check("unackedDeliveryIsRedelivered") {
        val events = newEventStore()
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

        val second = join(newEventStore(), spec)
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
        val store = newMessageStore()
        val events = newEventStore()
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
        assertTrue(inTransaction("record the first marker", store) { store.processedEvents.record(firstMarker) }, "the first handling records its marker")
        first.close() // the crash: committed, never acknowledged

        val second = join(newEventStore(), spec)
        second.hold(only)
        second.until("the event again after the crash") { it.hasAll(only) }
        val again = second.first(handled.id)
        val recorded = inTransaction("record the marker of the redelivery", store, rollback = true) { store.processedEvents.record(marker(again)) }
        assertFalse(recorded, "the redelivered event is a duplicate")
        second.ack(again)
        second.close()

        val fence = event(source, key)
        publishAll(events, listOf(fence))
        val third = join(newEventStore(), spec)
        third.until("the next event of the key") { it.has(fence.id) }
        assertFalse(third.has(handled.id), "the skipped event, acknowledged, does not come again")
        third.close()
    }

    /** A paused subscription delivers nothing (paused before its first `next` too) while its assignment goes on; resumed, it delivers from where it stopped, each event once, in order. */
    @Test
    fun pausedSubscriptionReceivesNothingAndResumesInPlace() = check("pausedSubscriptionReceivesNothingAndResumesInPlace") {
        val events = newEventStore()
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
        val events = newEventStore()
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
        assertEquals(earliest.map { it.segment }.sortedWith(SegmentOrder), earliest.map { it.segment }, "a plan lists its segments in segment order")
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
        // Events of one millisecond share a timestamp, so the checks use only what a time decides: an event strictly before it is
        // moved past, an event at or after it is not (the first position at or after the time is no later than the event's).
        val oldest = delivered.first()
        val timestamps = delivered.map { it.timestampMs }.sorted()
        val atTime = timestamps[timestamps.size / 2]
        val at = bounded("admin plan Timestamp") { admin.plan(ResetTarget.Timestamp(atTime)) }
        for (d in delivered) {
            val from = at.firstOrNull { it.segment == d.segment }?.from ?: fail("a time moves segment ${d.segment}: $at")
            if (d.timestampMs < atTime) {
                assertTrue(before(d.position, from), "a time moves past ${d.position} of segment ${d.segment} (appended before it): $at")
            } else {
                assertTrue(!before(d.position, from), "a time keeps ${d.position} of segment ${d.segment} (appended at or after it): $at")
            }
        }
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
        val second = join(newEventStore(), spec)
        second.until("the acknowledged events replayed") { it.hasAll(all) }
        second.close()
    }

    /** Without `replay`: the reset side of a subscription is refused as unsupported. */
    @Test
    fun adminWithoutReplayIsUnsupported() = check("adminWithoutReplayIsUnsupported") {
        val events = newEventStore()
        assumeFalse(events.supports(Capability.REPLAY), "the event store declares replay")
        val refused = runCatching { events.admin(spec()) }.exceptionOrNull()
        assertInstanceOf(EventStoreException.Unsupported::class.java, refused, "admin without replay: $refused")
        assertEquals(Capability.REPLAY, (refused as EventStoreException.Unsupported).capability)
    }

    /** Without `lag`: no segment reports a lag. */
    @Test
    fun lagUnknownWithoutCapability() = check("lagUnknownWithoutCapability") {
        val events = newEventStore()
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
        val events = newEventStore()
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
        assertEquals(
            reader.assigned.sortedWith(SegmentOrder), reader.subscription.segments().map { it.segment },
            "segments() lists the assignment in segment order",
        )

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
        val events = newEventStore()
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

    /** Reads all of [readers] (at least once) until [done], at most a minute. */
    private suspend fun allUntil(readers: List<Reader>, what: String, done: () -> Boolean) {
        val deadline = System.nanoTime() + wait.toNanos()
        do {
            if (System.nanoTime() > deadline) {
                fail<Unit>("$what within ${wait.seconds} s (assigned ${readers.map { it.assigned }}, revocations ${readers.map { it.revocations }})")
            }
            pumpAll(readers)
        } while (!done())
    }

    /** Reads all of [readers] for one slice, side by side. */
    private suspend fun pumpAll(readers: List<Reader>) = coroutineScope { for (reader in readers) launch { reader.pump(SLICE_MS) } }

    /** One service instance of the event store under test, [eventStore] bounded like every call. */
    private suspend fun newEventStore(): EventStore = bounded("eventStore()") { eventStore() }

    private suspend fun newMessageStore(): MessageStore = bounded("messageStore()") { messageStore() }

    /**
     * Runs [block] in a transaction of [store], as the core does: committed, or rolled back when [rollback]. The transaction is
     * thread-bound, so all of it runs on one thread of the bounded call, which names [step] when the message store does not answer.
     */
    private suspend fun <T> inTransaction(step: String, store: MessageStore, rollback: Boolean = false, block: suspend () -> T): T =
        bounded(step) {
            var result: Result<T>? = null
            TransactionTemplate(store.transactions).executeWithoutResult { status ->
                result = Result.success(runBlocking { block() })
                if (rollback) status.setRollbackOnly()
            }
            result!!.getOrThrow()
        }

    /**
     * One subscription and what it gave: its assignment and its deliveries, in order. Every delivery is acknowledged as it
     * comes but the held ones; every one is checked against the assignment and the segment and position bounds.
     */
    private inner class Reader(val name: String, private var open: Subscription?) {
        val assigned = TreeSet<String>()

        /** Every segment a [SubscriptionEvent.Revoked] named. */
        val revoked = TreeSet<String>()

        /** How many [SubscriptionEvent.Revoked] came. */
        var revocations = 0

        /** A [SubscriptionEvent.Revoked] came and no [SubscriptionEvent.Assigned] after it yet (the contract closes every revocation with one). */
        var changing = false

        /** How many assignment changes came. */
        var changes = 0

        /** How many events (changes and deliveries) came. */
        var taken = 0

        /** The kind of the first event that came: `Assigned`, `Revoked` or `Delivery`. */
        var first: String? = null
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
            taken++
            if (first == null) {
                first = when (event) {
                    is SubscriptionEvent.Assigned -> "Assigned"
                    is SubscriptionEvent.Revoked -> "Revoked"
                    is Delivery -> "Delivery"
                }
            }
            if (event !is Delivery) changes++
            when (event) {
                is SubscriptionEvent.Assigned -> {
                    changing = false
                    for (segment in event.segments) {
                        assertTrue(
                            segment.isNotEmpty() && segment.toByteArray(Charsets.UTF_8).size <= MAX_SEGMENT_BYTES,
                            "$name: a segment name of 1 to $MAX_SEGMENT_BYTES bytes: \"$segment\"",
                        )
                        assigned += segment
                    }
                }
                is SubscriptionEvent.Revoked -> {
                    revocations++
                    changing = true
                    for (segment in event.segments) {
                        assertTrue(segment in assigned, "$name: a revoke of segment $segment it does not hold ($assigned)")
                    }
                    assigned -= event.segments.toSet()
                    revoked += event.segments
                }
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

        /** How long a check waits for the assignment change a join may cause. */
        const val JOIN_WINDOW_MS = 6_000L

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
