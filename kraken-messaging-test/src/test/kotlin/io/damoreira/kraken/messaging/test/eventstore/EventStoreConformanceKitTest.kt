package io.damoreira.kraken.messaging.test.eventstore

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SegmentStatus
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ExternalCallStore
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxStore
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import io.damoreira.kraken.messaging.store.SagaStores
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.SimpleTransactionStatus
import java.time.Duration
import java.time.Instant
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The event-store kit's own test (ADR 0027): the kit runs on a minimal in-process event store of this file, a test double
 * (not the in-memory event store add-on, which comes later), with a message store that only keeps processed-event markers. An
 * event-store add-on proves itself by running the same kit.
 */
class EventStoreConformanceKitTest : EventStoreConformanceTest() {
    private val log = Log(Capability.entries.toSet())
    private val markers = Markers()

    override val source = "conformance.events"

    override fun eventStore(): EventStore = log

    override fun messageStore(): MessageStore = markers
}

/** The same double declaring no capability: the contract checks pass and the capability checks are skipped. */
class EventStoreConformanceKitWithoutCapabilitiesTest : EventStoreConformanceTest() {
    private val log = Log(emptySet())
    private val markers = Markers()

    override val source = "conformance.events"

    override fun eventStore(): EventStore = log

    override fun messageStore(): MessageStore = markers
}

/** A call of the double that never answers, not even when cancelled. */
private enum class Stuck { PUBLISH, CLOSE, NEXT }

/** A store call that never answers fails the check naming it, after the kit's wait, instead of hanging the kit. */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD) // a kit that hangs fails here instead
class EventStoreConformanceKitBoundsTest {
    private fun failure(stuck: Stuck): String {
        // An anonymous kit, so that JUnit does not run it as a test class of its own; it waits a second.
        val kit = object : EventStoreConformanceTest() {
            private val log = Log(Capability.entries.toSet(), stuck)

            override val source = "conformance.events"
            override val wait: Duration = Duration.ofSeconds(1)

            override fun eventStore(): EventStore = log

            override fun messageStore(): MessageStore = Markers()
        }
        val started = System.nanoTime()
        val failure = assertFailsWith<AssertionError> { kit.headersRoundTrip() }
        kit.closeOpenSubscriptions()
        assertTrue(System.nanoTime() - started < Duration.ofSeconds(30).toNanos(), "failed in time")
        return failure.message!!
    }

    @Test
    fun `a publish that never answers fails the check naming it`() {
        val message = failure(Stuck.PUBLISH)
        assertTrue("check `headersRoundTrip`: publish " in message && "no answer within 1 s" in message, message)
    }

    @Test
    fun `a close that never answers fails the check naming it`() {
        val message = failure(Stuck.CLOSE)
        assertTrue("check `headersRoundTrip`: close conformance." in message && "no answer within 1 s" in message, message)
    }

    @Test
    fun `a next that ignores its cancellation fails the check naming it`() {
        val message = failure(Stuck.NEXT)
        assertTrue("check `headersRoundTrip`: next of conformance." in message && "no answer within 1 s" in message, message)
    }
}

/** Segments per source. */
private const val SEGMENTS = 2

private class Record(val key: String?, val headers: Map<String, String>, val payload: ByteArray, val timestampMs: Long)

/** A subscription name's committed positions and its members, in joining order. */
private class Group {
    val committed = HashMap<Int, Long>()
    val members = ArrayList<Long>()
}

/** The event store: one shared state for every "instance", guarded by its own monitor. */
private class Log(override val capabilities: Set<Capability>, val stuck: Stuck? = null) : EventStore {
    private val sources = HashMap<String, List<MutableList<Record>>>()
    private val groups = HashMap<SubscriptionSpec, Group>()
    private var nextMember = 0L
    private var spread = 0

    override val name = "kit-double"

    fun <T> locked(block: Log.() -> T): T = synchronized(this) { block() }

    fun source(source: String): List<MutableList<Record>> = sources.getOrPut(source) { List(SEGMENTS) { mutableListOf() } }

    fun head(source: String, segment: Int): Long = sources[source]?.get(segment)?.size?.toLong() ?: 0L

    fun group(spec: SubscriptionSpec): Group = groups.getOrPut(spec) { Group() }

    /** The segments of [member]: round robin over the members. */
    fun assignment(spec: SubscriptionSpec, member: Long): Set<Int> {
        val group = groups[spec] ?: return emptySet()
        val at = group.members.indexOf(member)
        if (at < 0) return emptySet()
        return (0 until SEGMENTS).filter { it % group.members.size == at }.toSet()
    }

    override suspend fun publish(event: OutgoingEvent) {
        if (stuck == Stuck.PUBLISH) never()
        locked { append(event) }
    }

    private fun append(event: OutgoingEvent) {
        val at = event.key?.let { Math.floorMod(it.hashCode(), SEGMENTS) } ?: (++spread % SEGMENTS)
        source(event.source)[at] += Record(event.key, event.headers, event.payload, System.currentTimeMillis())
    }

    override suspend fun subscribe(spec: SubscriptionSpec): Subscription = locked {
        val member = ++nextMember
        group(spec).members += member
        Member(this, spec, member)
    }

    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin =
        if (supports(Capability.REPLAY)) Admin(this, spec) else throw EventStoreException.Unsupported(Capability.REPLAY)

    override fun compare(a: String, b: String): Int = position(a).compareTo(position(b))
}

/** Never returns, not even when cancelled. */
private suspend fun never(): Nothing = withContext(NonCancellable) { awaitCancellation() }

private fun position(p: String): Long = p.toLongOrNull() ?: throw EventStoreException.Invalid("not a decimal position: $p")

private fun segment(s: String): Int = s.toIntOrNull()?.takeIf { it in 0 until SEGMENTS } ?: throw EventStoreException.Invalid("no segment $s")

private class Member(private val log: Log, private val spec: SubscriptionSpec, private val member: Long) : Subscription {
    private val assigned = TreeSet<Int>()
    private val cursor = ConcurrentHashMap<Int, Long>()
    @Volatile private var paused = false

    /** What `next` gives now, if anything. */
    private fun step(): SubscriptionEvent? = log.locked {
        val now = assignment(spec, member)
        val revoked = assigned.filter { it !in now }
        if (revoked.isNotEmpty()) {
            assigned -= revoked.toSet()
            revoked.forEach { cursor.remove(it) }
            return@locked SubscriptionEvent.Revoked(revoked.map { it.toString() })
        }
        val added = now.filter { it !in assigned }.sorted()
        if (added.isNotEmpty()) {
            val committed = group(spec).committed
            for (s in added) {
                assigned += s
                cursor[s] = committed[s] ?: 0L
            }
            return@locked SubscriptionEvent.Assigned(added.map { it.toString() })
        }
        if (paused) return@locked null
        val segments = source(spec.source)
        for (s in assigned) {
            val at = cursor.getValue(s)
            val r = segments[s].getOrNull(at.toInt()) ?: continue
            cursor[s] = at + 1
            return@locked Delivery(s.toString(), at.toString(), r.key, r.headers, r.payload, r.timestampMs)
        }
        null
    }

    override suspend fun next(): SubscriptionEvent {
        if (log.stuck == Stuck.NEXT) never()
        while (true) {
            step()?.let { return it }
            delay(5)
        }
    }

    override fun ack(segment: String, position: String) = log.locked {
        val s = segment(segment)
        val committed = group(spec).committed
        committed[s] = maxOf(committed[s] ?: 0L, position(position) + 1)
    }

    override fun redeliver(segment: String, position: String) {
        cursor[segment(segment)] = position(position)
    }

    override fun pause() {
        paused = true
    }

    override fun resume() {
        paused = false
    }

    override fun segments(): List<SegmentStatus> = log.locked {
        val committed = group(spec).committed
        assigned.map { s ->
            val head = head(spec.source, s)
            val at = cursor.getValue(s)
            val done = committed[s]
            val lag = if (supports(Capability.LAG)) head - (done ?: at) else null
            SegmentStatus(s.toString(), at.toString(), done?.toString(), head.toString(), lag)
        }
    }

    override suspend fun close() {
        if (log.stuck == Stuck.CLOSE) never()
        log.locked { group(spec).members.remove(member) }
    }
}

private class Admin(private val log: Log, private val spec: SubscriptionSpec) : SubscriptionAdmin {
    override suspend fun members(): List<String> = log.locked { group(spec).members.map { it.toString() } }

    override suspend fun plan(target: ResetTarget): List<ReplayRange> = log.locked {
        val source = spec.source
        fun range(s: Int, to: Long) = ReplayRange(source, s.toString(), to.toString(), group(spec).committed[s]?.toString().orEmpty())
        when (target) {
            ResetTarget.Earliest -> (0 until SEGMENTS).map { range(it, 0) }
            ResetTarget.Latest -> (0 until SEGMENTS).map { range(it, head(source, it)) }
            is ResetTarget.Timestamp -> (0 until SEGMENTS).map { s ->
                val at = source(source)[s].indexOfFirst { it.timestampMs >= target.timestampMs }
                range(s, if (at < 0) head(source, s) else at.toLong())
            }
            is ResetTarget.Positions -> target.positions.map { (name, at) ->
                val s = segment(name)
                val to = position(at)
                if (to > head(source, s)) throw EventStoreException.Invalid("$at past the head")
                range(s, to)
            }.sortedBy { it.segment }
        }
    }

    override suspend fun apply(ranges: List<ReplayRange>) = log.locked {
        val group = group(spec)
        if (group.members.isNotEmpty()) throw EventStoreException.SubscribersActive(group.members.toString())
        val moves = ranges.map { segment(it.segment) to position(it.from) }
        for ((s, to) in moves) group.committed[s] = to
    }
}

/** A message store keeping committed markers only (each record commits at once); the event-store kit uses nothing else. */
private class Markers : MessageStore {
    private val ids = ConcurrentHashMap.newKeySet<String>()

    override val name = "kit-markers"

    override val processedEvents: ProcessedEventStore = object : ProcessedEventStore {
        override suspend fun record(marker: ProcessedMarker): Boolean = ids.add(marker.id)

        override suspend fun exists(id: String): Boolean = id in ids

        override suspend fun purge(before: Instant): Long = TODO("not used by the event-store kit")
    }

    override val transactions: PlatformTransactionManager = object : PlatformTransactionManager {
        override fun getTransaction(definition: TransactionDefinition?): TransactionStatus = SimpleTransactionStatus()

        override fun commit(status: TransactionStatus) = Unit

        override fun rollback(status: TransactionStatus) = Unit
    }

    override val outbox: OutboxStore get() = TODO("not used by the event-store kit")
    override val deadLetters: DeadLetterStore get() = TODO("not used by the event-store kit")
    override val processorControl: ProcessorControlStore get() = TODO("not used by the event-store kit")
    override val externalCalls: ExternalCallStore get() = TODO("not used by the event-store kit")
    override val sagas: SagaStores get() = TODO("not used by the event-store kit")
}
