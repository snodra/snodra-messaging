package io.damoreira.kraken.messaging.eventstore.memory

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SegmentOrder
import io.damoreira.kraken.messaging.eventstore.SegmentStatus
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import java.time.Clock
import java.util.Collections

/** Settings of the [MemoryEventStore] (`kraken.messaging.memory.*`). */
data class MemoryEventStoreSettings(
    /**
     * How many segments every source has (`kraken.messaging.memory.segments`, default 4): an event's key always goes to the
     * same one (order per key), and the competing instances of a subscription share them.
     */
    val segments: Int = DEFAULT_SEGMENTS,
) {
    init {
        require(segments in 1..MAX_SEGMENTS) { "kraken.messaging.memory.segments must be between 1 and $MAX_SEGMENTS, was $segments" }
    }

    companion object {
        const val DEFAULT_SEGMENTS = 4
        const val MAX_SEGMENTS = 1024
    }
}

/**
 * One event the [MemoryEventStore] keeps: what was published to [source], where it went ([segment], [position]) and when
 * ([timestampMs], the store's clock, epoch milliseconds). [headers] cannot be changed, and [payload] (the JSON bytes as
 * published) is a copy each time: nothing a reader does changes the log.
 */
class StoredEvent internal constructor(
    val source: String,
    val segment: String,
    val position: String,
    val key: String?,
    headers: Map<String, String>,
    /** The log's own copy of the payload: never handed out. */
    internal val bytes: ByteArray,
    val timestampMs: Long,
    /** The store-wide append order. */
    internal val sequence: Long,
) {
    val headers: Map<String, String> = Collections.unmodifiableMap(LinkedHashMap(headers))

    /** The payload, a fresh copy on every call. */
    val payload: ByteArray get() = bytes.copyOf()

    /** The `type` header. */
    val type: String? get() = headers[EventHeaders.TYPE]

    /** The `id` header. */
    val id: String? get() = headers[EventHeaders.ID]

    override fun toString(): String =
        "StoredEvent(source=$source, segment=$segment, position=$position, key=$key, headers=$headers, payload=${bytes.size} bytes, timestampMs=$timestampMs)"
}

/**
 * The in-memory event store (ADR 0027): every source is a log of a fixed number of segments ([MemoryEventStoreSettings.segments])
 * in this JVM's memory, for tests, local runs and single-process demos. **Not durable**: everything is gone when the process
 * ends, and only the subscriptions of this JVM (of this instance of the store) read it. Name `memory`; capabilities `replay`,
 * `browse` (the read access below, [events] and [event]) and `lag`; no `native-dead-letter` (ADR 0027's table).
 *
 * - **Publish**: an event goes to the segment of its key (FNV-1a 64 of the key's UTF-8 bytes, unsigned, modulo the segments), an
 *   event without a key to the next segment in turn; its position is its index in the segment (`0`, `1`, ...), its timestamp the
 *   store's [clock]. Payloads and headers are kept as given.
 * - **Subscriptions**: one cursor per segment and subscription name, a committed position per segment and name. The instances
 *   of one name share the segments round robin in joining order (segment `s` is the `s % n`-th instance's); a segment changes
 *   hands only once its previous holder gave it up (its next [Subscription.next] reports it revoked), so no two instances ever
 *   read one segment at once. What an instance had not acknowledged comes again to the next holder. A new name starts at the
 *   earliest position. [Subscription.next] wakes up at once when something it waits for happens (a publish, a resume, a
 *   reassignment).
 * - **Replay**: [admin] plans and applies resets (earliest, latest, a time, explicit positions) while no instance reads the
 *   subscription.
 * - Nothing is ever deleted: a long run keeps every event in memory.
 */
class MemoryEventStore(
    val settings: MemoryEventStoreSettings = MemoryEventStoreSettings(),
    private val clock: Clock = Clock.systemUTC(),
) : EventStore {
    override val name: String = NAME

    override val capabilities: Set<Capability> = setOf(Capability.REPLAY, Capability.BROWSE, Capability.LAG)

    private val lock = Any()
    private val sources = HashMap<String, Log>()
    private val groups = HashMap<SubscriptionSpec, Group>()

    /** Bumped on every change a waiting [Subscription.next] may care about. */
    private val changes = MutableStateFlow(0L)
    private var members = 0L
    private var appended = 0L
    private var spread = 0

    private val segmentCount = settings.segments

    override suspend fun publish(event: OutgoingEvent) {
        locked {
            val log = log(event.source)
            val segment = event.key?.let { segmentOf(it, segmentCount) } ?: Math.floorMod(spread++, segmentCount)
            val events = log.segments[segment]
            events += StoredEvent(
                event.source, segment.toString(), events.size.toString(), event.key, event.headers, event.payload.copyOf(),
                clock.millis(), appended++,
            )
            changed()
        }
    }

    override suspend fun subscribe(spec: SubscriptionSpec): Subscription = locked {
        val member = Member(spec, "memory-${++members}")
        group(spec).members += member
        changed()
        member
    }

    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin = Admin(spec)

    override fun compare(a: String, b: String): Int = position(a).compareTo(position(b))

    // ------------------------------------------------------------------------------------------------------------- browse

    /** The sources that have at least one event, by name. */
    fun sources(): List<String> = locked { sources.filterValues { log -> log.segments.any { it.isNotEmpty() } }.keys.sorted() }

    /**
     * The events of [source] in the order they were appended, those of [type] (header `type`) and [key] only when given.
     * A copy: later publishes do not change it.
     */
    fun events(source: String, type: String? = null, key: String? = null): List<StoredEvent> = locked {
        sources[source]?.segments?.flatMap { it }.orEmpty()
            .filter { (type == null || it.type == type) && (key == null || it.key == key) }
            .sortedBy { it.sequence }
    }

    /** The event at [position] of [segment] of [source], or null. */
    fun event(source: String, segment: String, position: String): StoredEvent? = locked {
        val s = segmentIndexOrNull(segment) ?: return@locked null
        val p = decimalOrNull(position) ?: return@locked null
        sources[source]?.segments?.get(s)?.getOrNull(p.toIntOrNull() ?: return@locked null)
    }

    // ------------------------------------------------------------------------------------------------------------ internals

    private fun <T> locked(block: () -> T): T = synchronized(lock, block)

    /** Called under the lock after a change. */
    private fun changed() {
        changes.value = changes.value + 1
    }

    private fun log(source: String): Log = sources.getOrPut(source) { Log(segmentCount) }

    private fun group(spec: SubscriptionSpec): Group = groups.getOrPut(spec) { Group() }

    private fun head(source: String, segment: Int): Long = sources[source]?.segments?.get(segment)?.size?.toLong() ?: 0L

    /** [segment] as an index of this store's segments (only their canonical names `0` to `n - 1`), else null. */
    private fun segmentIndexOrNull(segment: String): Int? =
        decimalOrNull(segment)?.toIntOrNull()?.takeIf { it in 0 until segmentCount }

    private fun segmentIndex(segment: String): Int =
        segmentIndexOrNull(segment) ?: throw EventStoreException.Invalid("no segment \"$segment\" (the segments are 0 to ${segmentCount - 1})")

    private class Log(segments: Int) {
        val segments: List<MutableList<StoredEvent>> = List(segments) { ArrayList() }
    }

    /** A subscription name's committed positions (segment to the next position to read), its members and who holds each segment. */
    private class Group {
        val committed = HashMap<Int, Long>()
        val members = ArrayList<Member>()
        val holders = HashMap<Int, Member>()

        /** Who should hold [segment]: round robin over the members in joining order. */
        fun target(segment: Int): Member? = if (members.isEmpty()) null else members[segment % members.size]
    }

    private inner class Member(private val spec: SubscriptionSpec, val id: String) : Subscription {
        // Guarded by the store's lock.
        private val held = sortedSetOf<Int>()
        private val cursor = HashMap<Int, Long>()
        private var closed = false
        private var turn = 0

        /** A [SubscriptionEvent.Revoked] was given and no [SubscriptionEvent.Assigned] since: the next step closes the change. */
        private var changing = false

        /** An [SubscriptionEvent.Assigned] was given: a new subscription's first event is one, empty when it holds nothing. */
        private var announced = false

        @Volatile private var paused = false

        override suspend fun next(): SubscriptionEvent {
            while (true) {
                val seen = changes.value
                step()?.let { return it }
                changes.first { it != seen }
            }
        }

        /** What [next] gives now, if anything; nothing suspends in here, so a cancelled [next] loses nothing. */
        private fun step(): SubscriptionEvent? = locked {
            if (closed) throw EventStoreException.Failed("the subscription ${spec.name} is closed")
            val group = group(spec)
            val lost = held.filter { group.target(it) !== this }
            if (lost.isNotEmpty()) {
                for (s in lost) {
                    held -= s
                    cursor -= s
                    group.holders.remove(s)
                }
                changed() // the next holder may take them now
                changing = true
                return@locked SubscriptionEvent.Revoked(lost.map(Int::toString))
            }
            val gained = (0 until segmentCount).filter { group.target(it) === this && it !in held && group.holders[it] == null }
            if (gained.isNotEmpty()) {
                for (s in gained) {
                    held += s
                    cursor[s] = group.committed[s] ?: 0L
                    group.holders[s] = this
                }
                changing = false
                announced = true
                return@locked SubscriptionEvent.Assigned(gained.map(Int::toString))
            }
            if (changing || !announced) {
                // Every revocation is closed by an assignment, an empty one when nothing new came; and a new subscription that
                // holds nothing says so (a member, idle).
                changing = false
                announced = true
                return@locked SubscriptionEvent.Assigned(emptyList())
            }
            if (paused || held.isEmpty()) return@locked null
            val segments = log(spec.source).segments
            val order = held.toList()
            for (i in order.indices) {
                // Each delivery starts the round at the next held segment, so a busy segment never starves the others.
                val s = order[(turn + i) % order.size]
                val at = cursor.getValue(s)
                val event = segments[s].getOrNull(at.toInt()) ?: continue
                cursor[s] = at + 1
                turn = (turn + i + 1) % order.size
                return@locked Delivery(event.segment, event.position, event.key, event.headers, event.bytes.copyOf(), event.timestampMs)
            }
            null
        }

        override fun ack(segment: String, position: String) {
            val s = segmentIndex(segment)
            val p = position(position)
            locked {
                if (s !in held) return@locked // given up: its next holder starts from what was acknowledged before
                val committed = group(spec).committed
                committed[s] = maxOf(committed[s] ?: 0L, p + 1)
            }
        }

        override fun redeliver(segment: String, position: String) {
            val s = segmentIndex(segment)
            val p = position(position)
            locked {
                val at = cursor[s] ?: return@locked
                if (p < at) cursor[s] = p
                changed()
            }
        }

        /** Under the store's lock: once it returned, no delivery leaves (a [next] that took one before returned it already). */
        override fun pause() = locked {
            paused = true
        }

        override fun resume() {
            paused = false
            locked { changed() }
        }

        override fun segments(): List<SegmentStatus> = locked {
            val committed = group(spec).committed
            held.map { s ->
                val head = head(spec.source, s)
                val at = cursor.getValue(s)
                val done = committed[s]
                SegmentStatus(s.toString(), at.toString(), done?.toString(), head.toString(), head - (done ?: at))
            }.sortedWith(compareBy(SegmentOrder) { it.segment })
        }

        override suspend fun close() {
            locked {
                if (closed) return@locked
                closed = true
                val group = group(spec)
                group.members.remove(this)
                for (s in held) group.holders.remove(s)
                held.clear()
                cursor.clear()
                changed()
            }
        }
    }

    private inner class Admin(private val spec: SubscriptionSpec) : SubscriptionAdmin {
        override suspend fun members(): List<String> = locked { groups[spec]?.members?.map { it.id }.orEmpty() }

        override suspend fun plan(target: ResetTarget): List<ReplayRange> = locked {
            val source = spec.source
            val committed = groups[spec]?.committed.orEmpty()
            fun range(s: Int, from: Long) = ReplayRange(source, s.toString(), from.toString(), committed[s]?.toString().orEmpty())
            val all = 0 until segmentCount
            when (target) {
                ResetTarget.Earliest -> all.map { range(it, 0L) }
                ResetTarget.Latest -> all.map { range(it, head(source, it)) }
                is ResetTarget.Timestamp -> all.map { s ->
                    val events = sources[source]?.segments?.get(s).orEmpty()
                    val at = events.indexOfFirst { it.timestampMs >= target.timestampMs }
                    range(s, if (at < 0) events.size.toLong() else at.toLong())
                }
                is ResetTarget.Positions -> target.positions.map { (segment, position) ->
                    val s = segmentIndex(segment)
                    range(s, within(source, s, position))
                }.sortedWith(compareBy(SegmentOrder) { it.segment })
            }
        }

        override suspend fun apply(ranges: List<ReplayRange>) = locked {
            val group = group(spec)
            if (group.members.isNotEmpty()) throw EventStoreException.SubscribersActive(group.members.joinToString { it.id })
            val moves = ranges.map { range ->
                if (range.source != spec.source) throw EventStoreException.Invalid("a range of source ${range.source}, not ${spec.source}")
                val s = segmentIndex(range.segment)
                s to within(spec.source, s, range.from)
            }
            for ((s, to) in moves) group.committed[s] = to
            changed()
        }

        /** [position] of segment [s] of [source] when it is one the segment has (its head included), else [EventStoreException.Invalid]. */
        private fun within(source: String, s: Int, position: String): Long {
            val p = position(position)
            val head = head(source, s)
            if (p > head) throw EventStoreException.Invalid("position $position is past the head $head of segment $s")
            return p
        }
    }

    companion object {
        /** The event store's name (`kraken.messaging.event-store=memory`). */
        const val NAME = "memory"

        /** The segment of [key] among [segments]: FNV-1a 64 of its UTF-8 bytes, unsigned, modulo [segments]. */
        fun segmentOf(key: String, segments: Int): Int {
            var hash = -0x340d631b7bdddcdbL // 0xcbf29ce484222325, the FNV-1a 64 offset basis
            for (b in key.toByteArray(Charsets.UTF_8)) {
                hash = hash xor (b.toLong() and 0xFF)
                hash *= 0x100000001b3L // the FNV-1a 64 prime
            }
            return java.lang.Long.remainderUnsigned(hash, segments.toLong()).toInt()
        }

        /** [text] when it is a decimal number written canonically (no sign, no leading zero), else null. */
        private fun decimalOrNull(text: String): String? =
            text.takeIf { it.isNotEmpty() && it.length <= 19 && it.all { c -> c in '0'..'9' } && (it == "0" || it[0] != '0') }

        /** A position: a canonical decimal number, else [EventStoreException.Invalid]. */
        private fun position(text: String): Long =
            decimalOrNull(text)?.toLongOrNull() ?: throw EventStoreException.Invalid("not a position of the in-memory event store: \"$text\"")
    }
}
