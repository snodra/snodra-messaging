package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.SegmentOrder
import io.damoreira.kraken.messaging.eventstore.SegmentStatus
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * One instance's subscription of a name on a source ([RabbitEventStore.subscribe]): a consumer named `<service>.<processor>` on
 * every partition stream of the source, each a single active consumer (with `super-stream=<source>` for a super stream, so the
 * broker spreads the partitions over the consumers sharing the name) carrying `kraken-member=<this instance's id>`.
 *
 * - **Assignment** is the broker's consumer updates: a partition whose consumer becomes active is `Assigned`, one that stops
 *   being active is `Revoked` followed by an empty `Assigned` (the change is over). On activation the subscription reads the
 *   offset stored under its name (on a connection of its own: the update is answered from the data connection's own thread) and
 *   answers the broker `offset(stored)`, or `first` when none is stored; a failed read ends the subscription (never `first`). On
 *   deactivation it stores what was acknowledged first, waits until it reads it back, then answers.
 * - **Acknowledgement** moves the position stored under the name (server-side offset tracking): the stored value is the next
 *   offset to read, the offset after the acknowledged delivery (kraken's rule, not the Java client's "last processed" one: the
 *   client's automatic tracking is never used). Every stored offset is an entry in the stream itself, so it is stored every
 *   [RabbitEventStoreSettings.offsetStoreMessages] acknowledgements or [RabbitEventStoreSettings.offsetStoreInterval], whichever
 *   comes first, and always on deactivation and at [close], which waits until the broker reads it back. An acknowledgement of an
 *   offset not handed out since the partition last became active is ignored.
 * - **Flow**: each partition asks for one chunk at a time; the next is asked for once the deliveries waiting are under
 *   [READY_LIMIT], never while paused.
 * - **Redelivery** cannot seek a single active consumer in place: the deliveries handed out and not acknowledged are kept, and
 *   [redeliver] hands them out again ahead of the later ones.
 * - **Status**: every [STATUS_REFRESH] (sooner until known) the end of each active partition is read on a connection of its own;
 *   the lag counts the deliveries received and not acknowledged one by one and what is not received yet by offsets (offsets also
 *   number the offset-tracking entries, so that part may be overstated).
 * - **Errors**: a lost connection, or a partition stream deleted or moved, ends the subscription: [next] throws
 *   [EventStoreException.Failed] (the core subscribes again).
 */
internal class RabbitSubscription private constructor(
    private val spec: SubscriptionSpec,
    private val connector: RabbitConnector,
    private val member: String,
    private val settings: RabbitEventStoreSettings,
    private val onClose: () -> Unit,
) : Subscription {
    private val log = LoggerFactory.getLogger(RabbitSubscription::class.java)
    private val name = spec.name

    /** Guards every part, [changes] and [turn]; never held across a call to the broker. */
    private val lock = ReentrantLock()
    private val parts = ArrayList<PartState>()
    private val changes = ArrayDeque<SubscriptionEvent>()
    private var turn = 0
    private val signal = Channel<Unit>(Channel.CONFLATED)
    private val conns = ArrayList<DataConn>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val recovered = AtomicReference<String?>(null)

    @Volatile private var paused = false

    @Volatile private var closing = false

    @Volatile private var broken: EventStoreException.Failed? = null

    /** Stored-offset reads and read-backs, never on a data connection (whose thread may be the one waiting). */
    private lateinit var control: Client

    /** One data connection (to one leader node) and the partitions it consumes, by subscription id. */
    private inner class DataConn(node: Client.Broker) {
        val label = "${node.host}:${node.port}"
        val byId = HashMap<Byte, PartState>()
        var nextId = 0
        val client: Client = connector.node(node) { p ->
            p.chunkListener { client, id, _, _, _ -> chunk(client, id); null }
            p.messageListener { id, offset, timestamp, _, _, message -> message(id, offset, timestamp, message) }
            p.consumerUpdateListener { _, id, active -> update(id, active) }
            p.shutdownListener { context -> if (!closing) broke("the connection to $label closed (${context.shutdownReason})") }
            p.metadataListener { stream, code ->
                if (!closing && lock.withLock { byId.values.any { it.stream == stream } }) broke("the stream $stream moved or was deleted (code $code)")
            }
        }

        fun part(id: Byte): PartState? = lock.withLock { byId[id] }

        private fun chunk(client: Client, id: Byte) {
            val part = part(id) ?: return
            val credit = lock.withLock {
                // A chunk that came while not active (sent just before a deactivation) is dropped; its credit is given back so the
                // broker can send again once this consumer is active.
                val credit = !part.active || (!paused && part.ready.size < READY_LIMIT)
                part.owed = !credit
                credit
            }
            if (credit) credit(client, id)
        }

        private fun message(id: Byte, offset: Long, timestamp: Long, message: com.rabbitmq.stream.Message) {
            val part = part(id) ?: return
            val added = lock.withLock {
                if (!part.active || offset < part.next) return@withLock false
                part.ready.addLast(RabbitWire.delivery(part.segment, offset, timestamp, message))
                part.next = offset + 1
                part.head = maxOf(part.head ?: 0, part.next)
                true
            }
            if (added) signal.trySend(Unit)
        }

        private fun update(id: Byte, active: Boolean): OffsetSpecification {
            val part = part(id) ?: return OffsetSpecification.next()
            return if (active) activate(this, id, part) else deactivate(this, id, part)
        }
    }

    /** One partition stream: what it received, handed out and acknowledged in its current activation. */
    private class PartState(val segment: String, val stream: String, val conn: DataConn, val id: Byte) {
        var active = false

        /** The next offset taken from the broker (a chunk may start before it). */
        var next = 0L

        /** Received, not handed out. */
        val ready = ArrayDeque<Delivery>()

        /** Handed out, not acknowledged. */
        val inflight = ArrayDeque<Delivery>()

        /** The offset after the last acknowledged delivery. */
        var acked: Long? = null

        /** The offset stored under the name when the partition became active. */
        var stored: Long? = null
        var head: Long? = null

        /** A chunk was received and the next not asked for yet. */
        var owed = false

        /** One past the last offset handed out in this activation (null: nothing handed out yet). */
        var handed: Long? = null

        /** What was last stored (or found stored). */
        var lastStored: Long? = null

        /** Acknowledgements since the last store. */
        var unstored = 0L

        /**
         * Acknowledges [offset] and every earlier one; false when it does not move the acknowledged position: already acknowledged,
         * or not handed out in this activation (an acknowledgement from an earlier activation, whose deliveries another instance may
         * have handled since).
         */
        fun ack(offset: Long): Boolean {
            val handedNow = handed?.let { offset < it } == true && stored.let { it == null || offset >= it }
            if (!active || !handedNow || acked?.let { it >= offset + 1 } == true) return false
            acked = offset + 1
            inflight.removeAll { it.position.toLong() <= offset }
            return true
        }

        /** Hands out again, ahead of the rest and in order, the deliveries handed out at or after [from] and not acknowledged. */
        fun redeliver(from: Long) {
            val again = inflight.filter { it.position.toLong() >= from }
            inflight.removeAll { it.position.toLong() >= from }
            for (d in again.asReversed()) ready.addFirst(d)
        }

        fun handOut(): Delivery? {
            val d = ready.removeFirstOrNull() ?: return null
            handed = maxOf(handed ?: 0, d.position.toLong() + 1)
            inflight.addLast(d)
            return d
        }
    }

    // ------------------------------------------------------------------------------------------------ the broker's updates

    private fun activate(conn: DataConn, id: Byte, part: PartState): OffsetSpecification {
        val stored = try {
            RabbitConnector.storedOffset(control, name, part.stream)
        } catch (e: EventStoreException) {
            // Never from the first offset on a failed read (the whole partition again, its markers maybe purged): the subscription
            // ends and the core subscribes again.
            broke("${part.stream}: the stored offset was not read on activation: ${e.message}")
            return OffsetSpecification.next()
        }
        val credit = lock.withLock {
            // A credit held back (a pause, a full buffer) before the last deactivation: the broker has none left to send with.
            val credit = part.owed && !paused
            if (credit) part.owed = false
            part.active = true
            part.next = stored ?: 0
            part.stored = stored
            part.acked = null
            part.handed = null
            part.lastStored = stored
            part.unstored = 0
            part.ready.clear()
            part.inflight.clear()
            part.head = stored?.let { maxOf(part.head ?: 0, it) } ?: part.head
            changes.addLast(SubscriptionEvent.Assigned(listOf(part.segment)))
            credit
        }
        log.info("Subscription {}: partition {} assigned (from {})", name, part.stream, stored ?: "first")
        signal.trySend(Unit)
        if (credit) credit(conn.client, id)
        return stored?.let { OffsetSpecification.offset(it) } ?: OffsetSpecification.first()
    }

    private fun deactivate(conn: DataConn, id: Byte, part: PartState): OffsetSpecification {
        flush(part)
        val owed = lock.withLock {
            val wasActive = part.active
            val owed = part.owed
            part.owed = false
            part.active = false
            part.acked = null
            part.ready.clear()
            part.inflight.clear()
            if (wasActive) {
                // The partition's move is over once it is revoked here (the other instance's activation is its own): an empty
                // assignment closes the change, so the core's status leaves STARTING.
                changes.addLast(SubscriptionEvent.Revoked(listOf(part.segment)))
                changes.addLast(SubscriptionEvent.Assigned(emptyList()))
            }
            owed
        }
        log.info("Subscription {}: partition {} revoked", name, part.stream)
        signal.trySend(Unit)
        // An inactive consumer always holds a credit, so the broker can send once it is active again.
        if (owed) credit(conn.client, id)
        return OffsetSpecification.next()
    }

    private fun credit(client: Client, id: Byte) {
        try {
            client.credit(id, 1)
        } catch (e: Exception) {
            if (!closing) broke("asking for the next chunk: ${e.message}")
        }
    }

    private fun broke(why: String) {
        if (broken == null) {
            log.warn("Subscription {} of {} ends: {}", name, spec.source, why)
            broken = EventStoreException.Failed("the subscription $name of ${spec.source} ended: $why")
        }
        signal.trySend(Unit)
    }

    // --------------------------------------------------------------------------------------------- offsets stored

    /** Stores the acknowledged offset of [part] if it moved (the broker applies it asynchronously). */
    private fun store(part: PartState) {
        val acked = lock.withLock {
            val acked = part.acked?.takeIf { part.active && part.lastStored.let { s -> s == null || s < it } } ?: return
            part.lastStored = acked
            part.unstored = 0
            acked
        }
        try {
            part.conn.client.storeOffset(name, part.stream, acked)
        } catch (e: Exception) {
            log.warn("Subscription {}: the acknowledged offset of {} was not stored: {}", name, part.stream, e.toString())
            recovered.set("storing the offset of ${part.stream}: ${e.message}")
            lock.withLock { if (part.lastStored == acked) part.lastStored = null }
        }
    }

    /**
     * Stores the acknowledged offset of [part] and waits until the broker reads it back, at most [STORE_WAIT] (an offset stored
     * already is only read back: every store is an entry in the stream).
     */
    private fun flush(part: PartState) {
        val (acked, stored) = lock.withLock { part.acked?.takeIf { part.active }?.let { it to (part.lastStored == it) } } ?: return
        try {
            RabbitConnector.storeAndConfirm(part.conn.client, control, name, part.stream, acked, STORE_WAIT, store = !stored)
            lock.withLock {
                part.lastStored = acked
                part.unstored = 0
            }
        } catch (e: EventStoreException) {
            log.warn("Subscription {}: the acknowledged offset of {} was not stored: {}", name, part.stream, e.message)
            recovered.set("storing the offset of ${part.stream}: ${e.message}")
        }
    }

    // ------------------------------------------------------------------------------------------------ the contract

    override suspend fun next(): SubscriptionEvent {
        while (true) {
            broken?.let { throw it }
            if (closing) throw EventStoreException.Failed("the subscription $name is closed")
            take()?.let { return it }
            signal.receive() // only a hint: the state is checked first, so a cancelled receive loses nothing
        }
    }

    private fun take(): SubscriptionEvent? {
        var owed: PartState? = null
        val event = lock.withLock {
            changes.removeFirstOrNull()?.let { return@withLock it }
            if (paused || parts.isEmpty()) return@withLock null
            for (k in parts.indices) {
                val i = (turn + k) % parts.size
                val part = parts[i]
                if (!part.active) continue
                val d = part.handOut() ?: continue
                if (part.owed && part.ready.size < READY_LIMIT) {
                    part.owed = false
                    owed = part
                }
                turn = i + 1
                return@withLock d
            }
            null
        }
        owed?.let { credit(it.conn.client, it.id) }
        return event
    }

    override fun ack(segment: String, position: String) {
        val offset = offsetOf(position)
        val part = lock.withLock {
            // A segment revoked meanwhile, or an offset not handed out in this activation: ignored (the delivery comes again to
            // whoever reads the partition).
            val part = parts.firstOrNull { it.segment == segment } ?: return
            if (!part.ack(offset)) return
            part.unstored++
            part.takeIf { it.unstored >= settings.offsetStoreMessages }
        }
        part?.let(::store)
    }

    override fun redeliver(segment: String, position: String) {
        val from = offsetOf(position)
        lock.withLock { parts.firstOrNull { it.segment == segment && it.active }?.redeliver(from) }
        signal.trySend(Unit)
    }

    override fun pause() {
        paused = true
    }

    override fun resume() {
        paused = false
        val owed = lock.withLock {
            parts.filter { it.owed && it.ready.size < READY_LIMIT }.onEach { it.owed = false }
        }
        owed.forEach { credit(it.conn.client, it.id) }
        signal.trySend(Unit)
    }

    override fun segments(): List<SegmentStatus> = lock.withLock {
        parts.filter { it.active }.map { p ->
            val position = p.ready.firstOrNull()?.position?.toLong() ?: p.next
            val unacked = (p.inflight.size + p.ready.size).toLong()
            SegmentStatus(
                segment = p.segment,
                position = position.toString(),
                committed = (p.acked ?: p.stored)?.toString(),
                head = p.head?.toString(),
                lag = p.head?.let { unacked + maxOf(0, it - p.next) },
            )
        }
    }

    override fun takeError(): String? = recovered.getAndSet(null)

    override suspend fun close() {
        if (closing) return
        withContext(Dispatchers.IO) {
            // Everything acknowledged is stored (and read back) before the consumers leave, so the next active consumer of a
            // partition resumes after it.
            val active = lock.withLock { parts.filter { it.active } }
            for (part in active) flush(part)
            closing = true
            scope.cancel()
            for (conn in conns) {
                for (id in lock.withLock { conn.byId.keys.toList() }) runCatching { conn.client.unsubscribe(id) }
                runCatching { conn.client.close() }
            }
            if (::control.isInitialized) runCatching { control.close() }
            onClose()
            signal.trySend(Unit)
        }
    }

    // ------------------------------------------------------------------------------------------------ joining

    private fun join() {
        control = connector.seed()
        val layout = RabbitConnector.layout(control, spec.source) ?: throw EventStoreException.Failed(
            "rabbitmq: neither a super stream nor a stream named ${spec.source} exists: create it " +
                "(rabbitmq-streams add_super_stream ${spec.source} --partitions 6)",
        )
        val superStream = !(layout.size == 1 && layout[0].stream == spec.source)
        for (p in layout.sortedWith(compareBy(SegmentOrder) { it.segment })) {
            val leader = RabbitConnector.leader(control, p.stream)
            val conn = lock.withLock { conns.firstOrNull { it.label == "${leader.host}:${leader.port}" && it.nextId <= MAX_SUBSCRIPTION_ID } }
                ?: DataConn(leader).also { lock.withLock { conns += it } }
            val id = (conn.nextId++).toByte()
            val part = PartState(p.segment, p.stream, conn, id)
            // Registered before the subscribe: the broker's first consumer update may come before its answer is read.
            lock.withLock {
                conn.byId[id] = part
                parts += part
            }
            val properties = linkedMapOf(
                "single-active-consumer" to "true",
                "name" to name,
                MEMBER_PROPERTY to member,
            )
            if (superStream) properties["super-stream"] = spec.source
            val response = try {
                conn.client.subscribe(id, p.stream, OffsetSpecification.next(), 1, properties)
            } catch (e: Exception) {
                throw EventStoreException.Failed("rabbitmq: cannot consume ${p.stream}: ${e.message}", e)
            }
            if (!response.isOk) throw EventStoreException.Failed("rabbitmq: cannot consume ${p.stream}: ${RabbitConnector.codeName(response.responseCode)}")
        }
        scope.launch { refresh() }
        scope.launch { storeEvery() }
    }

    /** Stores the acknowledged offsets every [RabbitEventStoreSettings.offsetStoreInterval]. */
    private suspend fun storeEvery() {
        while (scope.isActive) {
            delay(settings.offsetStoreInterval.toMillis())
            for (part in lock.withLock { parts.toList() }) store(part)
        }
    }

    /** Every [STATUS_REFRESH] (sooner until every active partition's end is known): the end of each active partition. */
    private suspend fun refresh() {
        var reads: RabbitReads? = null
        try {
            while (scope.isActive) {
                val active = lock.withLock { parts.filter { it.active } }
                var allKnown = active.isNotEmpty()
                for (part in active) {
                    val head = try {
                        val r = reads?.takeIf { it.isOpen } ?: RabbitReads(connector).also { reads = it }
                        if (r.firstOffset(part.stream) == null) 0L else r.head(part.stream, HEAD_WAIT)
                    } catch (e: EventStoreException) {
                        log.debug("Subscription {}: the end of {} was not read: {}", name, part.stream, e.message)
                        reads?.close()
                        reads = null
                        null
                    }
                    lock.withLock {
                        if (head != null && part.active) part.head = maxOf(part.head ?: 0, head)
                        if (part.head == null) allKnown = false
                    }
                }
                delay(if (allKnown) STATUS_REFRESH.toMillis() else STATUS_FIRST.toMillis())
            }
        } finally {
            reads?.close()
        }
    }

    companion object {
        /** The subscribe property that tells the members of one subscription apart (the management API shows it). */
        const val MEMBER_PROPERTY = "kraken-member"

        /** Deliveries waiting per partition above which no further chunk is asked for. */
        const val READY_LIMIT = 1_000

        /** How long a stored offset may take to be read back (at close, at deactivation). */
        val STORE_WAIT: Duration = Duration.ofSeconds(5)

        /** How often the end of each active partition is read (ADR 0022: at most every 5 s). */
        val STATUS_REFRESH: Duration = Duration.ofSeconds(5)

        /** Until every active partition's end is known: how long between two reads. */
        private val STATUS_FIRST: Duration = Duration.ofMillis(500)

        /** How long the last chunk of a partition that has some may take to come. */
        private val HEAD_WAIT: Duration = Duration.ofSeconds(5)

        /** Subscription ids are one byte per connection: past the last one another connection to the node is opened. */
        private const val MAX_SUBSCRIPTION_ID = 255

        private fun offsetOf(position: String): Long =
            position.toLongOrNull()?.takeIf { it >= 0 } ?: throw EventStoreException.Invalid("position \"$position\" is not an offset")

        /**
         * Joins [spec] as one more member ([member] tells it apart), blocking; [EventStoreException.Failed] when the source does not
         * exist or a partition cannot be consumed (what was built is closed first).
         */
        fun join(
            spec: SubscriptionSpec,
            connector: RabbitConnector,
            member: String,
            settings: RabbitEventStoreSettings,
            onClose: () -> Unit,
        ): RabbitSubscription {
            val subscription = RabbitSubscription(spec, connector, member, settings, onClose)
            try {
                subscription.join()
            } catch (e: Exception) {
                kotlinx.coroutines.runBlocking { subscription.close() }
                throw e as? EventStoreException ?: EventStoreException.Failed("rabbitmq: subscribing ${spec.name} to ${spec.source}: ${e.message}", e)
            }
            return subscription
        }
    }
}
