package com.damoreira.snodra.messaging.kafka

import com.damoreira.snodra.messaging.eventstore.Delivery
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.SegmentOrder
import com.damoreira.snodra.messaging.eventstore.SegmentStatus
import com.damoreira.snodra.messaging.eventstore.Subscription
import com.damoreira.snodra.messaging.eventstore.SubscriptionEvent
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * One instance's subscription of a consumer group ([KafkaEventStore]).
 *
 * A thread of its own owns the [KafkaConsumer] (which is not thread safe): it polls, keeps the events in a queue that [next]
 * reads, commits what [ack] recorded, and applies [redeliver] and the pause. Everything the other threads do reaches the
 * consumer through that thread.
 *
 * **Queue.** The poll thread fills it with the assignment changes (from the rebalance listener, on that thread) and the
 * records of each poll. It keeps fetching while the queue holds fewer than a batch of records (`maxPollRecords`): at that
 * mark the partitions are paused (flow control; a paused poll still keeps the group membership alive) and they resume as [next]
 * drains the queue below the low mark (half a batch), so the queue is refilled before it is empty. The records of a partition
 * that is revoked are dropped from the queue with it. [next] takes the first event the pause allows, so cancelling it loses
 * nothing: the event stays in the queue.
 *
 * **Eager rebalances** (the default `range` assignor): all partitions are revoked, then the new assignment is made, so a
 * subscription sees [SubscriptionEvent.Revoked] of what it held, then [SubscriptionEvent.Assigned] of what it holds now. The
 * acknowledgements of the revoked partitions are committed before they are given up.
 */
internal class KafkaSubscription private constructor(
    private val spec: SubscriptionSpec,
    private val settings: KafkaEventStoreSettings,
    private val consumer: Consumer<String, ByteArray>,
) : Subscription {
    private val log = LoggerFactory.getLogger(KafkaSubscription::class.java)
    private val topic = spec.source

    /** What the poll thread tracks of one assigned partition. */
    private class Track(committed: Long) {
        /** The next offset [next] delivers; -1 until known (the first delivery or the poll thread's look at the consumer). */
        val position = AtomicLong(-1)

        /** The group's committed offset; -1 when none or not known yet. */
        @Volatile var committed = committed

        /** Whether [committed] was read from the group (false: the read failed in the rebalance listener and is retried). */
        @Volatile var committedKnown = true

        @Volatile var head = -1L
    }

    private val tracks = ConcurrentSkipListMap<Int, Track>()

    /** Guards [queue]; also makes "enqueue what a poll returned" atomic with [redeliver] dropping the queued records. */
    private val lock = ReentrantLock()
    private val queue = ArrayDeque<SubscriptionEvent>()
    private var queuedRecords = 0
    private val drained = lock.newCondition()
    private val high = settings.maxPollRecords
    private val low = maxOf(1, settings.maxPollRecords / 2)
    private val signal = Channel<Unit>(Channel.CONFLATED)

    /** The offset after the last acknowledged one, per partition. */
    private val acked = ConcurrentHashMap<Int, Long>()
    private val seeks = ConcurrentLinkedQueue<Pair<Int, Long>>()

    @Volatile private var paused = false

    @Volatile private var closing = false

    @Volatile private var failure: Throwable? = null

    /** The latest trouble recovered from since the last [takeError] (a commit or a committed-offset read that failed). */
    private val recovered = AtomicReference<String?>(null)
    private val finished = CompletableDeferred<Unit>()
    private lateinit var thread: Thread
    private var nextRefresh = 0L

    override suspend fun next(): SubscriptionEvent {
        while (true) {
            if (closing) throw EventStoreException.Failed("the subscription ${spec.name} is closed")
            failure?.let { throw EventStoreException.Failed("the subscription ${spec.name} failed: ${it.message ?: it::class.java.simpleName}", it) }
            take()?.let { return it }
            signal.receive() // only a hint: the queue is checked first, so a cancelled receive loses nothing
        }
    }

    private fun take(): SubscriptionEvent? = lock.withLock {
        val it = queue.iterator()
        while (it.hasNext()) {
            val event = it.next()
            if (event is Delivery) {
                if (paused) continue
                it.remove()
                if (--queuedRecords <= low) drained.signalAll()
                tracks[event.segment.toInt()]?.position?.set(event.position.toLong() + 1)
            } else {
                it.remove()
            }
            return@withLock event
        }
        null
    }

    override fun ack(segment: String, position: String) {
        val partition = segment.toIntOrNull() ?: throw EventStoreException.Invalid("segment \"$segment\" is not a partition")
        val offset = position.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$position\" is not an offset")
        acked.merge(partition, offset + 1, ::maxOf)
    }

    override fun redeliver(segment: String, position: String) {
        val partition = segment.toIntOrNull() ?: throw EventStoreException.Invalid("segment \"$segment\" is not a partition")
        val offset = position.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$position\" is not an offset")
        lock.withLock {
            seeks.add(partition to offset)
            queue.removeAll { it is Delivery && it.segment == segment }
            queuedRecords = queue.count { e -> e is Delivery }
            drained.signalAll()
            tracks[partition]?.position?.set(offset)
        }
    }

    override fun pause() {
        paused = true
    }

    override fun resume() {
        paused = false
        signal.trySend(Unit)
        lock.withLock { drained.signalAll() }
    }

    override fun segments(): List<SegmentStatus> = tracks.entries.map { (partition, t) ->
        val position = t.position.get().takeIf { it >= 0 }
        val committed = t.committed.takeIf { it >= 0 }
        val head = t.head.takeIf { it >= 0 }
        val from = committed ?: position
        SegmentStatus(
            segment = partition.toString(),
            position = position?.toString(),
            committed = committed?.toString(),
            head = head?.toString(),
            lag = if (head != null && from != null) maxOf(0L, head - from) else null,
        )
    }.sortedWith(compareBy(SegmentOrder) { it.segment })

    override fun takeError(): String? = recovered.getAndSet(null)

    override suspend fun close() {
        closing = true
        signal.trySend(Unit)
        finished.await()
    }

    // ------------------------------------------------------------------------------------------------- the poll thread

    private fun run() {
        try {
            consumer.subscribe(listOf(topic), Rebalances())
            while (!closing) {
                // Flow control, decided before each poll: hold (paused partitions, a poll that only keeps the membership alive)
                // while the user paused or a batch waits in the queue; fetch again once the queue is below the low mark.
                val hold = holding()
                applyPause(hold)
                if (hold) {
                    lock.withLock { if (holding() && !closing) drained.await(HOLD_MS, TimeUnit.MILLISECONDS) }
                }
                val records = consumer.poll(if (hold) Duration.ZERO else POLL)
                var queued = false
                lock.withLock {
                    val moved = HashSet<Int>()
                    while (true) {
                        val (partition, offset) = seeks.poll() ?: break
                        val tp = TopicPartition(topic, partition)
                        if (partition in tracks) {
                            consumer.seek(tp, offset)
                            moved += partition
                        }
                    }
                    // What a poll returned for a partition that was just moved comes from before the move: it is read again.
                    for (record in records) {
                        if (record.partition() in moved || record.partition() !in tracks) continue
                        queue.addLast(toDelivery(record))
                        queuedRecords++
                        queued = true
                    }
                }
                if (queued) signal.trySend(Unit)
                commit()
                refreshStatus()
            }
            // Not redundant: an ack recorded while close() raced the last iteration (after its commit) is flushed here.
            commit()
        } catch (e: Throwable) {
            failure = e
            log.warn("Subscription {} of {} failed", spec.name, topic, e)
            signal.trySend(Unit)
        } finally {
            runCatching { consumer.close(Duration.ofSeconds(10)) }
            finished.complete(Unit)
        }
    }

    /** Whether fetching is held: the user paused, or the queue holds a batch (until it drains to the low mark; between the marks it stays as it was). */
    private fun holding(): Boolean = paused || lock.withLock {
        if (queuedRecords >= high) flowHeld = true else if (queuedRecords <= low) flowHeld = false
        flowHeld
    }

    private var flowHeld = false

    /** Pauses every assigned partition when [hold], else resumes the paused ones. */
    private fun applyPause(hold: Boolean) {
        val already = consumer.paused()
        if (hold) {
            val toPause = consumer.assignment() - already
            if (toPause.isNotEmpty()) consumer.pause(toPause)
        } else if (already.isNotEmpty()) {
            consumer.resume(already)
        }
    }

    /** Commits what was acknowledged since, for the partitions this subscription holds. */
    private fun commit(only: Collection<Int>? = null) {
        val offsets = HashMap<TopicPartition, OffsetAndMetadata>()
        for ((partition, offset) in acked) {
            if (only != null && partition !in only) continue
            val track = tracks[partition] ?: continue
            // Never backwards relative to what this subscription knows. While the group's offset is unknown (its lookup failed) an
            // ack still commits: the worst case is a late ack below another instance's offset, a few duplicates (at-least-once).
            if (offset > track.committed) offsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset)
        }
        if (offsets.isEmpty()) return
        try {
            consumer.commitSync(offsets, settings.adminTimeout)
            for ((tp, offset) in offsets) tracks[tp.partition()]?.committed = offset.offset()
        } catch (e: Exception) {
            // The partition moved to another instance or the brokers are away: what was not committed comes again.
            log.warn("Subscription {} of {} could not commit {}: {}", spec.name, topic, offsets.mapKeys { it.key.partition() }.mapValues { it.value.offset() }, e.toString())
            recovered.set("kafka commit: $e")
        }
    }

    /**
     * Refreshes what [segments] reports without holding the fetching up. The hot path asks nothing of the brokers: the head is the
     * consumer's position plus its `currentLag` (what the fetch responses reported). A partition without a lag (not fetched yet)
     * or that the user paused (nothing is fetched for it, so its lag would freeze) gets its end offset from the brokers, at this
     * cadence, with a short timeout. A committed offset the rebalance listener could not read is looked up again.
     */
    private fun refreshStatus() {
        val now = System.nanoTime()
        if (now - nextRefresh < 0) return
        nextRefresh = now + settings.statusRefresh.toNanos()
        val assignment = consumer.assignment()
        if (assignment.isEmpty()) return
        val ask = HashSet<TopicPartition>()
        for (tp in assignment) {
            val track = tracks[tp.partition()] ?: continue
            if (track.position.get() < 0) {
                // The first position is the first record still queued for the partition, never the consumer's (which is past them).
                lock.withLock {
                    val queued = queue.firstOrNull { it is Delivery && it.segment == tp.partition().toString() } as Delivery?
                    if (queued != null) {
                        track.position.compareAndSet(-1, queued.position.toLong())
                    } else {
                        try {
                            track.position.compareAndSet(-1, consumer.position(tp, Duration.ZERO))
                        } catch (_: Exception) {
                            // not known yet
                        }
                    }
                }
            }
            val lag = if (paused) java.util.OptionalLong.empty() else consumer.currentLag(tp)
            val fetched = try {
                consumer.position(tp, Duration.ZERO)
            } catch (_: Exception) {
                -1L
            }
            if (lag.isPresent && fetched >= 0) track.head = fetched + lag.asLong else ask += tp
        }
        if (ask.isNotEmpty()) {
            // A running partition with no currentLag yet (before its first fetch) is asked too: until that fetch the call may hold
            // the poll thread for up to END_OFFSETS_LOOKUP per refresh.
            try {
                for ((tp, end) in consumer.endOffsets(ask, END_OFFSETS_LOOKUP)) tracks[tp.partition()]?.head = end
            } catch (e: Exception) {
                log.debug("Subscription {} could not read the end offsets of {}: {}", spec.name, topic, e.toString())
            }
        }
        val unknown = assignment.filter { tracks[it.partition()]?.committedKnown == false }.toSet()
        if (unknown.isNotEmpty()) {
            try {
                for ((tp, committed) in consumer.committed(unknown, COMMITTED_LOOKUP)) {
                    val track = tracks[tp.partition()] ?: continue
                    track.committed = committed?.offset() ?: -1
                    track.committedKnown = true
                }
            } catch (e: Exception) {
                log.debug("Subscription {} could not read the committed offsets of {}: {}", spec.name, topic, e.toString())
            }
        }
    }

    private fun toDelivery(record: org.apache.kafka.clients.consumer.ConsumerRecord<String, ByteArray>): Delivery {
        val headers = LinkedHashMap<String, String>()
        record.headers().forEach { headers[it.key()] = String(it.value() ?: ByteArray(0), Charsets.UTF_8) }
        return Delivery(
            segment = record.partition().toString(),
            position = record.offset().toString(),
            key = record.key(),
            headers = headers,
            payload = record.value() ?: ByteArray(0),
            timestampMs = record.timestamp(),
        )
    }

    /** Runs on the poll thread, inside `poll`. */
    private inner class Rebalances : ConsumerRebalanceListener {
        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            if (partitions.isEmpty()) {
                // An assignment of nothing (more instances than partitions) is announced too: the instance is a member, idle.
                lock.withLock { queue.addLast(SubscriptionEvent.Assigned(emptyList())) }
                signal.trySend(Unit)
                return
            }
            // A broker that does not answer here must not fail the subscription (an exception from a listener ends the poll): what
            // cannot be read now is unknown and read again by the poll thread; the assignment itself is always taken.
            val committed = try {
                consumer.committed(partitions.toSet(), settings.adminTimeout)
            } catch (e: Exception) {
                log.warn("Subscription {} could not read the committed offsets of {}, retrying: {}", spec.name, topic, e.toString())
                recovered.set("rebalance: could not read the committed offsets: $e")
                null
            }
            for (tp in partitions) {
                acked.remove(tp.partition())
                val track = Track(committed = committed?.get(tp)?.offset() ?: -1)
                track.committedKnown = committed != null
                tracks[tp.partition()] = track
            }
            if (holding()) consumer.pause(partitions)
            lock.withLock { queue.addLast(SubscriptionEvent.Assigned(partitions.map { it.partition().toString() }.sortedWith(SegmentOrder))) }
            signal.trySend(Unit)
        }

        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            if (partitions.isEmpty()) return
            commit(partitions.map { it.partition() })
            giveUp(partitions)
        }

        override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
            if (partitions.isEmpty()) return
            giveUp(partitions)
        }

        private fun giveUp(partitions: Collection<TopicPartition>) {
            val segments = partitions.map { it.partition().toString() }
            val numbers = partitions.map { it.partition() }.toSet()
            lock.withLock {
                seeks.removeIf { it.first in numbers }
                queue.removeAll { it is Delivery && it.segment in segments }
                queuedRecords = queue.count { it is Delivery }
                drained.signalAll()
                queue.addLast(SubscriptionEvent.Revoked(segments.sortedWith(SegmentOrder)))
            }
            for (tp in partitions) {
                tracks.remove(tp.partition())
                acked.remove(tp.partition())
            }
            signal.trySend(Unit)
        }
    }

    companion object {
        private val POLL: Duration = Duration.ofMillis(100)

        /** The longest a poll thread waits for the queue to drain while it holds the partitions paused. */
        private const val HOLD_MS = 20L
        private val COMMITTED_LOOKUP: Duration = Duration.ofMillis(200)
        private val END_OFFSETS_LOOKUP: Duration = Duration.ofSeconds(1)

        /** Creates the consumer of [spec] and starts its thread; the group is joined by the first polls. */
        fun start(spec: SubscriptionSpec, settings: KafkaEventStoreSettings, number: Int): KafkaSubscription {
            return start(spec, settings, KafkaConsumer(settings.consumerProperties(spec.name, number)))
        }

        /** Starts the subscription over [consumer] (a `MockConsumer` in tests). */
        fun start(spec: SubscriptionSpec, settings: KafkaEventStoreSettings, consumer: Consumer<String, ByteArray>): KafkaSubscription {
            val subscription = KafkaSubscription(spec, settings, consumer)
            subscription.thread = Thread(subscription::run, "snodra-kafka-${spec.name}").apply { isDaemon = true }
            subscription.thread.start()
            return subscription
        }
    }
}
