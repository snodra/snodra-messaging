package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.SegmentStatus
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
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
 * records of one poll, and polls again only once the queue holds no record: while records wait, the consumer's partitions are
 * paused (a paused poll still keeps the group membership alive), so the records of a partition never queue behind a revoke.
 * [next] takes the first event the pause allows, so cancelling it loses nothing: the event stays in the queue.
 *
 * **Eager rebalances** (the default `range` assignor): all partitions are revoked, then the new assignment is made, so a
 * subscription sees [SubscriptionEvent.Revoked] of what it held, then [SubscriptionEvent.Assigned] of what it holds now. The
 * acknowledgements of the revoked partitions are committed before they are given up.
 */
internal class KafkaSubscription private constructor(
    private val spec: SubscriptionSpec,
    private val settings: KafkaEventStoreSettings,
    private val consumer: KafkaConsumer<String, ByteArray>,
) : Subscription {
    private val log = LoggerFactory.getLogger(KafkaSubscription::class.java)
    private val topic = spec.source

    /** What the poll thread tracks of one assigned partition. */
    private class Track(position: Long, committed: Long, head: Long) {
        /** The next offset [next] delivers. */
        val position = AtomicLong(position)

        @Volatile var committed = committed

        @Volatile var head = head
    }

    private val tracks = ConcurrentSkipListMap<Int, Track>()

    /** Guards [queue]; also makes "enqueue what a poll returned" atomic with [redeliver] dropping the queued records. */
    private val lock = ReentrantLock()
    private val queue = ArrayDeque<SubscriptionEvent>()
    private var queuedRecords = 0
    private val signal = Channel<Unit>(Channel.CONFLATED)

    /** The offset after the last acknowledged one, per partition. */
    private val acked = ConcurrentHashMap<Int, Long>()
    private val seeks = ConcurrentLinkedQueue<Pair<Int, Long>>()

    @Volatile private var paused = false

    @Volatile private var closing = false

    @Volatile private var failure: Throwable? = null
    private val finished = CompletableDeferred<Unit>()
    private lateinit var thread: Thread
    private var nextRefresh = 0L

    override suspend fun next(): SubscriptionEvent {
        while (true) {
            take()?.let { return it }
            failure?.let { throw EventStoreException.Failed("the subscription ${spec.name} failed: ${it.message ?: it::class.java.simpleName}", it) }
            if (closing) throw EventStoreException.Failed("the subscription ${spec.name} is closed")
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
                queuedRecords--
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
            queue.removeAll { it is Delivery && it.segment == segment }.also { queuedRecords = queue.count { e -> e is Delivery } }
            tracks[partition]?.position?.set(offset)
        }
    }

    override fun pause() {
        paused = true
    }

    override fun resume() {
        paused = false
        signal.trySend(Unit)
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
    }.sortedBy { it.segment }

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
                val records = try {
                    consumer.poll(POLL)
                } catch (_: WakeupException) {
                    continue
                }
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
                applyPause()
                refreshStatus()
            }
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

    /** Records are polled only while none waits in the queue (and the subscription is not paused): the others are paused. */
    private fun applyPause() {
        val hold = paused || lock.withLock { queuedRecords > 0 }
        val assignment = consumer.assignment()
        val already = consumer.paused()
        if (hold) {
            val toPause = assignment - already
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
            if (offset > track.committed) offsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset)
        }
        if (offsets.isEmpty()) return
        try {
            consumer.commitSync(offsets, settings.adminTimeout)
            for ((tp, offset) in offsets) tracks[tp.partition()]?.committed = offset.offset()
        } catch (e: Exception) {
            // The partition moved to another instance or the brokers are away: what was not committed comes again.
            log.warn("Subscription {} of {} could not commit {}: {}", spec.name, topic, offsets.mapKeys { it.key.partition() }.mapValues { it.value.offset() }, e.toString())
        }
    }

    private fun refreshStatus() {
        val now = System.nanoTime()
        if (now - nextRefresh < 0) return
        nextRefresh = now + settings.statusRefresh.toNanos()
        val assignment = consumer.assignment()
        if (assignment.isEmpty()) return
        try {
            for ((tp, end) in consumer.endOffsets(assignment, settings.adminTimeout)) tracks[tp.partition()]?.head = end
        } catch (e: Exception) {
            log.debug("Subscription {} could not read the end offsets of {}: {}", spec.name, topic, e.toString())
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
            if (partitions.isEmpty()) return
            val committed = consumer.committed(partitions.toSet(), settings.adminTimeout)
            val head = runCatching { consumer.endOffsets(partitions, settings.adminTimeout) }.getOrDefault(emptyMap())
            for (tp in partitions) {
                acked.remove(tp.partition())
                tracks[tp.partition()] = Track(
                    position = consumer.position(tp, settings.adminTimeout),
                    committed = committed[tp]?.offset() ?: -1,
                    head = head[tp] ?: -1,
                )
            }
            if (paused || lock.withLock { queuedRecords > 0 }) consumer.pause(partitions)
            lock.withLock { queue.addLast(SubscriptionEvent.Assigned(partitions.map { it.partition().toString() }.sorted())) }
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
            lock.withLock {
                queue.removeAll { it is Delivery && it.segment in segments }
                queuedRecords = queue.count { it is Delivery }
                queue.addLast(SubscriptionEvent.Revoked(segments.sorted()))
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

        /** Creates the consumer of [spec] and starts its thread; the group is joined by the first polls. */
        fun start(spec: SubscriptionSpec, settings: KafkaEventStoreSettings): KafkaSubscription {
            val consumer = KafkaConsumer<String, ByteArray>(settings.consumerProperties(spec.name))
            val subscription = KafkaSubscription(spec, settings, consumer)
            subscription.thread = Thread(subscription::run, "kraken-kafka-${spec.name}").apply { isDaemon = true }
            subscription.thread.start()
            return subscription
        }
    }
}
