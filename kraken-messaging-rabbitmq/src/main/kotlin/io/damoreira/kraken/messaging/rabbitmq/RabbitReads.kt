package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * A stream's offsets: the first one ([first], a chunk's first offset), the one after its last event ([head]), and the time of its
 * last chunk of events ([lastTimestampMs], null when it holds no event).
 */
internal data class Bounds(val first: Long, val head: Long, val lastTimestampMs: Long?) {
    companion object {
        val EMPTY = Bounds(0, 0, null)
    }
}

/** One chunk of events as a read sees it: its first offset, its records and its time. */
internal data class ChunkRead(val first: Long, val count: Long, val timestampMs: Long)

/**
 * What decides correctness — stored offsets, stream statistics, the first and last chunk of events, the chunk at a time — read on
 * each stream's **leader** (a replica catching up could answer stale: "empty", "no offset"); the source's layout and the streams'
 * leaders come from a control connection to the first configured host that answers. One read at a time; connections are opened
 * on first use and kept until [close].
 *
 * Offset-tracking entries take offsets too: the head is read from the last chunk of events (the statistics' committed offset may
 * lie past it), and a stream whose statistics show chunks but whose first and last reads both bring no event within [QUIET] holds
 * only tracking entries (empty of events); one read answering and the other not is a failure, never "empty".
 */
internal class RabbitReads(private val connector: RabbitConnector) : AutoCloseable {
    private class Chunk(val first: Long, val count: Long) {
        @Volatile var seen = false
    }

    /** One leader node's connection and what its reads receive. */
    private inner class Node(val node: Client.Broker) {
        val reads = LinkedBlockingQueue<ChunkRead>()
        val client: Client = connector.node(node) { p ->
            p.chunkListener { _, _, offset, count, _ -> Chunk(offset, count) }
            p.messageListener { _, _, timestamp, _, context, _ ->
                val chunk = context as Chunk
                if (!chunk.seen) {
                    chunk.seen = true
                    reads.add(ChunkRead(chunk.first, chunk.count, timestamp))
                }
            }
        }
    }

    private var control: Client? = null
    private val nodes = HashMap<Pair<String, Int>, Node>()

    @Synchronized
    fun control(): Client = control?.takeIf { it.isOpen } ?: connector.seed().also { control = it }

    /** The partitions of [source] (see [RabbitConnector.layout]). */
    @Synchronized
    fun layout(source: String): List<Part>? = guarded { RabbitConnector.layout(control(), source) }

    /** The connection to the leader of [stream]. */
    @Synchronized
    fun leader(stream: String): Client = node(stream).client

    private fun node(stream: String): Node = guarded {
        val leader = RabbitConnector.leader(control(), stream)
        val key = leader.host to leader.port
        nodes[key]?.takeIf { it.client.isOpen } ?: Node(leader).also { nodes[key] = it }
    }

    /** The offset stored under [reference] on [stream], read on its leader; null when none. */
    @Synchronized
    fun storedOffset(reference: String, stream: String): Long? = guarded { RabbitConnector.storedOffset(leader(stream), reference, stream) }

    /** Stores [offset] through [writer] (unless [store] is false) and reads it back on the leader (see [RabbitConnector.storeAndConfirm]). */
    @Synchronized
    fun storeAndConfirm(writer: Client?, reference: String, stream: String, offset: Long, wait: Duration, store: Boolean = true) = guarded {
        val leader = leader(stream)
        RabbitConnector.storeAndConfirm(writer ?: leader, leader, reference, stream, offset, wait, store)
    }

    /** The statistics of [stream] on its leader: its first chunk's offset (null when it holds no chunk) and its committed chunk id. */
    @Synchronized
    fun stats(stream: String): Pair<Long?, Long?> = guarded {
        val response = try {
            leader(stream).streamStats(stream)
        } catch (e: EventStoreException) {
            throw e
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: the statistics of $stream: ${e.message}", e)
        }
        if (!response.isOk) throw EventStoreException.Failed("rabbitmq: the statistics of $stream: ${RabbitConnector.codeName(response.responseCode)}")
        response.info["first_chunk_id"]?.takeIf { it >= 0 } to response.info["committed_chunk_id"]?.takeIf { it >= 0 }
    }

    /** The bounds of [stream], read on its leader (see the class docs); [EventStoreException.Failed] on a mixed answer. */
    @Synchronized
    fun bounds(stream: String): Bounds {
        val (first, committed) = stats(stream)
        if (first == null) return Bounds.EMPTY
        val key = node(stream).node.let { "${it.host}:${it.port}/$stream" } // the same name on another broker is another stream
        return boundsWith(stream, key, first, committed, eventless) { last ->
            readOne(stream, if (last) OffsetSpecification.last() else OffsetSpecification.first())
        }
    }

    /**
     * The first offset of [stream] whose chunk was written at or after [timestampMs], read on its leader; the head when its last
     * chunk of events is older (no read). A read that does not come within [wait] is [EventStoreException.Failed].
     */
    @Synchronized
    fun offsetAt(stream: String, timestampMs: Long, bounds: Bounds, wait: Duration): Long {
        val node = node(stream)
        try {
            return offsetAt(stream, timestampMs, bounds) { first ->
                if (first) subscribe(node, stream, OffsetSpecification.timestamp(timestampMs)) else credit(node)
                node.reads.poll(wait.toMillis(), TimeUnit.MILLISECONDS)
            }
        } finally {
            unsubscribe(node)
        }
    }

    private fun readOne(stream: String, from: OffsetSpecification): ChunkRead? {
        val node = node(stream)
        subscribe(node, stream, from)
        try {
            return node.reads.poll(QUIET.toMillis(), TimeUnit.MILLISECONDS)
        } finally {
            unsubscribe(node)
        }
    }

    private fun subscribe(node: Node, stream: String, from: OffsetSpecification) {
        node.reads.clear()
        val response = try {
            node.client.subscribe(SUBSCRIPTION, stream, from, 1)
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: reading $stream: ${e.message}", e)
        }
        if (!response.isOk) throw EventStoreException.Failed("rabbitmq: reading $stream: ${RabbitConnector.codeName(response.responseCode)}")
    }

    private fun credit(node: Node) = try {
        node.client.credit(SUBSCRIPTION, 1)
    } catch (e: Exception) {
        throw EventStoreException.Failed("rabbitmq: reading: ${e.message}", e)
    }

    private fun unsubscribe(node: Node) {
        runCatching { node.client.unsubscribe(SUBSCRIPTION) }
    }

    /** A connection that failed is dropped (opened again on next use). */
    private fun <T> guarded(block: () -> T): T = try {
        block()
    } catch (e: EventStoreException.Failed) {
        drop()
        throw e
    }

    private fun drop() {
        control?.let { runCatching { it.close() } }
        control = null
        nodes.values.forEach { runCatching { it.client.close() } }
        nodes.clear()
    }

    @Synchronized
    override fun close() = drop()

    companion object {
        /** How long a read of the first or last chunk waits before it is taken for "no event". */
        val QUIET: Duration = Duration.ofMillis(1500)

        private const val SUBSCRIPTION: Byte = 0

        /** Streams found empty of events, for the whole process (see [EventlessStreams]). */
        private val eventless = EventlessStreams()

        /**
         * The bounds of [stream] (statistics: first chunk [first], committed chunk [committed]) known to [eventless] under [key]:
         * empty without a read while its statistics are those it was found event-less with; otherwise the two reads of
         * [boundsOf] ([read] with true: the last chunk of events, false: the first).
         */
        fun boundsWith(stream: String, key: String, first: Long, committed: Long?, eventless: EventlessStreams, read: (Boolean) -> ChunkRead?): Bounds {
            if (committed != null && eventless.known(key, first, committed)) return Bounds.EMPTY
            // Chunks added since it was found event-less take both reads: one quiet read is no proof (a stalled read is silent too).
            val lastRead = read(true)
            val bounds = boundsOf(stream, first, read(false), lastRead)
            if (bounds == Bounds.EMPTY && committed != null) eventless.remember(key, first, committed)
            return bounds
        }

        /**
         * The bounds of [stream] whose statistics show chunks from [first] on: empty of events when neither read brought a chunk
         * of events (only tracking entries), a failure when one did and the other did not.
         */
        fun boundsOf(stream: String, first: Long, firstRead: ChunkRead?, lastRead: ChunkRead?): Bounds = when {
            firstRead == null && lastRead == null -> Bounds.EMPTY
            firstRead == null || lastRead == null -> throw EventStoreException.Failed(
                "rabbitmq: $stream answered one of its first and last chunks and not the other within $QUIET",
            )
            else -> Bounds(first, maxOf(first, lastRead.first + lastRead.count), lastRead.timestampMs)
        }

        /**
         * The offset at [timestampMs] (see the instance method) from the chunks [next] reads (true for the first read); a read that
         * brings nothing is a failure, never the head.
         */
        fun offsetAt(stream: String, timestampMs: Long, bounds: Bounds, next: (Boolean) -> ChunkRead?): Long {
            val last = bounds.lastTimestampMs
            if (last == null || last < timestampMs) return bounds.head
            var first = true
            while (true) {
                val chunk = next(first) ?: throw EventStoreException.Failed("rabbitmq: reading $stream at the time $timestampMs: no answer in time")
                first = false
                if (chunk.timestampMs >= timestampMs) return maxOf(chunk.first, bounds.first)
                if (chunk.first + chunk.count >= bounds.head) return bounds.head
            }
        }
    }
}

/**
 * Streams found empty of events (only offset-tracking entries), each with its statistics then (first and committed chunk ids):
 * not read again (two quiet reads) while they are unchanged. Bounded: at most [capacity] streams, the least recently used
 * pruned, and each kept for [ttl] at most. The stream protocol offers nothing that tells one incarnation of a stream from the
 * next (no creation id in its metadata or statistics), so a stream deleted and created again under the same name, whose
 * statistics happen to equal what an event-less one had, reads as empty until [ttl] passes: a reset then starts that
 * partition at its beginning (events read again, never lost).
 */
internal class EventlessStreams(
    private val capacity: Int = 4096,
    private val ttl: Duration = Duration.ofMinutes(10),
    private val now: () -> Long = System::nanoTime,
) {
    private class Seen(val first: Long, val committed: Long, val at: Long)

    private val seen = object : LinkedHashMap<String, Seen>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Seen>?) = size > capacity
    }

    /** Whether the stream [key] was found event-less with exactly these statistics, within [ttl]. */
    @Synchronized
    fun known(key: String, first: Long, committed: Long): Boolean {
        val s = seen[key] ?: return false
        if (now() - s.at > ttl.toNanos()) {
            seen.remove(key)
            return false
        }
        return s.first == first && s.committed == committed
    }

    @Synchronized
    fun remember(key: String, first: Long, committed: Long) {
        seen[key] = Seen(first, committed, now())
    }

    @Synchronized
    fun size(): Int = seen.size
}
