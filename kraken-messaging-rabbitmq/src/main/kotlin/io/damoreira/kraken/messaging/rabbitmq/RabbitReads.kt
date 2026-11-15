package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** A stream's offsets: the first one ([first], a chunk's first offset) and the one after its last event ([head]). */
internal data class Bounds(val first: Long, val head: Long)

/**
 * Reads what the stream protocol has no command for, by reading chunks on a connection of its own (one read at a time):
 * where a stream ends (the last chunk of events: its first offset plus its messages; offset-tracking entries also take offsets,
 * so the committed offset of the stream statistics may lie past the last event) and the first chunk written at or after a time.
 * Whether a stream holds anything comes from its statistics (`first_chunk_id`), never from silence.
 */
internal class RabbitReads(connector: RabbitConnector) : AutoCloseable {
    private class Chunk(val first: Long, val count: Long) {
        @Volatile var seen = false
    }

    private class Read(val first: Long, val count: Long, val timestampMs: Long)

    private val reads = LinkedBlockingQueue<Read>()

    private val client: Client = connector.seed { p ->
        p.chunkListener { _, _, offset, count, _ -> Chunk(offset, count) }
        p.messageListener { _, _, timestamp, _, context, _ ->
            val chunk = context as Chunk
            if (!chunk.seen) {
                chunk.seen = true
                reads.add(Read(chunk.first, chunk.count, timestamp))
            }
        }
    }

    val isOpen: Boolean get() = client.isOpen

    /** The statistics' first offset of [stream]; null when it holds no chunk. */
    @Synchronized
    fun firstOffset(stream: String): Long? {
        val response = try {
            client.streamStats(stream)
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: the statistics of $stream: ${e.message}", e)
        }
        if (!response.isOk) throw EventStoreException.Failed("rabbitmq: the statistics of $stream: ${RabbitConnector.codeName(response.responseCode)}")
        return response.info["first_chunk_id"]?.takeIf { it >= 0 }
    }

    /**
     * The bounds of [stream]: (0, 0) when it holds nothing; else its first offset and the end of its last chunk of events, read
     * within [wait]. [EventStoreException.Failed] when that chunk does not come (it never says "empty" from silence).
     */
    @Synchronized
    fun bounds(stream: String, wait: Duration): Bounds {
        val first = firstOffset(stream) ?: return Bounds(0, 0)
        val head = head(stream, wait)
            ?: throw EventStoreException.Failed("rabbitmq: the last chunk of $stream did not come within $wait (it has chunks: $first on)")
        return Bounds(first, maxOf(first, head))
    }

    /** The end of the last chunk of events of [stream] (its first offset plus its messages), null when none came within [wait]. */
    @Synchronized
    fun head(stream: String, wait: Duration): Long? {
        var head: Long? = null
        read(stream, OffsetSpecification.last(), wait) { r ->
            head = r.first + r.count
            false
        }
        return head
    }

    /**
     * The offset of the first chunk of [stream] written at or after [timestampMs] (epoch milliseconds, the chunk's time), else
     * [bounds]' head. A time reads at most [wait], and once [QUIET] passes without a chunk the rest is not events: the head.
     */
    @Synchronized
    fun offsetAt(stream: String, timestampMs: Long, bounds: Bounds, wait: Duration): Long {
        if (bounds.head <= bounds.first) return bounds.head
        var found = bounds.head
        val deadline = System.nanoTime() + wait.toNanos()
        read(stream, OffsetSpecification.timestamp(timestampMs), QUIET) { r ->
            if (System.nanoTime() > deadline) throw EventStoreException.Failed("rabbitmq: the time $timestampMs in $stream: not found within $wait")
            when {
                r.timestampMs >= timestampMs -> { found = r.first; false }
                r.first + r.count >= bounds.head -> false
                else -> true
            }
        }
        return found
    }

    /** Reads [stream] from [from], one chunk at a time, while [more] says so; returns when a chunk does not come within [quiet]. */
    private fun read(stream: String, from: OffsetSpecification, quiet: Duration, more: (Read) -> Boolean) {
        reads.clear()
        val response = try {
            client.subscribe(SUBSCRIPTION, stream, from, 1)
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: reading $stream: ${e.message}", e)
        }
        if (!response.isOk) throw EventStoreException.Failed("rabbitmq: reading $stream: ${RabbitConnector.codeName(response.responseCode)}")
        try {
            while (true) {
                val r = reads.poll(quiet.toMillis(), TimeUnit.MILLISECONDS) ?: return
                if (!more(r)) return
                client.credit(SUBSCRIPTION, 1)
            }
        } finally {
            runCatching { client.unsubscribe(SUBSCRIPTION) }
        }
    }

    override fun close() {
        runCatching { client.close() }
    }

    companion object {
        /** How long a read by time waits for the next chunk before the rest is taken for no events. */
        val QUIET: Duration = Duration.ofMillis(1500)

        private const val SUBSCRIPTION: Byte = 0
    }
}
