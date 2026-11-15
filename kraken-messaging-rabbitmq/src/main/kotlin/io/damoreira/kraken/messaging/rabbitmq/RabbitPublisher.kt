package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Constants
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.function.ToLongFunction
import kotlin.concurrent.withLock

/**
 * The publishing side of [RabbitEventStore]: one connection per leader node, one publisher (no name: no deduplication) per
 * partition stream on it. Each event is one message in its own publish frame (protocol version 1, never a sub-entry batch, never
 * compressed), and [publish] returns once the broker confirmed it, at most [SEND_TIMEOUT]. A source's partitions are read again
 * every [LAYOUT_FOR].
 */
internal class RabbitPublisher(private val connector: RabbitConnector) : AutoCloseable {
    private val log = LoggerFactory.getLogger(RabbitPublisher::class.java)

    /** Guards the connections, the publishers and the layouts (the setup of a publish; never held while a confirm is awaited). */
    private val lock = ReentrantLock()
    private var control: Client? = null
    private val layouts = HashMap<String, Pair<List<Part>, Long>>()
    private val nodes = HashMap<Pair<String, Int>, Node>()

    @Volatile private var closed = false

    /** One leader node's connection: its publishers by stream and the publishes waiting for their confirm. */
    private inner class Node(val key: Pair<String, Int>) {
        val pending = ConcurrentHashMap<Long, CompletableFuture<Unit>>()
        val sequence = AtomicLong()
        val publishers = HashMap<String, Byte>()
        var nextId = 0

        val client: Client = connector.node(Client.Broker(key.first, key.second)) { p ->
            p.publishConfirmListener { _, id -> pending.remove(id)?.complete(Unit) }
            p.publishErrorListener { _, id, code -> pending.remove(id)?.completeExceptionally(PublishError(code)) }
            p.shutdownListener { context -> lost("the connection closed (${context.shutdownReason})") }
            p.metadataListener { stream, _ -> lock.withLock { publishers.remove(stream) } }
        }

        fun lost(why: String) {
            lock.withLock { if (nodes[key] === this) nodes.remove(key) }
            val failure = EventStoreException.Failed("rabbitmq: publishing through ${key.first}:${key.second}: $why")
            pending.keys.toList().forEach { pending.remove(it)?.completeExceptionally(failure) }
        }
    }

    private class PublishError(val code: Short) : RuntimeException("the broker refused the message: ${RabbitConnector.codeName(code)}")

    suspend fun publish(event: OutgoingEvent) {
        if (event.source.isBlank()) throw EventStoreException.Invalid("the event has no source")
        val message = RabbitWire.message(event)
        val size = RabbitWire.codec.encode(message).size + FRAME_OVERHEAD
        if (size > MAX_FRAME) {
            throw EventStoreException.Rejected("the event ${event.id} is $size bytes on the wire, over the $MAX_FRAME-byte frame")
        }
        val (node, publisher, stream) = withContext(Dispatchers.IO) { route(event) }
        val confirmed = CompletableFuture<Unit>()
        val id = node.sequence.incrementAndGet()
        node.pending[id] = confirmed
        try {
            node.client.publish(publisher, listOf(message), ToLongFunction<Any> { id })
        } catch (e: Exception) {
            node.pending.remove(id)
            node.lost("publish failed: ${e.message}")
            runCatching { node.client.close() }
            throw failedOrRejected(event, stream, e)
        }
        val outcome = withTimeoutOrNull(SEND_TIMEOUT.toMillis()) { runCatching { confirmed.await() } }
        if (outcome == null) {
            node.pending.remove(id)
            // A connection that does not confirm within the bound is replaced: it may have stopped reading.
            node.lost("no confirm within $SEND_TIMEOUT")
            runCatching { node.client.close() }
            throw EventStoreException.Failed("rabbitmq: publishing ${event.id} to $stream: no confirm within $SEND_TIMEOUT")
        }
        outcome.exceptionOrNull()?.let { throw failedOrRejected(event, stream, it) }
    }

    private fun failedOrRejected(event: OutgoingEvent, stream: String, e: Throwable): EventStoreException {
        if (e is EventStoreException) return e
        val why = "publishing ${event.id} to $stream: ${e.message ?: e::class.java.simpleName}"
        val code = (e as? PublishError)?.code
        return if (code == Constants.RESPONSE_CODE_ACCESS_REFUSED || code == Constants.RESPONSE_CODE_FRAME_TOO_LARGE || e is IllegalArgumentException) {
            EventStoreException.Rejected(why, e)
        } else {
            EventStoreException.Failed(why, e)
        }
    }

    /** The node, the publisher and the partition stream of [event] (declaring what is missing). Blocking. */
    private fun route(event: OutgoingEvent): Triple<Node, Byte, String> = lock.withLock {
        if (closed) throw EventStoreException.Failed("rabbitmq: the event store is closed")
        try {
            val parts = layout(event.source)
            val stream = parts[RabbitWire.partitionIndex(RabbitWire.routingKey(event), parts.size)].stream
            val leader = RabbitConnector.leader(control(), stream)
            val key = leader.host to leader.port
            var node = nodes[key]
            if (node == null || !node.client.isOpen || node.nextId > MAX_PUBLISHER_ID) {
                node?.let { old -> nodes.remove(key); runCatching { old.client.close() } }
                node = Node(key).also { nodes[key] = it }
            }
            val publisher = node.publishers[stream] ?: run {
                val id = node.nextId++.toByte()
                val response = node.client.declarePublisher(id, null, stream)
                if (!response.isOk) {
                    val why = "rabbitmq: declaring a publisher on $stream: ${RabbitConnector.codeName(response.responseCode)}"
                    throw if (response.responseCode == Constants.RESPONSE_CODE_ACCESS_REFUSED) EventStoreException.Rejected(why) else EventStoreException.Failed(why)
                }
                id.also { node.publishers[stream] = it }
            }
            Triple(node, publisher, stream)
        } catch (e: EventStoreException) {
            if (e is EventStoreException.Failed) dropControl()
            throw e
        } catch (e: Exception) {
            dropControl()
            throw EventStoreException.Failed("rabbitmq: publishing ${event.id} to ${event.source}: ${e.message ?: e::class.java.simpleName}", e)
        }
    }

    /** The partitions of [source], read again every [LAYOUT_FOR]. */
    private fun layout(source: String): List<Part> {
        val now = System.nanoTime()
        layouts[source]?.let { (parts, at) -> if (now - at < LAYOUT_FOR.toNanos()) return parts }
        val parts = RabbitConnector.layout(control(), source) ?: throw EventStoreException.Failed(
            "rabbitmq: neither a super stream nor a stream named $source exists: create it (rabbitmq-streams add_super_stream $source --partitions 6)",
        )
        layouts[source] = parts to now
        return parts
    }

    private fun control(): Client = control?.takeIf { it.isOpen } ?: connector.seed().also { control = it }

    private fun dropControl() {
        control?.let { runCatching { it.close() } }
        control = null
    }

    override fun close() {
        lock.withLock {
            closed = true
            for (node in nodes.values) runCatching { node.client.close() }
            nodes.clear()
            dropControl()
        }
        log.debug("RabbitMQ Streams publisher closed")
    }

    companion object {
        /** The longest [publish] waits for the broker's confirm. */
        val SEND_TIMEOUT: java.time.Duration = java.time.Duration.ofSeconds(10)

        /** How long a source's partitions are used before they are read again. */
        val LAYOUT_FOR: java.time.Duration = java.time.Duration.ofSeconds(30)

        /** The broker's default frame (1 MiB): a larger event is [EventStoreException.Rejected] before any connection. */
        const val MAX_FRAME = 1_048_576

        /** A publish frame's own bytes around one message: size, key, version, publisher id, count, publishing id, length. */
        const val FRAME_OVERHEAD = 4 + 2 + 2 + 1 + 4 + 8 + 4

        /** Publisher ids are one byte per connection; past the last one the connection is replaced, never wrapped. */
        private const val MAX_PUBLISHER_ID = 255
    }
}
