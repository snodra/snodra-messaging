package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Constants
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.ToLongFunction

/**
 * The publishing side of [RabbitEventStore]: one connection per leader node, one publisher (no name: no deduplication) per
 * partition stream on it. Each event is one message in its own publish frame (protocol version 1, never a sub-entry batch, never
 * compressed), and [publish] returns once the broker confirmed it, at most [confirmTimeout] (a connection that does not confirm
 * within it is replaced). A source's partitions and each stream's leader are cached, looked up on a miss or after a failure and
 * again every [LAYOUT_FOR]; no lock is held across a call to the broker but a node's own while it declares a publisher on it.
 */
internal class RabbitPublisher(private val connector: RabbitConnector, private val confirmTimeout: Duration) : AutoCloseable {
    private class Cached<T>(val value: T, val at: Long) {
        fun fresh() = System.nanoTime() - at < LAYOUT_FOR.toNanos()
    }

    private val layouts = ConcurrentHashMap<String, Cached<List<Part>>>()
    private val leaders = ConcurrentHashMap<String, Cached<Pair<String, Int>>>()
    private val nodes = ConcurrentHashMap<Pair<String, Int>, Node>()
    private val controlLock = Any()
    private var control: Client? = null

    @Volatile private var closed = false

    /**
     * One leader node's connection (opened on first use): its publishers by stream and the publishes waiting for their confirm.
     * The client's own threads (listeners) never wait for this node's lock: what they report is handed over ([forget], [lost]).
     */
    internal inner class Node(val key: Pair<String, Int>) {
        val pending = ConcurrentHashMap<Long, CompletableFuture<Unit>>()
        val sequence = AtomicLong()
        private val publishers = HashMap<String, Byte>()
        private var nextId = 0

        /** Streams whose publisher is gone on the broker, handed over by [forget]: dropped from [publishers] on the next [publisher]. */
        private val forgotten = ConcurrentLinkedQueue<String>()

        /** The connection once opened, [GONE] once this node is given up (a connection opened after that is closed at once). */
        private val opened = AtomicReference<Any?>(null)

        @Volatile var dead = false

        /** The publisher of [stream] is gone on the broker (a metadata update, a publish error): declared again on next use. Never waits. */
        fun forget(stream: String) {
            forgotten.add(stream)
            leaders.remove(stream)
        }

        /** The publisher id of [stream] on this node, and the connection (opening it and declaring the publisher when missing). */
        @Synchronized
        fun publisher(stream: String): Pair<Client, Byte> {
            if (dead) throw EventStoreException.Failed("rabbitmq: the connection to ${key.first}:${key.second} was replaced")
            if (closed) throw EventStoreException.Failed("rabbitmq: the event store is closed")
            while (true) publishers.remove(forgotten.poll() ?: break)
            val client = opened.get() as? Client ?: open()
            publishers[stream]?.let { return client to it }
            if (nextId > MAX_PUBLISHER_ID) {
                // Publisher ids are one byte per connection: past the last one the connection is replaced, never wrapped.
                lost("publisher ids used up")
                throw EventStoreException.Failed("rabbitmq: publisher ids of ${key.first}:${key.second} used up; retrying on a new connection")
            }
            val id = (nextId++).toByte()
            val response = client.declarePublisher(id, null, stream)
            if (!response.isOk) {
                val why = "rabbitmq: declaring a publisher on $stream: ${RabbitConnector.codeName(response.responseCode)}"
                throw if (response.responseCode == Constants.RESPONSE_CODE_ACCESS_REFUSED) EventStoreException.Rejected(why) else EventStoreException.Failed(why)
            }
            publishers[stream] = id
            return client to id
        }

        private fun open(): Client {
            val client = connector.node(Client.Broker(key.first, key.second)) { p ->
                p.publishConfirmListener { _, id -> pending.remove(id)?.complete(Unit) }
                p.publishErrorListener { _, id, code -> pending.remove(id)?.completeExceptionally(PublishError(code)) }
                p.shutdownListener { context -> lost("the connection closed (${context.shutdownReason})") }
                p.metadataListener { stream, _ -> forget(stream) }
            }
            // Given up while it connected (the store closed, the node lost): this connection is never used.
            if (!opened.compareAndSet(null, client)) {
                runCatching { client.close() }
                throw EventStoreException.Failed("rabbitmq: the connection to ${key.first}:${key.second} was given up while it opened")
            }
            return client
        }

        /** This connection is given up: dropped from the nodes, its waiting publishes failed, the connection closed. Never waits. */
        fun lost(why: String) {
            dead = true
            nodes.remove(key, this)
            val failure = EventStoreException.Failed("rabbitmq: publishing through ${key.first}:${key.second}: $why")
            pending.keys.toList().forEach { pending.remove(it)?.completeExceptionally(failure) }
            (opened.getAndSet(GONE) as? Client)?.let { runCatching { it.close() } }
        }
    }

    /** Test hook: runs after a publish took its node's client, before it registers for the confirm. */
    @Volatile internal var beforeRegister: ((Node) -> Unit)? = null

    private class PublishError(val code: Short) : RuntimeException("the broker refused the message: ${RabbitConnector.codeName(code)}")

    suspend fun publish(event: OutgoingEvent) {
        if (event.source.isBlank()) throw EventStoreException.Invalid("the event has no source")
        if (closed) throw EventStoreException.Failed("rabbitmq: the event store is closed")
        val message = RabbitWire.message(event)
        val size = RabbitWire.codec.encode(message).size + FRAME_OVERHEAD
        if (size > MAX_FRAME) {
            throw EventStoreException.Rejected("the event ${event.id} is $size bytes on the wire, over the $MAX_FRAME-byte frame")
        }
        val stream = withContext(Dispatchers.IO) { partition(event) }
        val (node, client, publisher) = withContext(Dispatchers.IO) {
            val node = node(stream)
            try {
                val (client, publisher) = node.publisher(stream)
                Triple(node, client, publisher)
            } catch (e: EventStoreException.Failed) {
                leaders.remove(stream) // looked up again on the next try
                throw e
            }
        }
        val confirmed = CompletableFuture<Unit>()
        val id = node.sequence.incrementAndGet()
        beforeRegister?.invoke(node)
        node.pending[id] = confirmed
        // A lost() between taking the client and the line above failed nothing of this publish (it was not pending yet); lost()
        // marks the node dead before it fails the pending ones, so a dead node seen here is that case: fail now, not at the
        // confirm timeout.
        if (node.dead) {
            node.pending.remove(id)
            leaders.remove(stream)
            throw EventStoreException.Failed("rabbitmq: publishing ${event.id} to $stream: the connection to ${node.key.first}:${node.key.second} was replaced")
        }
        try {
            client.publish(publisher, listOf(message), ToLongFunction<Any> { id })
        } catch (e: Exception) {
            node.pending.remove(id)
            node.lost("publish failed: ${e.message}")
            throw failedOrRejected(event, stream, e)
        }
        val outcome = withTimeoutOrNull(confirmTimeout.toMillis()) { runCatching { confirmed.await() } }
        if (outcome == null) {
            node.pending.remove(id)
            // A connection that does not confirm within the bound is replaced: it may have stopped reading.
            node.lost("no confirm within $confirmTimeout")
            leaders.remove(stream)
            throw EventStoreException.Failed("rabbitmq: publishing ${event.id} to $stream: no confirm within $confirmTimeout")
        }
        outcome.exceptionOrNull()?.let {
            // A refused message too (e.g. the publisher no longer exists after its stream's leader moved): looked up and declared again.
            leaders.remove(stream)
            if (it is PublishError) node.forget(stream)
            throw failedOrRejected(event, stream, it)
        }
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

    /** The partition stream of [event] (its source's partitions cached). Blocking. */
    private fun partition(event: OutgoingEvent): String {
        val parts = layouts[event.source]?.takeIf { it.fresh() }?.value ?: lookup {
            RabbitConnector.layout(it, event.source) ?: throw EventStoreException.Failed(
                "rabbitmq: neither a super stream nor a stream named ${event.source} exists: create it " +
                    "(rabbitmq-streams add_super_stream ${event.source} --partitions 6)",
            )
        }.also { layouts[event.source] = Cached(it, System.nanoTime()) }
        return parts[RabbitWire.partitionIndex(RabbitWire.routingKey(event), parts.size)].stream
    }

    /** The node of [stream]'s leader (cached). Blocking. */
    private fun node(stream: String): Node {
        val key = leaders[stream]?.takeIf { it.fresh() }?.value
            ?: lookup { RabbitConnector.leader(it, stream).let { b -> b.host to b.port } }.also { leaders[stream] = Cached(it, System.nanoTime()) }
        while (true) {
            val node = nodes.computeIfAbsent(key) { Node(it) } // no network in here: the node connects on first use
            if (!node.dead) return node
            nodes.remove(key, node)
        }
    }

    /** [call] on the control connection (opened when missing, dropped when it fails). */
    private fun <T> lookup(call: (Client) -> T): T = synchronized(controlLock) {
        try {
            call(control?.takeIf { it.isOpen } ?: connector.seed().also { control = it })
        } catch (e: Exception) {
            control?.let { runCatching { it.close() } }
            control = null
            throw e as? EventStoreException ?: EventStoreException.Failed("rabbitmq: ${e.message}", e)
        }
    }

    override fun close() {
        closed = true
        nodes.values.toList().forEach { it.lost("the event store closed") }
        synchronized(controlLock) {
            control?.let { runCatching { it.close() } }
            control = null
        }
    }

    companion object {
        /** How long a source's partitions and a stream's leader are used before they are looked up again. */
        val LAYOUT_FOR: Duration = Duration.ofSeconds(30)

        /** The broker's default frame (1 MiB): a larger event is [EventStoreException.Rejected] before any connection. */
        const val MAX_FRAME = 1_048_576

        /** A publish frame's own bytes around one message: size, key, version, publisher id, count, publishing id, length. */
        const val FRAME_OVERHEAD = 4 + 2 + 2 + 1 + 4 + 8 + 4

        private const val MAX_PUBLISHER_ID = 255

        /** A node's connection slot once the node is given up. */
        private val GONE = Any()
    }
}
