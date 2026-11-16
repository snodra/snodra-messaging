package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/** The publisher's connections: listeners never wait for a node, nothing opens after a close, a refused publish is forgotten. */
class RabbitPublisherTest {
    private fun fresh(what: String) = "publisher.$what." + UUID.randomUUID().toString().replace("-", "").take(10)

    private fun event(source: String) = UUID.randomUUID().toString().let { id ->
        OutgoingEvent(source, id, "t", "k", mapOf("id" to id, "type" to "t"), "{}".toByteArray())
    }

    /** Every connection the publisher opens, in order (the first is the control connection); [gate] runs before a node's opens. */
    private class Opening(private val gate: (Int) -> Unit = {}) : RabbitConnector(RabbitBroker.settings(management = false), "kraken-test") {
        val clients = CopyOnWriteArrayList<Client>()
        val calls = AtomicInteger()

        override fun newClient(host: String, port: Int, parameters: Client.ClientParameters): Client {
            gate(calls.incrementAndGet())
            return super.newClient(host, port, parameters).also { clients += it }
        }
    }

    @Test
    fun `a metadata update never waits for the node's lock (a declare in flight on the same connection holds it)`() {
        val publisher = RabbitPublisher(RabbitConnector(RabbitEventStoreSettings(), "kraken-test"), Duration.ofSeconds(1))
        val node = publisher.Node("rabbitmq" to 5552)
        val holding = CountDownLatch(1)
        val release = CountDownLatch(1)
        val declaring = thread {
            synchronized(node) {
                holding.countDown()
                release.await(10, TimeUnit.SECONDS)
            }
        }
        holding.await()
        val started = System.nanoTime()
        node.forget("s") // what the client's metadata listener calls, on the client's own thread
        val tookMs = (System.nanoTime() - started) / 1_000_000
        release.countDown()
        declaring.join()
        assertTrue(tookMs < 1_000, "the listener returned at once, not after the declare: $tookMs ms")
    }

    @Test
    fun `a publish racing close opens no connection after the close`() = runBlocking {
        val source = fresh("close")
        RabbitBroker.stream(source)
        val opening = CountDownLatch(1)
        val closed = CountDownLatch(1)
        // The second connection is the node's: it is held until the store has closed.
        val connector = Opening { n ->
            if (n == 2) {
                opening.countDown()
                closed.await(30, TimeUnit.SECONDS)
            }
        }
        val publisher = RabbitPublisher(connector, Duration.ofSeconds(10))
        val publish = async(Dispatchers.IO) { runCatching { publisher.publish(event(source)) } }
        assertTrue(opening.await(30, TimeUnit.SECONDS), "the publish opens the node's connection")
        publisher.close()
        closed.countDown()
        val outcome = publish.await()
        assertInstanceOf(EventStoreException.Failed::class.java, outcome.exceptionOrNull(), "the publish fails: $outcome")
        assertEquals(2, connector.clients.size)
        assertFalse(connector.clients[1].isOpen, "the connection opened across the close is closed")
        val after = runCatching { publisher.publish(event(source)) }
        assertInstanceOf(EventStoreException.Failed::class.java, after.exceptionOrNull())
        assertEquals(2, connector.calls.get(), "no connection is opened after the close")
    }

    @Test
    fun `a publish the broker refuses drops the cached publisher, and the next publish declares it again`() = runBlocking {
        val source = fresh("refused")
        RabbitBroker.stream(source)
        val connector = Opening()
        RabbitPublisher(connector, Duration.ofSeconds(10)).use { publisher ->
            publisher.publish(event(source)) // declares publisher 0 on the node's connection
            // The broker forgets the publisher, as it does when the stream's leader moves.
            val node = connector.clients[1]
            assertTrue(node.deletePublisher(0.toByte()).isOk)
            val refused = runCatching { publisher.publish(event(source)) }
            assertInstanceOf(EventStoreException.Failed::class.java, refused.exceptionOrNull(), "publisher does not exist: $refused")
            publisher.publish(event(source))
        }
    }

    @Test
    fun `a connection lost between taking its client and registering the publish fails the publish at once`() = runBlocking {
        val source = fresh("lostrace")
        RabbitBroker.stream(source)
        RabbitPublisher(Opening(), Duration.ofSeconds(20)).use { publisher ->
            publisher.publish(event(source)) // the node's connection and its publisher exist
            publisher.beforeRegister = { node ->
                publisher.beforeRegister = null
                node.lost("lost in the race") // what the client's shutdown listener calls, on its own thread
            }
            val started = System.nanoTime()
            val outcome = runCatching { publisher.publish(event(source)) }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertInstanceOf(EventStoreException.Failed::class.java, outcome.exceptionOrNull(), "$outcome")
            assertTrue(tookMs < 5_000, "failed at once, not after the 20 s confirm timeout: $tookMs ms")
            publisher.publish(event(source)) // a new connection takes the next one
        }
    }
}
