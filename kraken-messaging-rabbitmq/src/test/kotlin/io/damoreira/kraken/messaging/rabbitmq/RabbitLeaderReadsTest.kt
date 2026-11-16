package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Codec
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.MessageBuilder
import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.codec.ByteArrayEncodedMessage
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.netty.buffer.ByteBuf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * On a real broker ([RabbitBroker]): the reads that decide correctness go to each stream's leader, an event-less partition does not
 * break resets, an undecodable message ends the subscription instead of leaving a gap, and the node a connection landed on reads.
 */
class RabbitLeaderReadsTest {
    private fun fresh(what: String) = "leader.$what." + UUID.randomUUID().toString().replace("-", "").take(10)

    private fun event(source: String, key: String) = UUID.randomUUID().toString().let { id ->
        OutgoingEvent(source, id, "t", key, mapOf("id" to id, "type" to "t"), "{}".toByteArray())
    }

    /**
     * Records which node each call goes to: the store reaches the broker through `localhost`, the broker advertises every stream's
     * leader as `127.0.0.1`, so the two names tell the control connection from the leader's.
     */
    private class Recording(settings: RabbitEventStoreSettings) : RabbitConnector(settings, "kraken-test") {
        val calls = ConcurrentLinkedQueue<Pair<String, String>>()

        override fun newClient(host: String, port: Int, parameters: Client.ClientParameters): Client {
            val node = "$host:$port"
            fun note(call: String) = calls.add(node to call)
            return object : Client(parameters) {
                override fun queryOffset(reference: String, stream: String) = super.queryOffset(reference, stream).also { note("queryOffset") }
                override fun streamStats(stream: String) = super.streamStats(stream).also { note("streamStats") }
                override fun storeOffset(reference: String, stream: String, offset: Long) = super.storeOffset(reference, stream, offset).also { note("storeOffset") }
                override fun subscribe(id: Byte, stream: String, spec: OffsetSpecification, credit: Int) = super.subscribe(id, stream, spec, credit).also { note("read") }
                override fun subscribe(id: Byte, stream: String, spec: OffsetSpecification, credit: Int, properties: MutableMap<String, String>) =
                    super.subscribe(id, stream, spec, credit, properties).also { note("consume") }
                override fun metadata(vararg streams: String) = super.metadata(*streams).also { note("metadata") }
                override fun partitions(superStream: String) = super.partitions(superStream).also { note("partitions") }
            }
        }
    }

    @Test
    fun `a read by time that fails leaves the leader's connection ready for the next read`() = runBlocking {
        val stream = fresh("bytime")
        RabbitBroker.stream(stream)
        RabbitEventStore(RabbitBroker.settings(management = false)).use { store -> repeat(3) { store.publish(event(stream, "k")) } }
        RabbitReads(RabbitConnector(RabbitBroker.settings(), "kraken-test")).use { reads ->
            val bounds = reads.bounds(stream)
            assertEquals(3, bounds.head)
            // A time no chunk reaches, with bounds that say one does: the read gets nothing and fails...
            val future = Bounds(0, Long.MAX_VALUE, lastTimestampMs = Long.MAX_VALUE)
            assertThrows(EventStoreException.Failed::class.java) { reads.offsetAt(stream, Long.MAX_VALUE - 1, future, Duration.ofMillis(500)) }
            // ...and leaves no subscription behind on the connection: the next read on it works.
            assertEquals(3, reads.bounds(stream).head)
        }
    }

    @Test
    fun `offsets, statistics and chunk reads go to each stream's leader, never to the host the store reached first`() = runBlocking {
        val source = fresh("routing")
        RabbitBroker.superStream(source, 2)
        val settings = RabbitBroker.settings().copy(hosts = listOf("localhost:${RabbitBroker.streamPort}"))
        val leader = "127.0.0.1:${RabbitBroker.streamPort}"
        val connector = Recording(settings)
        RabbitEventStore(RabbitBroker.settings(management = false)).use { store -> repeat(10) { store.publish(event(source, "k-$it")) } }
        val spec = SubscriptionSpec("svc.routing", source)
        val sub = RabbitSubscription.join(spec, connector, "m-1", settings) {}
        var got = 0
        while (got < 10) {
            val e = withTimeoutOrNull(30_000) { sub.next() } ?: error("only $got deliveries")
            if (e is Delivery) {
                got++
                sub.ack(e.segment, e.position)
            }
        }
        // A status refresh reads the ends.
        withTimeoutOrNull(15_000) { while (sub.segments().any { it.head == null }) kotlinx.coroutines.delay(100) }
        sub.close()
        val management = RabbitManagement(settings.management!!, settings.username, settings.password, settings.virtualHost)
        val admin = RabbitSubscriptionAdmin(spec, connector, management, RabbitLive())
        admin.apply(admin.plan(ResetTarget.Earliest))
        admin.plan(ResetTarget.Timestamp(System.currentTimeMillis() - 60_000))

        val calls = connector.calls.toList()
        for (decisive in listOf("queryOffset", "streamStats", "storeOffset", "read", "consume")) {
            val nodes = calls.filter { it.second == decisive }.map { it.first }.toSet()
            assertEquals(setOf(leader), nodes, "$decisive on the leader only: $calls")
        }
        assertTrue(calls.any { it.first != leader && it.second == "partitions" }, "the layout from the host reached first: $calls")
    }

    @Test
    fun `a partition holding only offset-tracking entries is empty of events, resets go on`() = runBlocking {
        val source = fresh("eventless")
        RabbitBroker.superStream(source, 3)
        RabbitEventStore.open(RabbitBroker.settings()).use { store ->
            // One key: one partition gets the events, the two others none.
            repeat(3) { store.publish(event(source, "only-key")) }
            val admin = store.admin(SubscriptionSpec("svc.eventless", source))
            admin.apply(admin.plan(ResetTarget.Earliest)) // stores 0 on the empty partitions: tracking entries, no event
            val again = admin.plan(ResetTarget.Earliest)
            assertEquals(3, again.size, "a plan after the reset: $again")
            admin.apply(again)
            assertEquals(3, admin.plan(ResetTarget.Timestamp(System.currentTimeMillis() - 60_000)).size)
            // Nothing added since: the event-less partitions are not read again (no quiet wait per partition per call).
            val started = System.nanoTime()
            assertEquals(3, admin.plan(ResetTarget.Latest).size)
            assertTrue((System.nanoTime() - started) / 1_000_000 < 3_000, "no wait for the event-less partitions known as such")
        }
    }

    @Test
    fun `a message the client cannot decode ends the subscription, never a gap acknowledged past`() = runBlocking {
        val stream = fresh("gap")
        RabbitBroker.stream(stream)
        val events = (0 until 4).map { event(stream, "k") }
        val bytes = events.map { RabbitWire.encode(RabbitWire.message(it)) }
        // A data section whose type code no AMQP decoder knows.
        val garbage = byteArrayOf(0x00, 0x53, 0x75, 0x99.toByte(), 0x01)
        val passThrough = object : Codec {
            override fun encode(message: Message): Codec.EncodedMessage = message.bodyAsBinary.let { ByteArrayEncodedMessage(it.size, it) }
            override fun decode(buf: ByteBuf, length: Int): Message = RabbitWire.codec.decode(buf, length)
            override fun messageBuilder(): MessageBuilder = RabbitWire.codec.messageBuilder()
        }
        val confirms = CountDownLatch(5)
        RabbitBroker.client(Client.ClientParameters().codec(passThrough).publishConfirmListener { _, _ -> confirms.countDown() }).use { c ->
            assertTrue(c.declarePublisher(0, null, stream).isOk)
            fun stand(b: ByteArray) = RabbitWire.codec.messageBuilder().addData(b).build()
            // One chunk: an event, the undecodable message, an event; then a chunk of two more events.
            c.publish(0, listOf(stand(bytes[0]), stand(garbage), stand(bytes[1])))
            Thread.sleep(500)
            c.publish(0, listOf(stand(bytes[2]), stand(bytes[3])))
            assertTrue(confirms.await(10, TimeUnit.SECONDS))
        }
        RabbitEventStore(RabbitBroker.settings(management = false)).use { store ->
            val sub = store.subscribe(SubscriptionSpec("svc.gap", stream))
            val delivered = mutableListOf<String>()
            val failure = runCatching {
                while (true) {
                    val e = withTimeoutOrNull(20_000) { sub.next() } ?: break
                    if (e is Delivery) {
                        delivered += e.headers["id"]!!
                        sub.ack(e.segment, e.position)
                    }
                }
            }.exceptionOrNull()
            sub.close()
            assertInstanceOf(EventStoreException.Failed::class.java, failure, "the subscription ends: $failure (delivered $delivered)")
            assertTrue("gap" in failure!!.message!! || "delivered" in failure.message!!, failure.message)
            assertFalse(events[2].id in delivered || events[3].id in delivered, "nothing past the gap: $delivered")
            val stored = RabbitBroker.client().use { it.queryOffset("svc.gap", stream) }
            assertTrue(!stored.isOk || stored.offset <= 1, "never stored past the gap: ${stored.offset}")
        }
    }

    @Test
    fun `the node a connection landed on reads as the broker advertises it`() {
        val connector = RabbitConnector(RabbitBroker.settings(), "kraken-test")
        val client = connector.seed()
        try {
            assertEquals("127.0.0.1" to RabbitBroker.streamPort, RabbitConnector.advertised(client))
        } finally {
            client.close()
        }
        // Through the load-balancer mode: the connection that lands on the leader is the one kept.
        val lb = RabbitConnector(RabbitBroker.settings().copy(loadBalancer = true), "kraken-test")
        lb.node(Client.Broker("127.0.0.1", RabbitBroker.streamPort)).close()
    }
}
