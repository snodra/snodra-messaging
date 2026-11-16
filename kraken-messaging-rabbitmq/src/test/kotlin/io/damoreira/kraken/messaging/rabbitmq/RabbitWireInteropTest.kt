package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Codec
import com.rabbitmq.stream.Constants
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.MessageBuilder
import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.codec.ByteArrayEncodedMessage
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.rabbitmq.Amqp.V
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.HexFormat
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Interoperability at the wire level, on a real broker ([RabbitBroker]), with plain clients of the Java stream library standing in
 * for any other program on the streams (the Rust add-on is checked against the same rules by its own tests; a cross-language run
 * comes later):
 *
 * - what the Kotlin add-on writes, read back raw by a plain client, follows every wire rule (partition, AMQP types, one message
 *   per entry), and lands where the client's own hash routing puts the same keys;
 * - the exact bytes the Rust add-on encodes, written by a plain producer where the Rust add-on routes them, read by the Kotlin add-on;
 * - offsets: what a plain client stores the Rust way (the next offset to read) is where the Kotlin subscription resumes, and what
 *   the Kotlin subscription stores reads back, to a plain client, as the next offset to read.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RabbitWireInteropTest {
    private val store = RabbitEventStore(RabbitBroker.settings(management = false))
    private val headers = mapOf(
        "type" to "orders.Placed", "id" to "", "correlationId" to "c-1", "timestamp" to "2026-11-14T10:00:00.000Z",
        "source" to "orders", "userId" to "", "traceparent" to "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    )

    @AfterAll
    fun close() = store.close()

    private fun fresh(what: String) = "interop.$what." + UUID.randomUUID().toString().replace("-", "").take(10)

    private fun event(source: String, key: String?, id: String = UUID.randomUUID().toString()) =
        OutgoingEvent(source, id, "orders.Placed", key, headers + ("id" to id) + ("x-extra" to "kept"), """{"id":"$id"}""".toByteArray())

    /** One stored message as a plain client reads it: where, its raw bytes, and its chunk's entries and records. */
    private class Stored(val stream: String, val offset: Long, val raw: ByteArray, val entries: Int, val records: Int)

    /** Every message of [streams], read by a plain client with a codec that keeps the raw bytes and a look at each chunk's header. */
    private fun readRaw(streams: List<String>, expected: Int): List<Stored> {
        val chunks = HashMap<Long, Pair<Int, Int>>() // first offset of a chunk -> (entries, records), per subscription below
        val all = ConcurrentLinkedQueue<Stored>()
        val done = CountDownLatch(expected)
        var current = ""
        val raw = object : Codec {
            val inner = RabbitWire.codec
            override fun encode(message: Message) = inner.encode(message)
            override fun messageBuilder(): MessageBuilder = inner.messageBuilder()
            override fun decode(buf: ByteBuf, length: Int): Message {
                val bytes = ByteArray(length).also { buf.getBytes(buf.readerIndex(), it) }
                return RawMessage(inner.decode(buf, length), bytes)
            }
        }
        val client = RabbitBroker.client(
            Client.ClientParameters().codec(raw)
                .channelCustomizer { channel -> channel.pipeline().addBefore("StreamHandler", "kraken-chunk-headers", ChunkHeaders(chunks)) }
                .chunkListener { c, id, offset, _, _ -> c.credit(id, 1); offset }
                .messageListener { _, offset, _, _, chunk, message ->
                    val header = synchronized(chunks) { chunks[chunk as Long] } ?: (-1 to -1)
                    all += Stored(current, offset, (message as RawMessage).raw, header.first, header.second)
                    done.countDown()
                },
        )
        try {
            for ((i, stream) in streams.withIndex()) {
                current = stream
                val before = all.size
                assertTrue(client.subscribe(i.toByte(), stream, OffsetSpecification.first(), 1).isOk)
                // Until this stream gives nothing more for a moment.
                var seen = -1
                while (seen != all.size) {
                    seen = all.size
                    Thread.sleep(500)
                }
                client.unsubscribe(i.toByte())
                assertTrue(all.size >= before)
            }
            assertTrue(done.await(10, TimeUnit.SECONDS), "every message read: ${all.size} of $expected")
        } finally {
            client.close()
        }
        return all.toList()
    }

    private class RawMessage(inner: Message, val raw: ByteArray) : Message by inner

    /** Reads each deliver frame's chunk header (entries, records) before the client decodes it. */
    private class ChunkHeaders(val chunks: HashMap<Long, Pair<Int, Int>>) : ChannelInboundHandlerAdapter() {
        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            val b = msg as ByteBuf
            val at = b.readerIndex()
            if ((b.getShort(at).toInt() and 0x7FFF) == Constants.COMMAND_DELIVER.toInt()) {
                val version = b.getShort(at + 2).toInt()
                val chunk = at + 4 + 1 + if (version >= 2) 8 else 0 // key, version, subscription id, [committed chunk id]
                // magic and version (1), chunk type (1), entries (2), records (4), timestamp (8), epoch (8), first offset (8)
                val entries = b.getUnsignedShort(chunk + 2)
                val records = b.getInt(chunk + 4)
                val first = b.getLong(chunk + 8 + 8 + 8)
                synchronized(chunks) { chunks[first] = entries to records }
            }
            ctx.fireChannelRead(msg)
        }
    }

    @Test
    fun `what the Kotlin add-on writes follows every wire rule, read by a plain client`() = runBlocking {
        val source = fresh("rules")
        RabbitBroker.superStream(source, 6)
        val keyed = mapOf("order-1" to 0, "order-2" to 2, "customer-123" to 5, "order-7" to 3, "" to 4)
        val events = keyed.keys.map { event(source, it) } + event(source, null, id = "e-unkeyed-1")
        for (e in events) store.publish(e)

        // The client's own super stream producer (its default hash routing) puts the same keys on the same partitions.
        RabbitBroker.environment().use { env ->
            val producer = env.producerBuilder().superStream(source).routing { m -> m.applicationProperties["key"] as String }.producerBuilder().build()
            val confirmed = CountDownLatch(keyed.size)
            for (key in keyed.keys) {
                producer.send(producer.messageBuilder().applicationProperties().entry("key", key).messageBuilder().addData(byteArrayOf()).build()) {
                    if (it.isConfirmed) confirmed.countDown()
                }
            }
            assertTrue(confirmed.await(10, TimeUnit.SECONDS))
            producer.close()
        }

        val stored = readRaw((0 until 6).map { "$source-$it" }, events.size + keyed.size)
        for ((key, partition) in keyed) {
            val mine = stored.filter { s -> Amqp.section(s.raw, Amqp.ANNOTATIONS).any { it == V.MapV(listOf(V.Sym("x-routing-key") to V.Str(key))) } }
            assertEquals(listOf("$source-$partition"), mine.map { it.stream }, "the event keyed \"$key\": its partition")
            val theirs = stored.filter { s -> (Amqp.section(s.raw, Amqp.APPLICATION_PROPERTIES).singleOrNull() as? V.MapV)?.entries == listOf(V.Str("key") to V.Str(key)) }
            assertEquals(listOf("$source-$partition"), theirs.map { it.stream }, "the client's own hash routing of \"$key\"")
        }
        val unkeyed = stored.single { s -> (Amqp.section(s.raw, Amqp.PROPERTIES).singleOrNull() as? V.Lst)?.items?.first() == V.Str("e-unkeyed-1") }
        assertEquals("$source-${RabbitWire.partitionIndex("e-unkeyed-1", 6)}", unkeyed.stream, "an unkeyed event is routed by its id")
        assertTrue(Amqp.sections(unkeyed.raw).none { it.first == Amqp.ANNOTATIONS }, "no annotation without a key")

        for (e in events) {
            val s = stored.single { s -> (Amqp.section(s.raw, Amqp.PROPERTIES).singleOrNull() as? V.Lst)?.items?.first() == V.Str(e.id) }
            val properties = Amqp.section(s.raw, Amqp.PROPERTIES).single() as V.Lst
            assertEquals(V.Sym("application/json"), properties.items[6], "content-type")
            val application = Amqp.section(s.raw, Amqp.APPLICATION_PROPERTIES).single() as V.MapV
            assertEquals(e.headers, application.entries.associate { (k, v) -> (k as V.Str).text to (v as V.Str).text }, "headers: strings")
            assertEquals(listOf(V.Bin(e.payload.toList())), Amqp.section(s.raw, Amqp.DATA), "one data section")
            assertTrue(Amqp.section(s.raw, Amqp.AMQP_VALUE).isEmpty())
            assertEquals(s.entries, s.records, "one message per entry: no sub-entry batch (${s.entries} entries, ${s.records} records)")
            assertTrue(s.entries >= 1)
        }
    }

    @Test
    fun `the bytes the Rust add-on writes, where it routes them, read by the Kotlin add-on`() = runBlocking {
        val source = fresh("rust")
        RabbitBroker.superStream(source, 6)
        val passThrough = object : Codec {
            override fun encode(message: Message): Codec.EncodedMessage {
                val bytes = message.bodyAsBinary // the encoded message, carried as the body of a stand-in
                return ByteArrayEncodedMessage(bytes.size, bytes)
            }
            override fun decode(buf: ByteBuf, length: Int): Message = RabbitWire.codec.decode(buf, length)
            override fun messageBuilder(): MessageBuilder = RabbitWire.codec.messageBuilder()
        }
        val confirmed = CountDownLatch(2)
        val client = RabbitBroker.client(Client.ClientParameters().codec(passThrough).publishConfirmListener { _, _ -> confirmed.countDown() })
        // The Rust add-on routes "order-42" by its key, the unkeyed event by its id "e-42".
        val keyedPartition = RabbitWire.partitionIndex("order-42", 6)
        val unkeyedPartition = RabbitWire.partitionIndex("e-42", 6)
        for ((i, pair) in listOf(keyedPartition to RUST_KEYED, unkeyedPartition to RUST_UNKEYED).withIndex()) {
            val (partition, hex) = pair
            assertTrue(client.declarePublisher(i.toByte(), null, "$source-$partition").isOk)
            val stand = RabbitWire.codec.messageBuilder().addData(HexFormat.of().parseHex(hex)).build()
            client.publish(i.toByte(), listOf(stand))
        }
        assertTrue(confirmed.await(10, TimeUnit.SECONDS))
        client.close()

        val sub = store.subscribe(SubscriptionSpec("svc.rust-bytes", source))
        val got = deliveries(sub, 2)
        sub.close()
        val keyed = got.single { it.key == "order-42" }
        assertEquals(keyedPartition.toString(), keyed.segment)
        val unkeyed = got.single { it.key == null }
        assertEquals(unkeyedPartition.toString(), unkeyed.segment)
        for (d in got) {
            assertEquals(RUST_HEADERS, d.headers)
            assertEquals("""{"total":42}""", String(d.payload))
            assertTrue(d.timestampMs > 0)
        }
    }

    @Test
    fun `the stored value is the next offset to read, both ways`() = runBlocking {
        val source = fresh("offsets") // a plain stream: one segment, 0
        RabbitBroker.stream(source)
        for (i in 0 until 6) store.publish(event(source, "k"))

        // A plain client stores the Rust way: 3 is the next offset to read.
        RabbitBroker.client().use { it.storeOffset("svc.resume", source, 3) }
        awaitStored("svc.resume", source, 3)
        val resumed = store.subscribe(SubscriptionSpec("svc.resume", source))
        assertEquals("3", deliveries(resumed, 1).single().position, "resumes exactly at the stored offset")
        resumed.close()

        // Stored 0 (a reset to the first offset): from 0, which the Java client's "last processed" convention cannot say.
        RabbitBroker.client().use { it.storeOffset("svc.zero", source, 0) }
        awaitStored("svc.zero", source, 0)
        val zero = store.subscribe(SubscriptionSpec("svc.zero", source))
        assertEquals("0", deliveries(zero, 1).single().position)
        zero.close()

        // The Kotlin subscription acknowledges offset 4 and closes: a plain client reads 5.
        val acking = store.subscribe(SubscriptionSpec("svc.acks", source))
        val five = deliveries(acking, 5)
        assertEquals((0 until 5).map(Int::toString), five.map { it.position })
        acking.ack("0", "4")
        acking.close()
        assertEquals(5L, RabbitBroker.client().use { it.queryOffset("svc.acks", source).offset }, "stored: the acknowledged offset + 1")

        // Nothing stored: from the first offset.
        val fresh = store.subscribe(SubscriptionSpec("svc.fresh", source))
        assertEquals("0", deliveries(fresh, 1).single().position)
        fresh.close()
    }

    private fun awaitStored(reference: String, stream: String, offset: Long) = RabbitBroker.client().use { c ->
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (c.queryOffset(reference, stream).let { !(it.isOk && it.offset == offset) }) {
            check(System.nanoTime() < deadline) { "the offset $offset of $reference was not stored" }
            Thread.sleep(50)
        }
    }

    /** The first [n] deliveries of [subscription] (within 30 s each). */
    private suspend fun deliveries(subscription: Subscription, n: Int): List<Delivery> {
        val got = mutableListOf<Delivery>()
        while (got.size < n) {
            when (val e = withTimeoutOrNull(30_000) { subscription.next() } ?: error("only ${got.size} of $n deliveries")) {
                is Delivery -> got += e
                is SubscriptionEvent.Assigned, is SubscriptionEvent.Revoked -> {}
            }
        }
        return got
    }

    private companion object {
        val RUST_HEADERS = mapOf(
            "type" to "orders.Placed", "id" to "e-42", "correlationId" to "c-1", "timestamp" to "2026-11-14T10:00:00.000Z",
            "source" to "orders", "userId" to "", "traceparent" to "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "x-extra" to "kept",
        )

        // The Rust add-on's encoding of that event, keyed "order-42" and unkeyed (as in RabbitWireTest).
        const val RUST_KEYED =
            "005372c11a02a30d782d726f7574696e672d6b6579a1086f726465722d3432005373c0240da104652d34324040404040a3106170706c69636174696f6e2f6a736f6e404040404040005374c1c810a106757365724964a100a10b7472616365706172656e74a13730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031a107782d6578747261a1046b657074a10d636f7272656c6174696f6e4964a103632d31a1026964a104652d3432a10474797065a10d6f72646572732e506c61636564a106736f75726365a1066f7264657273a10974696d657374616d70a118323032362d31312d31345431303a30303a30302e3030305a005375a00c7b22746f74616c223a34327d"
        const val RUST_UNKEYED =
            "005373c0240da104652d34324040404040a3106170706c69636174696f6e2f6a736f6e404040404040005374c1c810a10974696d657374616d70a118323032362d31312d31345431303a30303a30302e3030305aa1026964a104652d3432a106736f75726365a1066f7264657273a10b7472616365706172656e74a13730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031a107782d6578747261a1046b657074a10474797065a10d6f72646572732e506c61636564a106757365724964a100a10d636f7272656c6174696f6e4964a103632d31005375a00c7b22746f74616c223a34327d"
    }
}
