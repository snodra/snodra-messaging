package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.impl.HashUtils
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.rabbitmq.Amqp.V
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.HexFormat

/** The layout on the wire shared with the Rust add-on: routing, the message's AMQP types, how a message reads back. */
class RabbitWireTest {
    private val headers = mapOf(
        "type" to "orders.Placed",
        "id" to "e-42",
        "correlationId" to "c-1",
        "timestamp" to "2026-11-14T10:00:00.000Z",
        "source" to "orders",
        "userId" to "",
        "traceparent" to "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "x-extra" to "kept",
    )

    private fun event(key: String?) = OutgoingEvent("kraken.events", "e-42", "orders.Placed", key, headers, """{"total":42}""".toByteArray())

    /**
     * The partition table the Rust add-on pins too (murmur3 x86_32, seed 104729, the hash as an unsigned number modulo 6
     * partitions); the hash is the Java stream client's own (its HashRoutingStrategy's), and a signed modulo is wrong for
     * `order-7`, whose hash has its top bit set.
     */
    @Test
    fun `keys hash to the partition the stream clients pick`() {
        for ((key, hash, partition) in listOf(
            Triple("order-1", 0x3163ba7a, 0),
            Triple("order-2", 0x4aa7bf62, 2),
            Triple("customer-123", 0x6fabe837, 5),
            Triple("order-7", 0x813d1adb.toInt(), 3),
            Triple("", 0xc6757d56.toInt(), 4),
        )) {
            assertEquals(hash, HashUtils.MURMUR3.applyAsInt(key), "the hash of \"$key\"")
            assertEquals(partition, RabbitWire.partitionIndex(key, 6), "the partition of \"$key\"")
        }
        assertEquals(104_729, RabbitWire.HASH_SEED)
        assertNotEquals(Math.abs(0x813d1adb.toInt() % 6), RabbitWire.partitionIndex("order-7", 6), "a signed modulo gives another partition")
    }

    @Test
    fun `an unkeyed event routes by its id`() {
        assertEquals("e-42", RabbitWire.routingKey(event(null)))
        assertEquals("k", RabbitWire.routingKey(event("k")))
    }

    /** What the Kotlin add-on writes, read section by section: the AMQP types of the layout. */
    @Test
    fun `the message has the types of the wire layout`() {
        val bytes = RabbitWire.encode(RabbitWire.message(event("order-42")))
        val properties = Amqp.section(bytes, Amqp.PROPERTIES).single() as V.Lst
        assertEquals(V.Str("e-42"), properties.items[0], "message-id: a string")
        assertEquals(V.Sym("application/json"), properties.items[6], "content-type: a symbol")
        assertEquals(
            listOf(V.MapV(listOf(V.Sym("x-routing-key") to V.Str("order-42")))),
            Amqp.section(bytes, Amqp.ANNOTATIONS),
            "the key: annotation x-routing-key (a symbol) with a string",
        )
        val application = Amqp.section(bytes, Amqp.APPLICATION_PROPERTIES).single() as V.MapV
        assertEquals(headers, application.entries.associate { (k, v) -> (k as V.Str).text to (v as V.Str).text }, "every header a string to a string")
        assertEquals(listOf(V.Bin("""{"total":42}""".toByteArray().toList())), Amqp.section(bytes, Amqp.DATA), "exactly one data section: the payload")
        assertTrue(Amqp.section(bytes, Amqp.AMQP_VALUE).isEmpty(), "no amqp-value")
    }

    @Test
    fun `an unkeyed event has no annotation`() {
        val bytes = RabbitWire.encode(RabbitWire.message(event(null)))
        assertTrue(Amqp.sections(bytes).none { it.first == Amqp.ANNOTATIONS })
    }

    /**
     * The bytes the Rust add-on writes for the same event (its `wire::message`, encoded by `rabbitmq-stream-protocol` 0.11.0;
     * the application properties come in its map's order), read by the Kotlin add-on; and both encodings have the same sections
     * of the same types.
     */
    @Test
    fun `what the Rust add-on writes reads back as the event, with the types the Kotlin add-on writes`() {
        for ((key, hex) in listOf("order-42" to RUST_KEYED, null to RUST_UNKEYED)) {
            val rust = HexFormat.of().parseHex(hex)
            val read = RabbitWire.delivery("3", 12, 1_700_000_000_000, RabbitWire.codec.decode(Unpooled.wrappedBuffer(rust), rust.size))
            assertEquals(key, read.key)
            assertEquals(headers, read.headers)
            assertEquals("""{"total":42}""", String(read.payload))
            assertEquals("3" to "12", read.segment to read.position)
            assertEquals(1_700_000_000_000, read.timestampMs)

            val kotlin = RabbitWire.encode(RabbitWire.message(event(key)))
            assertEquals(shape(rust), shape(kotlin), "the sections and their types")
        }
    }

    @Test
    fun `a header of another AMQP type reads as text, a message without a data section as an empty payload`() {
        val message = RabbitWire.codec.messageBuilder()
            .applicationProperties().entrySymbol("sym", "s").entry("bin", "b".toByteArray()).entry("n", 7L).messageBuilder()
            .messageAnnotations().entrySymbol(RabbitWire.KEY_ANNOTATION, "k").messageBuilder()
            .build()
        val bytes = RabbitWire.encode(message)
        val read = RabbitWire.delivery("0", 0, 1, RabbitWire.codec.decode(Unpooled.wrappedBuffer(bytes), bytes.size))
        assertEquals(mapOf("sym" to "s", "bin" to "b", "n" to "7"), read.headers)
        assertEquals("k", read.key)
        assertEquals(0, read.payload.size)
        val unkeyed = RabbitWire.encode(RabbitWire.codec.messageBuilder().addData(byteArrayOf()).build())
        assertNull(RabbitWire.delivery("0", 0, 1, RabbitWire.codec.decode(Unpooled.wrappedBuffer(unkeyed), unkeyed.size)).key)
    }

    @Test
    fun `segments are partition suffixes`() {
        assertEquals("0", RabbitWire.segmentOf("kraken.events", "kraken.events-0"))
        assertEquals("11", RabbitWire.segmentOf("kraken.events", "kraken.events-11"))
        assertEquals("eu", RabbitWire.segmentOf("kraken.events", "kraken.events-eu"))
        assertEquals("other-3", RabbitWire.segmentOf("kraken.events", "other-3"))
        assertEquals("kraken.events-", RabbitWire.segmentOf("kraken.events", "kraken.events-"))
    }

    /** The section codes in order of appearance, each with the types the layout fixes (strings, symbols, maps' entries). */
    private fun shape(bytes: ByteArray): List<String> = Amqp.sections(bytes).map { (code, v) ->
        val types = when (v) {
            is V.Lst -> "list[0]=${type(v.items[0])} list[6]=${type(v.items[6])}"
            is V.MapV -> v.entries.map { (k, x) -> "${type(k)}:${type(x)}" }.toSet().joinToString()
            else -> type(v)
        }
        "0x%02x %s".format(code, types)
    }.sorted()

    private fun type(v: V) = v::class.simpleName

    private companion object {
        // Encoded by the Rust add-on's `wire::message` (commit 99c3698 of kraken-messaging-rs) for the event above, publishing id 1.
        const val RUST_KEYED =
            "005372c11a02a30d782d726f7574696e672d6b6579a1086f726465722d3432005373c0240da104652d34324040404040a3106170706c69636174696f6e2f6a736f6e404040404040005374c1c810a106757365724964a100a10b7472616365706172656e74a13730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031a107782d6578747261a1046b657074a10d636f7272656c6174696f6e4964a103632d31a1026964a104652d3432a10474797065a10d6f72646572732e506c61636564a106736f75726365a1066f7264657273a10974696d657374616d70a118323032362d31312d31345431303a30303a30302e3030305a005375a00c7b22746f74616c223a34327d"
        const val RUST_UNKEYED =
            "005373c0240da104652d34324040404040a3106170706c69636174696f6e2f6a736f6e404040404040005374c1c810a10974696d657374616d70a118323032362d31312d31345431303a30303a30302e3030305aa1026964a104652d3432a106736f75726365a1066f7264657273a10b7472616365706172656e74a13730302d30616637363531393136636434336464383434386562323131633830333139632d623761643662373136393230333333312d3031a107782d6578747261a1046b657074a10474797065a10d6f72646572732e506c61636564a106757365724964a100a10d636f7272656c6174696f6e4964a103632d31005375a00c7b22746f74616c223a34327d"
    }
}
