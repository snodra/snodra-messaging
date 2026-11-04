package io.damoreira.kraken.messaging.deadletter

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.bson.BsonDocument
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Query
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.Optional
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The storage contract of `dead_letters` (ADR 0022): the documents this library writes are exactly the hand-written
 * fixtures in `src/test/resources/dead_letters_contract` (written from the ADR's table; the Rust library checks its own
 * fixtures from the same table), types included (Int32 `partition`, Int64 `offset`, BinData, dates), with no `_class`
 * and no null; a fixture document reads back into a [DeadLetter]; the indexes have the ADR's names.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadLetterContractTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private val now = Instant.parse("2026-11-04T10:00:00Z")
    private lateinit var store: DeadLetterStore

    @BeforeAll fun start() {
        mongoContainer.start()
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "dead_letter_contract"))
        store = DeadLetterStore(mongo, Duration.ofDays(30), Clock.fixed(now, ZoneOffset.UTC))
    }

    @AfterAll fun stop() = mongoContainer.stop()

    @BeforeEach fun clean() { mongo.remove(Query(), DeadLetterStore.COLLECTION) }

    private fun fixture(name: String): BsonDocument =
        BsonDocument.parse(javaClass.getResource("/dead_letters_contract/$name.json")!!.readText())

    private fun stored(id: String): BsonDocument =
        mongo.db.getCollection(DeadLetterStore.COLLECTION, BsonDocument::class.java).find(Document("_id", id)).first()!!

    private fun record(partition: Int, offset: Long, key: String?, value: ByteArray?, vararg headers: Pair<String, ByteArray?>) =
        ConsumerRecord("kraken.events", partition, offset, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
            -1, -1, key, value, RecordHeaders(headers.map { RecordHeader(it.first, it.second) }), Optional.empty())

    @Test fun `a HANDLER_FAILED letter is the fixture`() {
        val r = record(2, 41, "order-7", """{"orderId":"order-7","amount":12.50}""".toByteArray(),
            "type" to "examples.OrderPlaced".toByteArray(), "id" to "e-41".toByteArray(), "correlationId" to "c-0".toByteArray(),
            "correlationId" to "c-41".toByteArray()) // a repeated header keeps its last value
        val letter = store.letter("orders", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("IllegalStateException", "boom"), attempts = 5)
        assertTrue(store.park(letter))
        assertEquals(fixture("handler_failed"), stored("orders:kraken.events:2:41"))
    }

    @Test fun `a SEQUENCED letter is the fixture`() {
        val r = record(2, 42, "order-7", """{"orderId":"order-7"}""".toByteArray(),
            "type" to "examples.OrderShipped".toByteArray(), "id" to "e-42".toByteArray())
        val letter = store.sequenced("default", r, headId = "default:kraken.events:2:41")
        assertTrue(store.park(letter))
        assertEquals(fixture("sequenced"), stored("default:kraken.events:2:42"))
    }

    @Test fun `INVALID letters with a binary or an empty value and no key are the fixtures`() {
        val binary = record(0, 7, null, byteArrayOf(0xFF.toByte(), 0, 1, 2), "source" to "legacy".toByteArray() + 0xFF.toByte())
        assertTrue(store.park(store.letter("orders", binary, DeadLetterReason.INVALID, DeadLetterError.invalid("record without type header"), attempts = 0)))
        assertEquals(fixture("invalid_binary"), stored("orders:kraken.events:0:7"))

        val empty = record(1, 3, "k-3", ByteArray(0), "type" to "examples.OrderPlaced".toByteArray())
        assertTrue(store.park(store.letter("default", empty, DeadLetterReason.INVALID, DeadLetterError.invalid("payload is not a JSON object"), attempts = 0)))
        assertEquals(fixture("invalid_empty"), stored("default:kraken.events:1:3"))
    }

    @Test fun `parking is idempotent - the same record parks once`() {
        val r = record(2, 41, "order-7", "{}".toByteArray(), "type" to "t".toByteArray())
        assertTrue(store.park(store.letter("orders", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("X", "first"), attempts = 1)))
        assertFalse(store.park(store.letter("orders", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("X", "second"), attempts = 1)))
        assertEquals("first", stored("orders:kraken.events:2:41").getDocument("error").getString("message").value)
    }

    @Test fun `every fixture reads back as a DeadLetter`() {
        val coll = mongo.db.getCollection(DeadLetterStore.COLLECTION, BsonDocument::class.java)
        for (name in listOf("handler_failed", "sequenced", "invalid_binary", "invalid_empty")) coll.insertOne(fixture(name))

        val failed = store.get("orders:kraken.events:2:41")!!
        assertEquals(DeadLetter(
            id = "orders:kraken.events:2:41", processor = "orders", topic = "kraken.events", partition = 2, offset = 41, key = "order-7",
            eventId = "e-41", eventType = "examples.OrderPlaced",
            headers = mapOf("type" to "examples.OrderPlaced", "id" to "e-41", "correlationId" to "c-41"),
            payload = """{"orderId":"order-7","amount":12.50}""", payloadBinary = null, reason = DeadLetterReason.HANDLER_FAILED,
            error = DeadLetterError("IllegalStateException", "boom"), blockedBy = null, attempts = 5,
            parkedAt = now, lastAttemptAt = now, expireAt = Instant.parse("2026-12-04T10:00:00Z"),
        ), failed)
        assertEquals("default:kraken.events:2:41", store.get("default:kraken.events:2:42")!!.blockedBy)
        val binary = store.get("orders:kraken.events:0:7")!!
        assertEquals(listOf(0xFF.toByte(), 0, 1, 2), binary.payloadBinary!!.toList())
        assertNull(binary.key)
        assertNull(binary.eventId)
        assertEquals(listOf(0xFF.toByte(), 0, 1, 2), binary.value().toList())
        assertEquals(0, store.get("default:kraken.events:1:3")!!.value().size)
        assertNull(store.get("missing"))
    }

    @Test fun `the indexes have the ADR's names, keys and TTL`() {
        store.ensureIndexes()
        store.ensureIndexes() // idempotent
        val indexes = mongo.db.getCollection(DeadLetterStore.COLLECTION).listIndexes().associateBy { it.getString("name") }
        assertEquals(Document(mapOf("processor" to 1, "key" to 1, "offset" to 1)), indexes.getValue("dead_letters_processor_key_offset")["key"])
        assertEquals(Document(mapOf("processor" to 1, "parkedAt" to 1)), indexes.getValue("dead_letters_processor_parked")["key"])
        val ttl = indexes.getValue("dead_letters_expire")
        assertEquals(Document("expireAt", 1), ttl["key"])
        assertEquals(0L, (ttl["expireAfterSeconds"] as Number).toLong())
    }

    @Test fun `an error message is cut at 2000 code points without splitting a pair`() {
        val long = "x".repeat(1999) + "😀" + "tail"
        val cut = DeadLetterError("E", long).message
        assertEquals(2000, cut.codePointCount(0, cut.length))
        assertTrue(cut.endsWith("😀"))
    }
}
