package com.damoreira.snodra.messaging.mongo

import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetterError
import com.damoreira.snodra.messaging.deadletter.DeadLetterFactory
import com.damoreira.snodra.messaging.deadletter.DeadLetterMode
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.test.SnodraContainers
import io.mockk.every
import io.mockk.mockk
import com.damoreira.snodra.messaging.eventstore.Delivery
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
import kotlinx.coroutines.runBlocking
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The storage contract of `dead_letters`: the documents this library writes are exactly the hand-written
 * fixtures in `src/test/resources/dead_letters_contract` (the MongoDB layout of the specification,
 * https://snodra.damoreira.com/docs/library-authors/mongodb-layout/),
 * types included (Int32 `partition`, Int64 `offset`, BinData, dates), with no
 * `_class`, no null, a blank `id` or `type` header absent as `eventId` or `eventType`, and `replay` (parked while the
 * processor replayed the record) written only when true: the core's [DeadLetterFactory] builds the letter, this add-on
 * stores it. Every fixture document, as any Snodra library writes it, reads back through the store and the core's admin
 * API; the indexes have the documented names.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoDeadLetterContractTest {
    private val mongoContainer = SnodraContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private val now = Instant.parse("2026-11-04T10:00:00Z")
    private lateinit var store: MongoDeadLetterStore
    private val letters = DeadLetterFactory(Duration.ofDays(30), Clock.fixed(now, ZoneOffset.UTC))

    @BeforeAll fun start() {
        mongoContainer.start()
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "dead_letter_contract"))
        store = MongoDeadLetterStore(mongo, Clock.fixed(now, ZoneOffset.UTC))
    }

    @AfterAll fun stop() = mongoContainer.stop()

    @BeforeEach fun clean() { mongo.remove(Query(), MongoDeadLetterStore.COLLECTION) }

    private fun fixture(name: String): BsonDocument =
        BsonDocument.parse(javaClass.getResource("/dead_letters_contract/$name.json")!!.readText())

    private fun stored(id: String): BsonDocument =
        mongo.db.getCollection(MongoDeadLetterStore.COLLECTION, BsonDocument::class.java).find(Document("_id", id)).first()!!

    /** Letters built with the clock at [at] (the fixture's `parkedAt`). */
    private fun storeAt(at: String) = DeadLetterFactory(Duration.ofDays(30), Clock.fixed(Instant.parse(at), ZoneOffset.UTC))

    /**
     * A record of `snodra.events` as the Kafka event store delivers it: headers as lossy UTF-8 text, a repeated name keeping its
     * last value at its first place (the event-store contract carries headers as text, one value per name).
     */
    private fun record(partition: Int, offset: Long, key: String?, value: ByteArray?, vararg headers: Pair<String, ByteArray?>): Delivery {
        val text = LinkedHashMap<String, String>()
        headers.forEach { (name, bytes) -> text[name] = String(bytes ?: ByteArray(0), Charsets.UTF_8) }
        return Delivery(partition.toString(), offset.toString(), key, text, value ?: ByteArray(0), 0L)
    }

    @Test fun `a HANDLER_FAILED letter is the fixture`() = runBlocking<Unit> {
        val r = record(1, 42, "order-7", """{"orderId":"o-7","amount":12.30}""".toByteArray(),
            "type" to "examples.OrderPlaced".toByteArray(), "id" to "evt-7".toByteArray(), "correlationId" to "c-0".toByteArray(),
            "correlationId" to "corr-7".toByteArray(), // a repeated header keeps its last value
            "userId" to ByteArray(0))
        val at = storeAt("2026-11-04T10:00:05Z")
        val letter = at.letter("orders", "snodra.events", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("IllegalStateException", "boom"), attempts = 5)
        assertTrue(store.park(letter))
        assertEquals(fixture("handler_failed"), stored("orders:snodra.events:1:42"))
    }

    @Test fun `a SEQUENCED letter is the fixture`() = runBlocking<Unit> {
        val r = record(1, 43, "order-7", """{"orderId":"o-7"}""".toByteArray(),
            "type" to "examples.OrderShipped".toByteArray(), "id" to "evt-8".toByteArray())
        val at = storeAt("2026-11-04T10:00:06.250Z")
        assertTrue(store.park(at.sequenced("orders", "snodra.events", r, headId = "orders:snodra.events:1:42")))
        assertEquals(fixture("sequenced"), stored("orders:snodra.events:1:43"))
    }

    @Test fun `INVALID letters with a binary or an empty value and no key are the fixtures`() = runBlocking<Unit> {
        val binary = record(0, 7, null, byteArrayOf(0xFF.toByte(), 0, 0xFE.toByte()), "source" to "legacy-".toByteArray() + 0xFF.toByte() + "tool".toByteArray())
        val at = storeAt("2026-11-04T10:01:00Z")
        assertTrue(store.park(at.letter("default", "snodra.events", binary, DeadLetterReason.INVALID, DeadLetterError.invalid("record without type header"), attempts = 0)))
        assertEquals(fixture("invalid_binary"), stored("default:snodra.events:0:7"))

        val empty = record(2, 9, "order-9", ByteArray(0), "type" to "examples.OrderPlaced".toByteArray())
        val later = storeAt("2026-11-04T10:02:00Z")
        assertTrue(store.park(later.letter("orders", "snodra.events", empty, DeadLetterReason.INVALID, DeadLetterError.invalid("payload is not a JSON object"), attempts = 0)))
        assertEquals(fixture("invalid_empty"), stored("orders:snodra.events:2:9"))
    }

    @Test fun `blank id and type headers are kept in headers and absent as eventId and eventType`() = runBlocking<Unit> {
        val r = record(0, 8, null, "{}".toByteArray(), "type" to "  ".toByteArray(), "id" to ByteArray(0))
        val at = storeAt("2026-11-04T10:03:00Z")
        assertTrue(store.park(at.letter("default", "snodra.events", r, DeadLetterReason.INVALID, DeadLetterError.invalid("record without type header"), attempts = 0)))
        assertEquals(fixture("invalid_blank_headers"), stored("default:snodra.events:0:8"))
    }

    @Test fun `a letter parked during a replay is the fixture, replay written only then`() = runBlocking<Unit> {
        val r = record(1, 44, "order-8", """{"orderId":"o-8"}""".toByteArray(),
            "type" to "examples.OrderPlaced".toByteArray(), "id" to "evt-9".toByteArray())
        val at = storeAt("2026-11-04T10:04:00Z")
        assertTrue(store.park(at.letter("orders", "snodra.events", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("IllegalStateException", "boom"), attempts = 5, replay = true)))
        assertEquals(fixture("replayed"), stored("orders:snodra.events:1:44"))
        assertFalse("replay" in fixture("handler_failed"), "absent when false")
    }

    @Test fun `parking is idempotent - the same record parks once`() = runBlocking<Unit> {
        val r = record(2, 41, "order-7", "{}".toByteArray(), "type" to "t".toByteArray())
        assertTrue(store.park(letters.letter("orders", "snodra.events", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("X", "first"), attempts = 1)))
        assertFalse(store.park(letters.letter("orders", "snodra.events", r, DeadLetterReason.HANDLER_FAILED, DeadLetterError("X", "second"), attempts = 1)))
        assertEquals("first", stored("orders:snodra.events:2:41").getDocument("error").getString("message").value)
    }

    @Test fun `every shared fixture reads back through the store and the admin API`() = runBlocking<Unit> {
        val coll = mongo.db.getCollection(MongoDeadLetterStore.COLLECTION, BsonDocument::class.java)
        for (name in FIXTURES) coll.insertOne(fixture(name))

        val failed = store.get("orders:snodra.events:1:42")!!
        assertEquals(DeadLetter(
            id = "orders:snodra.events:1:42", processor = "orders", source = "snodra.events", segment = "1", position = "42", key = "order-7",
            eventId = "evt-7", eventType = "examples.OrderPlaced",
            headers = mapOf("type" to "examples.OrderPlaced", "id" to "evt-7", "correlationId" to "corr-7", "userId" to ""),
            payload = """{"orderId":"o-7","amount":12.30}""", payloadBinary = null, reason = DeadLetterReason.HANDLER_FAILED,
            error = DeadLetterError("IllegalStateException", "boom"), blockedBy = null, attempts = 5,
            parkedAt = Instant.parse("2026-11-04T10:00:05Z"), lastAttemptAt = Instant.parse("2026-11-04T10:00:05Z"),
            expireAt = Instant.parse("2026-12-04T10:00:05Z"),
        ), failed)
        val sequenced = store.get("orders:snodra.events:1:43")!!
        assertEquals(DeadLetterReason.SEQUENCED, sequenced.reason)
        assertEquals("orders:snodra.events:1:42", sequenced.blockedBy)
        assertEquals(DeadLetterError.sequenced("orders:snodra.events:1:42"), sequenced.error)
        assertEquals(Instant.parse("2026-11-04T10:00:06.250Z"), sequenced.parkedAt)
        val binary = store.get("default:snodra.events:0:7")!!
        assertEquals(listOf(0xFF.toByte(), 0, 0xFE.toByte()), binary.value().toList())
        assertNull(binary.payload)
        assertNull(binary.key)
        assertNull(binary.eventId)
        assertEquals("legacy-\uFFFDtool", binary.headers["source"])
        assertEquals(DeadLetterError.invalid("record without type header"), binary.error)
        val empty = store.get("orders:snodra.events:2:9")!!
        assertEquals(0, empty.value().size)
        assertEquals("examples.OrderPlaced", empty.eventType)
        assertNull(empty.eventId)
        val blank = store.get("default:snodra.events:0:8")!!
        assertNull(blank.eventId)
        assertNull(blank.eventType)
        assertEquals(mapOf("type" to "  ", "id" to ""), blank.headers)
        assertFalse(failed.replay)
        assertTrue(store.get("orders:snodra.events:1:44")!!.replay)
        assertNull(store.get("missing"))
        assertEquals("orders:snodra.events:1:42", store.head("orders", "order-7"))

        // The admin API's reads, on an instance that runs both processors.
        val dispatcher = mockk<LocalEventDispatcher> { every { runs(any()) } returns true }
        val admin = DeadLetters(DeadLetterMode.COLLECTION, store, dispatcher)
        val listed = admin.list(null, 100)
        assertEquals(
            listOf(
                "orders:snodra.events:1:42", "orders:snodra.events:1:43", "default:snodra.events:0:7", "orders:snodra.events:2:9",
                "default:snodra.events:0:8", "orders:snodra.events:1:44",
            ),
            listed.map { it.id }, "by parkedAt",
        )
        assertEquals(listed, FIXTURES.map { name -> admin.get(fixture(name).getString("_id").value)!! })
        assertEquals(6L, admin.count(null))
    }

    @Test fun `the indexes have the documented names, keys and TTL`() = runBlocking<Unit> {
        MongoIndexes.ensure(mongo, MongoStoreSettings())
        MongoIndexes.ensure(mongo, MongoStoreSettings()) // idempotent
        val indexes = mongo.db.getCollection(MongoDeadLetterStore.COLLECTION).listIndexes().associateBy { it.getString("name") }
        assertEquals(Document(mapOf("processor" to 1, "key" to 1, "offset" to 1)), indexes.getValue("dead_letters_processor_key_offset")["key"])
        assertEquals(Document(mapOf("processor" to 1, "parkedAt" to 1)), indexes.getValue("dead_letters_processor_parked")["key"])
        val ttl = indexes.getValue("dead_letters_expire")
        assertEquals(Document("expireAt", 1), ttl["key"])
        assertEquals(0L, (ttl["expireAfterSeconds"] as Number).toLong())
    }
}

/** Every shared fixture, in `parkedAt` order. */
private val FIXTURES = listOf("handler_failed", "sequenced", "invalid_binary", "invalid_empty", "invalid_blank_headers", "replayed")
