package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Control
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Lock
import io.damoreira.kraken.messaging.test.KrakenContainers
import kotlinx.coroutines.runBlocking
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
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The `processor_control` document (P6b, ADR 0023): its exact shape, the conditional lock, renewal, release and ranges. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoProcessorControlStoreTest {
    private val container = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private var now = Instant.parse("2026-11-04T10:00:00Z")
    private val clock = object : Clock() {
        override fun getZone() = ZoneOffset.UTC
        override fun withZone(zone: java.time.ZoneId?) = this
        override fun instant() = now
    }
    private lateinit var store: MongoProcessorControlStore

    @BeforeAll fun start() {
        container.start()
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(container.connectionString + "/?directConnection=true"), "processor_control_store_test"))
        store = MongoProcessorControlStore(mongo, clock)
    }

    @AfterAll fun stop() = container.stop()

    @BeforeEach fun clean() {
        mongo.remove(Query(), MongoProcessorControlStore.COLLECTION)
    }

    private fun raw(id: String): Document = mongo.db.getCollection(MongoProcessorControlStore.COLLECTION).find(Document("_id", id)).first()!!

    @Test fun `a pause writes every field of the document, a resume clears pausedAt and pausedBy`() = runBlocking<Unit> {
        store.setPaused("audit", true, "c-1")
        val doc = raw("audit")
        assertEquals(setOf("_id", "paused", "pausedAt", "pausedBy", "operation", "replay", "updatedAt"), doc.keys)
        assertEquals(true, doc["paused"])
        assertEquals(Date.from(now), doc["pausedAt"])
        assertEquals("c-1", doc["pausedBy"])
        assertNull(doc["operation"])
        assertTrue(doc.containsKey("operation"))
        assertEquals(emptyList<Any>(), doc["replay"])
        store.setPaused("audit", true, "c-2") // idempotent
        store.setPaused("audit", false, "local")
        val resumed = raw("audit")
        assertEquals(false, resumed["paused"])
        assertTrue(resumed.containsKey("pausedAt") && resumed["pausedAt"] == null)
        assertTrue(resumed.containsKey("pausedBy") && resumed["pausedBy"] == null)
        assertEquals(Control("audit", paused = false, updatedAt = now), store.get("audit"))
    }

    @Test fun `one operation holds the lock until it releases it or its lease passes`() = runBlocking<Unit> {
        assertEquals(Lock.Taken, store.lock("audit", "op-1", Duration.ofSeconds(30)))
        val doc = raw("audit")
        assertEquals(setOf("_id", "paused", "pausedAt", "pausedBy", "operation", "replay", "updatedAt"), doc.keys)
        val op = doc["operation"] as Document
        assertEquals(listOf("id", "kind", "leaseUntil", "preparedAt"), op.keys.toList())
        assertEquals("reset", op["kind"])
        assertEquals(Date.from(now.plusSeconds(30)), op["leaseUntil"])

        val held = assertIs<Lock.Held>(store.lock("audit", "op-2", Duration.ofSeconds(30)))
        assertEquals("op-1", held.by!!.id)
        assertFalse(store.release("audit", "op-2"), "never releases another operation's lock")

        now = now.plusSeconds(20)
        assertEquals(Lock.Taken, store.lock("audit", "op-1", Duration.ofSeconds(30)), "the same operation renews")
        val renewed = store.get("audit")!!.operation!!
        assertEquals(now.plusSeconds(30), renewed.leaseUntil)
        assertEquals(now.minusSeconds(20), renewed.preparedAt, "a renewal keeps preparedAt")
        assertFalse(store.release("audit", "op-1", expiredOnly = true), "not expired yet")

        now = now.plusSeconds(31)
        assertTrue(store.get("audit")!!.operation!!.expired(now))
        assertEquals(Lock.Taken, store.lock("audit", "op-2", Duration.ofSeconds(30)), "an expired lease is free")
        assertFalse(store.release("audit", "op-1", expiredOnly = true))
        assertTrue(store.release("audit", "op-2"))
        assertNull(store.get("audit")!!.operation)
        assertEquals(Lock.Taken, store.lock("audit", "op-3", Duration.ofSeconds(30)))
    }

    @Test fun `replay ranges are written as Int32 partitions and Int64 offsets and pulled one by one`() = runBlocking<Unit> {
        store.lock("default", "op-1", Duration.ofSeconds(30))
        val a = ReplayRange("kraken.events", "0", "0", "120")
        val b = ReplayRange("kraken.events", "1", "5", "9")
        store.setReplay("default", listOf(a, b))
        val item = (raw("default")["replay"] as List<*>).first() as Document
        assertEquals(listOf("topic", "partition", "from", "until"), item.keys.toList())
        assertIs<Int>(item["partition"])
        assertIs<Long>(item["from"])
        assertIs<Long>(item["until"])
        assertEquals(listOf(a, b), store.get("default")!!.replay)
        assertEquals(0, (item["partition"] as Int))
        assertEquals(120L, (item["until"] as Long))
        assertFalse(store.removeReplay("default", a.copy(until = "121")), "only the exact range")
        assertTrue(store.removeReplay("default", a))
        assertFalse(store.removeReplay("default", a))
        assertEquals(listOf(b), store.get("default")!!.replay)
        assertEquals(setOf("default"), store.read(listOf("default", "audit")).keys, "a missing document is absent")
    }

    @Test fun `a range whose segment or positions are not decimal is stored as text in the same fields, and read back and removed`() = runBlocking<Unit> {
        store.lock("default", "op-1", Duration.ofSeconds(30))
        val decimal = ReplayRange("kraken.events", "0", "0", "3")
        val text = ReplayRange("orders-stream", "p-0", "a-5", "a-9")
        val leadingZero = ReplayRange("kraken.events", "1", "007", "9") // not canonical: kept as written
        store.setReplay("default", listOf(decimal, text, leadingZero))
        val items = (raw("default")["replay"] as List<*>).map { it as Document }
        assertEquals(listOf("topic", "partition", "from", "until"), items[1].keys.toList(), "the same fields, no new ones")
        assertEquals(listOf<Any?>("p-0", "a-5", "a-9"), listOf(items[1]["partition"], items[1]["from"], items[1]["until"]))
        assertEquals("007", items[2]["from"])
        assertIs<Long>(items[2]["until"])
        assertEquals(listOf(decimal, text, leadingZero), store.get("default")!!.replay)
        assertTrue(store.removeReplay("default", text))
        assertTrue(store.removeReplay("default", leadingZero))
        assertEquals(listOf(decimal), store.get("default")!!.replay)
    }

    /**
     * Markers written before the event store became an add-on (Int32 `partition`, Int64 `offset`) are found and deleted by a
     * replay of their range as before; markers without a position (a saga's, an ingress') dedupe and survive any reset; a
     * marker of a store with text positions is deleted by a range of text positions (ordered by bytes).
     */
    @Test fun markersWithoutPositionBehaveAsBefore() = runBlocking<Unit> {
        val markers = MongoProcessedEventStore(mongo)
        val collection = mongo.db.getCollection(MongoProcessedEventStore.COLLECTION)
        collection.deleteMany(Document())
        val at = Date.from(now)
        // As the library wrote them before (P6b): Int32 partition, Int64 offset.
        collection.insertOne(Document("_id", "processor:default:old-4").append("type", "t").append("processedAt", at)
            .append("processor", "default").append("topic", "kraken.events").append("partition", 0).append("offset", 4L))
        collection.insertOne(Document("_id", "processor:default:old-9").append("type", "t").append("processedAt", at)
            .append("processor", "default").append("topic", "kraken.events").append("partition", 0).append("offset", 9L))
        val saga = io.damoreira.kraken.messaging.store.ProcessedMarker("saga:Order:e-1", "t", now)
        val ingress = io.damoreira.kraken.messaging.store.ProcessedMarker("ingress:printer:k-1", "t", now)
        val textual = io.damoreira.kraken.messaging.store.ProcessedMarker("processor:default:s-1", "t", now, "default", "orders-stream", "p-0", "a-5")
        val decimal = io.damoreira.kraken.messaging.store.ProcessedMarker("processor:default:new-5", "t", now, "default", "kraken.events", "0", "5")
        for (m in listOf(saga, ingress, textual, decimal)) assertTrue(markers.record(m))
        assertFalse(markers.record(saga), "a positionless marker dedupes")
        assertFalse(markers.record(io.damoreira.kraken.messaging.store.ProcessedMarker("processor:default:old-4", "t", now)), "an old marker dedupes")

        val new = collection.find(Document("_id", decimal.id)).first()!!
        assertIs<Int>(new["partition"], "a decimal segment is written as before")
        assertIs<Long>(new["offset"])
        assertEquals("a-5", collection.find(Document("_id", textual.id)).first()!!["offset"], "a text position in the same field")

        assertEquals(2L, store.deleteMarkers("default", listOf(ReplayRange("kraken.events", "0", "4", "6"))), "old-4 and new-5, by value")
        assertTrue(markers.exists("processor:default:old-9"), "past the range")
        assertEquals(1L, store.deleteMarkers("default", listOf(ReplayRange("orders-stream", "p-0", "a-0", "a-9"))), "a text range deletes a text marker")
        assertEquals(1L, store.deleteMarkers("default", listOf(ReplayRange("kraken.events", "0", "0", "99999"))), "old-9 goes by a wider range")
        for (m in listOf(saga, ingress)) assertTrue(markers.exists(m.id), "${m.id} survives every reset")
    }

    @Test fun `a document written with missing fields reads as not paused, no operation and no replay`() = runBlocking<Unit> {
        mongo.db.getCollection(MongoProcessorControlStore.COLLECTION).insertOne(Document("_id", "audit"))
        assertEquals(Control("audit"), store.get("audit"))
    }
}
