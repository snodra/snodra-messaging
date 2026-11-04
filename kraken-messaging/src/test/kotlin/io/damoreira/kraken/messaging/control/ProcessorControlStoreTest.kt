package io.damoreira.kraken.messaging.control

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.test.KrakenContainers
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
class ProcessorControlStoreTest {
    private val container = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private var now = Instant.parse("2026-11-04T10:00:00Z")
    private val clock = object : Clock() {
        override fun getZone() = ZoneOffset.UTC
        override fun withZone(zone: java.time.ZoneId?) = this
        override fun instant() = now
    }
    private lateinit var store: ProcessorControlStore

    @BeforeAll fun start() {
        container.start()
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(container.connectionString + "/?directConnection=true"), "processor_control_store_test"))
        store = ProcessorControlStore(mongo, clock)
    }

    @AfterAll fun stop() = container.stop()

    @BeforeEach fun clean() {
        mongo.remove(Query(), ProcessorControlStore.COLLECTION)
    }

    private fun raw(id: String): Document = mongo.db.getCollection(ProcessorControlStore.COLLECTION).find(Document("_id", id)).first()!!

    @Test fun `a pause writes every field of the document, a resume clears pausedAt and pausedBy`() {
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
        assertEquals(ProcessorControlStore.Control("audit", paused = false, updatedAt = now), store.get("audit"))
    }

    @Test fun `one operation holds the lock until it releases it or its lease passes`() {
        assertEquals(ProcessorControlStore.Lock.Taken, store.lock("audit", "op-1", Duration.ofSeconds(30)))
        val doc = raw("audit")
        assertEquals(setOf("_id", "paused", "pausedAt", "pausedBy", "operation", "replay", "updatedAt"), doc.keys)
        val op = doc["operation"] as Document
        assertEquals(listOf("id", "kind", "leaseUntil", "preparedAt"), op.keys.toList())
        assertEquals("reset", op["kind"])
        assertEquals(Date.from(now.plusSeconds(30)), op["leaseUntil"])

        val held = assertIs<ProcessorControlStore.Lock.Held>(store.lock("audit", "op-2", Duration.ofSeconds(30)))
        assertEquals("op-1", held.by!!.id)
        assertFalse(store.release("audit", "op-2"), "never releases another operation's lock")

        now = now.plusSeconds(20)
        assertEquals(ProcessorControlStore.Lock.Taken, store.lock("audit", "op-1", Duration.ofSeconds(30)), "the same operation renews")
        val renewed = store.get("audit")!!.operation!!
        assertEquals(now.plusSeconds(30), renewed.leaseUntil)
        assertEquals(now.minusSeconds(20), renewed.preparedAt, "a renewal keeps preparedAt")
        assertFalse(store.release("audit", "op-1", expiredOnly = true), "not expired yet")

        now = now.plusSeconds(31)
        assertTrue(store.get("audit")!!.operation!!.expired(now))
        assertEquals(ProcessorControlStore.Lock.Taken, store.lock("audit", "op-2", Duration.ofSeconds(30)), "an expired lease is free")
        assertFalse(store.release("audit", "op-1", expiredOnly = true))
        assertTrue(store.release("audit", "op-2"))
        assertNull(store.get("audit")!!.operation)
        assertEquals(ProcessorControlStore.Lock.Taken, store.lock("audit", "op-3", Duration.ofSeconds(30)))
    }

    @Test fun `replay ranges are written as Int32 partitions and Int64 offsets and pulled one by one`() {
        store.lock("default", "op-1", Duration.ofSeconds(30))
        val a = ReplayRange("kraken.events", 0, 0, 120)
        val b = ReplayRange("kraken.events", 1, 5, 9)
        store.setReplay("default", listOf(a, b))
        val item = (raw("default")["replay"] as List<*>).first() as Document
        assertEquals(listOf("topic", "partition", "from", "until"), item.keys.toList())
        assertIs<Int>(item["partition"])
        assertIs<Long>(item["from"])
        assertIs<Long>(item["until"])
        assertEquals(listOf(a, b), store.get("default")!!.replay)
        assertFalse(store.removeReplay("default", a.copy(until = 121)), "only the exact range")
        assertTrue(store.removeReplay("default", a))
        assertFalse(store.removeReplay("default", a))
        assertEquals(listOf(b), store.get("default")!!.replay)
        assertEquals(setOf("default"), store.read(listOf("default", "audit")).keys, "a missing document is absent")
    }

    @Test fun `a document written with missing fields reads as not paused, no operation and no replay`() {
        mongo.db.getCollection(ProcessorControlStore.COLLECTION).insertOne(Document("_id", "audit"))
        assertEquals(ProcessorControlStore.Control("audit"), store.get("audit"))
    }
}
