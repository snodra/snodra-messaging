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
import kotlin.test.assertFailsWith
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

    @Test fun `a range whose segment or positions are not decimal is refused, and is never there to remove`() = runBlocking<Unit> {
        store.lock("default", "op-1", Duration.ofSeconds(30))
        val ok = ReplayRange("kraken.events", "0", "0", "3")
        store.setReplay("default", listOf(ok))
        for (bad in listOf(ok.copy(segment = "kraken.events-0"), ok.copy(from = "a"), ok.copy(until = ""))) {
            assertFailsWith<IllegalArgumentException> { store.setReplay("default", listOf(bad)) }
            assertFailsWith<IllegalArgumentException> { store.deleteMarkers("default", listOf(bad)) }
            assertFalse(store.removeReplay("default", bad))
        }
        assertEquals(listOf(ok), store.get("default")!!.replay, "nothing written")
    }

    @Test fun `a document written with missing fields reads as not paused, no operation and no replay`() = runBlocking<Unit> {
        mongo.db.getCollection(MongoProcessorControlStore.COLLECTION).insertOne(Document("_id", "audit"))
        assertEquals(Control("audit"), store.get("audit"))
    }
}
