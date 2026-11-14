package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.external.ExternalCallStatus
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.test.KrakenContainers
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * The documents the MongoDB store writes, field by field, where the core's tests no longer read them raw (they run on the
 * in-memory store): existing data and other tools read these fields.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoDocumentShapesTest {
    private val container = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MongoTemplate
    private lateinit var store: MongoMessageStore

    @BeforeAll
    fun start() {
        container.start()
        client = MongoClients.create(container.connectionString + "/?directConnection=true")
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(client, "shapes_" + UUID.randomUUID().toString().replace("-", "")))
        store = MongoMessageStore(mongo).also { it.ensureIndexes() }
    }

    @AfterAll
    fun stop() {
        client.close()
        container.stop()
    }

    private fun raw(collection: String, id: String): Document = mongo.findById(id, Document::class.java, collection)!!

    @Test
    fun `a deadline keeps the correlation id of the message that scheduled it`() {
        val due = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        store.sagas.deadlines.insert(DeadlineDocument("d1", "t.Order", "s1", "remind", """{"n":1}""", "t.Remind", due, "corr-e-40"))
        val stored = raw("deadlines", "d1")
        assertEquals("corr-e-40", stored.getString("correlationId"))
        assertEquals("SCHEDULED", stored.getString("status"))
        assertEquals(java.util.Date.from(due), stored.getDate("dueAt"))
    }

    @Test
    fun `an external call is stored with its status as the enum name and its reply as JSON text, and read back from that layout`() {
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        runBlocking {
            store.externalCalls.insert(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.plusSeconds(300), owner = "call-A"))
            store.externalCalls.succeed("job-1", "call-A", """{"jobId":"j1","pages":2}""".toByteArray(), now)
        }
        val stored = raw("external_calls", "job-1")
        assertEquals("SUCCEEDED", stored.getString("status"))
        assertIs<String>(stored["reply"], "the reply is JSON text")
        assertEquals("""{"jobId":"j1","pages":2}""", stored.getString("reply"))
        assertNull(stored["owner"])
        assertNull(stored["leaseUntil"])

        // Another call (or an older version) writing the same layout is read as such.
        mongo.updateFirst(Query(Criteria.where("_id").`is`("job-1")), Update().set("status", ExternalCallStatus.UNKNOWN.name).unset("reply"), "external_calls")
        val read = runBlocking { store.externalCalls.find("job-1") }!!
        assertEquals(ExternalCallStatus.UNKNOWN, read.status)
        assertNull(read.reply)
    }
}
