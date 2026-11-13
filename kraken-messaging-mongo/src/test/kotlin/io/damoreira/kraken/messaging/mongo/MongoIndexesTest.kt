package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.index.IndexInfo
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** The indexes [MongoIndexes] creates (the spec's storage tables): names, keys, partial filters, TTLs, idempotence. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoIndexesTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private val settings = MongoStoreSettings(
        sagaEndedRetention = Duration.ofDays(30), sagaDispatchRetention = Duration.ofDays(7), deadlineRetention = Duration.ofDays(7),
        failedRetention = Duration.ofDays(30), externalCallRetention = Duration.ofDays(30),
    )
    private val now = Instant.parse("2026-11-03T10:00:00Z")

    @BeforeAll
    fun start() {
        mongoContainer.start()
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_indexes_test"))
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        mongo.db.drop()
        MongoIndexes.ensure(mongo, settings)
    }

    private fun index(collection: String, keys: List<String>): IndexInfo {
        val found = mongo.indexOps(collection).indexInfo.filter { info -> info.indexFields.map { it.key } == keys }
        return assertNotNull(found.singleOrNull(), "no single index on $collection$keys among ${mongo.indexOps(collection).indexInfo.map { it.name }}")
    }

    private fun saga(id: String, startKey: String?, ended: Boolean) = mongo.db.getCollection("sagas").insertOne(
        sagaDocument(SagaDocument(id, "PaymentSaga", "{}", startKey = startKey, ended = ended, endedAt = if (ended) now else null, createdAt = now, updatedAt = now)),
    )

    @Test
    fun `the indexes of the spec's storage table exist`() {
        val associations = index("sagas", listOf("type", "associations.k", "associations.v"))
        val live = associations.partialFilterExpression!!
        assertTrue("ended" in live && "false" in live && "\$exists" !in live, live)
        assertTrue(!associations.isUnique)

        val startKey = index("sagas", listOf("startKey"))
        assertTrue(startKey.isUnique)
        val filter = startKey.partialFilterExpression!!
        assertTrue("ended" in filter && "false" in filter && "startKey" in filter && "\$exists" in filter, filter)

        assertEquals(Duration.ofDays(30), index("sagas", listOf("endedAt")).expireAfter.orElse(null))

        index("saga_dispatch", listOf("sagaId", "seq"))
        index("saga_dispatch", listOf("dispatchedAt", "failedAt", "sagaId", "seq")) // what the relay's due-saga aggregation matches and sorts by
        val notice = index("saga_dispatch", listOf("noticeNextAt")).partialFilterExpression!!
        assertTrue("noticePending" in notice && "true" in notice, notice)
        assertEquals(Duration.ofDays(7), index("saga_dispatch", listOf("dispatchedAt")).expireAfter.orElse(null))
        assertEquals(Duration.ofDays(30), index("saga_dispatch", listOf("failedAt")).expireAfter.orElse(null), "an entry that failed for good expires too")
        assertEquals(Duration.ZERO, index("saga_dispatch_leases", listOf("until")).expireAfter.orElse(null))

        index("deadlines", listOf("status", "dueAt"))
        index("deadlines", listOf("sagaId", "status"))
        assertEquals(Duration.ofDays(7), index("deadlines", listOf("finishedAt")).expireAfter.orElse(null))

        index("saga_failures", listOf("sagaType", "lastFailedAt"))
        assertEquals(Duration.ofDays(30), index("saga_failures", listOf("lastFailedAt")).expireAfter.orElse(null), "a parked event expires after its last failure")

        assertEquals(Duration.ofDays(30), index("external_calls", listOf("updatedAt")).expireAfter.orElse(null))
        index("outbox", listOf("sentAt", "createdAt", "seq"))
        assertEquals(Duration.ofDays(7), index("outbox", listOf("sentAt")).expireAfter.orElse(null))
        assertEquals(Duration.ofDays(30), index("processed_events", listOf("processedAt")).expireAfter.orElse(null))
        assertEquals(Sort.Direction.ASC, index("deadlines", listOf("status", "dueAt")).indexFields.first().direction)
    }

    @Test
    fun `creating them twice is idempotent`() {
        val collections = listOf("outbox", "processed_events", "external_calls", "dead_letters", "sagas", "saga_dispatch", "saga_dispatch_leases", "deadlines", "saga_failures")
        val before = collections.associateWith { mongo.indexOps(it).indexInfo.map { i -> i.name }.sorted() }

        MongoIndexes.ensure(mongo, settings)

        assertEquals(before, before.keys.associateWith { mongo.indexOps(it).indexInfo.map { i -> i.name }.sorted() })
    }

    @Test
    fun `two live instances with the same startKey are refused while an ended one is not counted`() {
        saga("s-1", "PaymentSaga|orderId=o-1", ended = true)
        saga("s-2", "PaymentSaga|orderId=o-1", ended = true)
        saga("s-3", "PaymentSaga|orderId=o-1", ended = false)

        assertFailsWith<DuplicateKeyException> { translated { saga("s-4", "PaymentSaga|orderId=o-1", ended = false) } }

        // forceNew instances carry no startKey: any number may be live.
        saga("s-5", null, ended = false)
        saga("s-6", null, ended = false)
        // another key is independent, and ending the live one frees the key.
        saga("s-7", "PaymentSaga|orderId=o-2", ended = false)
        mongo.db.getCollection("sagas").updateOne(org.bson.Document("_id", "s-3"), org.bson.Document("\$set", org.bson.Document("ended", true)))
        saga("s-8", "PaymentSaga|orderId=o-1", ended = false)
    }
}
