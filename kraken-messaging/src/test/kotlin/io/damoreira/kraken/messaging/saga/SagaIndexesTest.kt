package io.damoreira.kraken.messaging.saga

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.external.ExternalCallIndexes
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaIndexesTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var indexes: SagaIndexes
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
        indexes = SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7))
        indexes.afterPropertiesSet()
        ExternalCallIndexes(mongo, Duration.ofDays(30)).afterPropertiesSet()
    }

    private fun index(collection: String, keys: List<String>): IndexInfo {
        val found = mongo.indexOps(collection).indexInfo.filter { info -> info.indexFields.map { it.key } == keys }
        return assertNotNull(found.singleOrNull(), "no single index on $collection$keys among ${mongo.indexOps(collection).indexInfo.map { it.name }}")
    }

    private fun saga(id: String, startKey: String?, ended: Boolean) =
        SagaDocument(id, "PaymentSaga", "{}", startKey = startKey, ended = ended, endedAt = if (ended) now else null, createdAt = now, updatedAt = now)

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
        index("saga_dispatch", listOf("dispatchedAt", "nextAttemptAt"))
        assertEquals(Duration.ofDays(7), index("saga_dispatch", listOf("dispatchedAt")).expireAfter.orElse(null))
        assertEquals(Duration.ZERO, index("saga_dispatch_leases", listOf("until")).expireAfter.orElse(null))

        index("deadlines", listOf("status", "dueAt"))
        index("deadlines", listOf("sagaId", "status"))
        assertEquals(Duration.ofDays(7), index("deadlines", listOf("finishedAt")).expireAfter.orElse(null))

        index("saga_failures", listOf("sagaType", "lastFailedAt"))

        assertEquals(Duration.ofDays(30), index("external_calls", listOf("updatedAt")).expireAfter.orElse(null))
        assertEquals(Sort.Direction.ASC, index("deadlines", listOf("status", "dueAt")).indexFields.first().direction)
    }

    @Test
    fun `creating them twice is idempotent`() {
        val before = listOf("sagas", "saga_dispatch", "deadlines", "saga_failures").associateWith { mongo.indexOps(it).indexInfo.map { i -> i.name }.sorted() }

        indexes.afterPropertiesSet()
        ExternalCallIndexes(mongo, Duration.ofDays(30)).afterPropertiesSet()

        assertEquals(before, before.keys.associateWith { mongo.indexOps(it).indexInfo.map { i -> i.name }.sorted() })
    }

    @Test
    fun `two live instances with the same startKey are refused while an ended one is not counted`() {
        mongo.insert(saga("s-1", "PaymentSaga|orderId=o-1", ended = true))
        mongo.insert(saga("s-2", "PaymentSaga|orderId=o-1", ended = true))
        mongo.insert(saga("s-3", "PaymentSaga|orderId=o-1", ended = false))

        assertFailsWith<DuplicateKeyException> { mongo.insert(saga("s-4", "PaymentSaga|orderId=o-1", ended = false)) }

        // forceNew instances carry no startKey: any number may be live.
        mongo.insert(saga("s-5", null, ended = false))
        mongo.insert(saga("s-6", null, ended = false))
        // another key is independent, and ending the live one frees the key.
        mongo.insert(saga("s-7", "PaymentSaga|orderId=o-2", ended = false))
        mongo.updateFirst(
            org.springframework.data.mongodb.core.query.Query(org.springframework.data.mongodb.core.query.Criteria.where("_id").`is`("s-3")),
            org.springframework.data.mongodb.core.query.Update().set("ended", true), SagaDocument::class.java,
        )
        mongo.insert(saga("s-8", "PaymentSaga|orderId=o-1", ended = false))
    }
}
