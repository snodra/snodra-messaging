package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.mongoStore
import io.damoreira.kraken.messaging.SAGAS
import io.damoreira.kraken.messaging.SAGA_FAILURES
import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaStores
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A saga type that fails while [broken] is set, as one with a bug (or an unavailable collaborator) does. */
object FlakySwitch {
    val broken = AtomicBoolean()
}

@Saga(name = "rt.Flaky")
class RtFlakySaga {
    var placed = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtOrderPlaced, commands: SagaCommands) {
        if (FlakySwitch.broken.get()) throw IllegalStateException("flaky on ${e.orderId}")
        placed++
        commands.send(RtDoIt(e.orderId))
    }
}

/**
 * A failure store that plays a `discard` committed by someone else right after the [after]th rolled-back attempt of a
 * retry, i.e. between the last failed attempt and the bookkeeping that records it.
 */
private class DiscardAfterRollbacks(private val inner: SagaFailureStore, private val plain: MongoTemplate, private val after: Int) : SagaFailureStore by inner {
    val rollbacks = AtomicInteger()

    override fun delete(id: String): Boolean {
        val deleted = inner.delete(id)
        if (deleted) {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCompletion(status: Int) {
                    if (status == TransactionSynchronization.STATUS_ROLLED_BACK && rollbacks.incrementAndGet() == after) {
                        plain.remove(Query(Criteria.where("_id").`is`(id)), SagaFailureDocument::class.java, SAGA_FAILURES)
                    }
                }
            })
        }
        return deleted
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaFailuresTest {
    private lateinit var plain: MongoTemplate
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private lateinit var runtime: SagaRuntime
    private lateinit var failures: SagaFailures
    private lateinit var store: MessageStore

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_failures_test")
        mongo = MongoTemplate(factory)
        plain = MongoTemplate(SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_failures_test"))
        tx = TransactionTemplate(MongoTransactionManager(factory))
        store = mongoStore(mongo)
        listOf("processed_events", "saga_failures").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events").forEach { mongo.remove(Query(), it) }
        FlakySwitch.broken.set(true)
        clock.now = start
        RtGate.reset()
        runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RtOrderSaga::class, RtFlakySaga::class)),
            ports = store.sagas,
            tx = tx,
            codec = codec,
            typeResolver = resolver,
            beans = { RtBean("the-bean") },
            clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(5), multiplier = 1.0, maxBackoff = Duration.ofMillis(20))),
        )
        failures = SagaFailures(store.sagas.failures, runtime)
    }

    private fun deliver(event: Event, id: String) {
        val type = resolver.typeNameOf(event::class)
        runtime.process(type, codec.encode(event), MessageContext(id, type, "corr-$id", "u-1", emptySet(), null, mapOf("id" to id, "type" to type, "userId" to "u-1", "correlationId" to "corr-$id")))
    }

    private fun sagas(type: String) = mongo.find(Query(Criteria.where("type").`is`(type)), SagaDocument::class.java, SAGAS)
    private fun marker(id: String) = mongo.exists(Query(Criteria.where("_id").`is`(id)), "processed_events")

    @Test
    fun `retry applies a parked event once and removes the failure`() {
        deliver(RtOrderPlaced("f1"), "e-f1")
        assertEquals(listOf("rt.Flaky:e-f1"), failures.list("rt.Flaky").map { it.id })
        assertEquals(1, sagas("rt.Order").single().stateDoc.getInteger("placed"), "the healthy type handled it at once")
        FlakySwitch.broken.set(false)

        assertTrue(failures.retry("rt.Flaky:e-f1"))

        val saga = sagas("rt.Flaky").single()
        assertEquals(1, saga.stateDoc.getInteger("placed"))
        assertEquals("rt.Flaky|orderId=f1", saga.startKey)
        assertEquals(emptyList(), failures.list())
        assertEquals(1, mongo.count(Query(Criteria.where("sagaId").`is`(saga.id)), "saga_dispatch"), "its command was recorded for the relay")
        assertEquals(1, sagas("rt.Order").single().stateDoc.getInteger("placed"), "the other saga type was not re-applied")
        assertTrue(marker("saga:rt.Flaky:e-f1"), "the marker of the parking stays")
        assertFalse(failures.retry("rt.Flaky:e-f1"), "applied once: a second retry finds nothing")
        assertEquals(1, sagas("rt.Flaky").size)
    }

    @Test
    fun `a retry that fails again keeps it parked with attempts increased`() {
        deliver(RtOrderPlaced("f2"), "e-f2")
        val before = failures.list("rt.Flaky").single()
        assertEquals(2, before.attempts)
        clock.now = start.plusSeconds(60)

        assertFalse(failures.retry(before.id))

        val after = failures.list("rt.Flaky").single()
        assertEquals(4, after.attempts)
        assertEquals(start.plusSeconds(60), after.lastFailedAt)
        assertEquals(before.firstFailedAt, after.firstFailedAt)
        assertTrue("flaky on f2" in after.error, after.error)
        assertEquals(0, sagas("rt.Flaky").size)
        assertEquals(before.payload, after.payload)
    }

    @Test
    fun `a discard that lands during a failing retry is not undone by the retry's bookkeeping`() {
        val discarding = DiscardAfterRollbacks(store.sagas.failures, plain, after = 2)
        val ports = store.sagas.let { SagaStores(it.sagas, it.dispatch, it.deadlines, discarding, it.markers) }
        val racing = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RtFlakySaga::class)), ports = ports, tx = tx, codec = codec, typeResolver = resolver,
            beans = { RtBean("the-bean") }, clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(5), multiplier = 1.0, maxBackoff = Duration.ofMillis(20))),
        )
        val admin = SagaFailures(store.sagas.failures, racing)
        val type = resolver.typeNameOf(RtOrderPlaced::class)
        racing.process(type, codec.encode(RtOrderPlaced("f9")), MessageContext("e-f9", type, "corr-e-f9", null, emptySet(), null, mapOf("id" to "e-f9", "type" to type)))
        assertEquals(1, admin.list().size)
        discarding.rollbacks.set(0)

        assertFalse(admin.retry("rt.Flaky:e-f9"), "it failed again")

        assertEquals(2, discarding.rollbacks.get(), "both attempts of the retry rolled back, the discard landed after the last one")
        assertEquals(emptyList(), admin.list(), "the discarded failure stays discarded")
    }

    @Test
    fun `retryAll retries every failure of a type`() {
        deliver(RtOrderPlaced("f3"), "e-f3")
        deliver(RtOrderPlaced("f4"), "e-f4")
        deliver(RtOrderPlaced("f5"), "e-f5")
        mongo.save(failures.list("rt.Flaky").first().copy(id = "rt.Other:e-x", sagaType = "rt.Other"), SAGA_FAILURES) // another type's failure
        FlakySwitch.broken.set(false)

        assertEquals(3, failures.retryAll("rt.Flaky"))

        assertEquals(3, sagas("rt.Flaky").size)
        assertEquals(listOf("rt.Other:e-x"), failures.list().map { it.id }, "only the other type's failure is left")
    }

    @Test
    fun `discard removes it without applying`() {
        deliver(RtOrderPlaced("f6"), "e-f6")
        FlakySwitch.broken.set(false)

        assertTrue(failures.discard("rt.Flaky:e-f6"))

        assertEquals(emptyList(), failures.list())
        assertEquals(0, sagas("rt.Flaky").size)
        assertTrue(marker("saga:rt.Flaky:e-f6"), "the event stays marked: a redelivery does not apply it either")
        assertFalse(failures.discard("rt.Flaky:e-f6"))
    }

    @Test
    fun `a retry of a saga type this service does not run is refused and leaves the parked event as it was`() {
        deliver(RtOrderPlaced("f7"), "e-f7")
        val parked = failures.list("rt.Flaky").single()
        mongo.save(parked.copy(id = "rt.Other:e-f7", sagaType = "rt.Other"), SAGA_FAILURES) // parked by another service, or before the type was removed
        val before = mongo.findById("rt.Other:e-f7", SagaFailureDocument::class.java, SAGA_FAILURES)!!
        clock.now = start.plusSeconds(60)

        val one = assertFailsWith<IllegalStateException> { failures.retry("rt.Other:e-f7") }
        val all = assertFailsWith<IllegalStateException> { failures.retryAll("rt.Other") }

        assertEquals("Saga type 'rt.Other' does not run in this service: parked event rt.Other:e-f7 is left as it was", one.message)
        assertEquals("Saga type 'rt.Other' does not run in this service: its parked events (1) are left as they were", all.message)
        assertEquals(before, mongo.findById("rt.Other:e-f7", SagaFailureDocument::class.java, SAGA_FAILURES), "its error, attempts and lastFailedAt are untouched")
    }

    @Test
    fun `retry of an unknown id is false`() {
        assertFalse(failures.retry("rt.Flaky:nope"))
        assertFalse(failures.retry("rt.Gone:e-1"))
        assertEquals(0, failures.retryAll("rt.Flaky"))
        assertEquals(0, failures.retryAll("rt.Gone"), "nothing parked: nothing to refuse, as in Rust")
        assertNull(failures.list().firstOrNull())
    }
}
