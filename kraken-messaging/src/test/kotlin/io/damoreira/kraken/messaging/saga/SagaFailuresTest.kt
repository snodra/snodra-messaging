package io.damoreira.kraken.messaging.saga

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
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
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
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

private class SettableClock(@Volatile var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneId.of("UTC")
    override fun withZone(zone: ZoneId?): Clock = this
    override fun instant(): Instant = now
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaFailuresTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private lateinit var runtime: SagaRuntime
    private lateinit var failures: SagaFailures

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_failures_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7)).afterPropertiesSet()
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
            mongo = mongo,
            tx = tx,
            codec = codec,
            typeResolver = resolver,
            beans = { RtBean("the-bean") },
            clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(5), multiplier = 1.0, maxBackoff = Duration.ofMillis(20))),
        )
        failures = SagaFailures(mongo, runtime)
    }

    private fun deliver(event: Event, id: String) {
        val type = resolver.typeNameOf(event::class)
        runtime.process(type, codec.encode(event), MessageContext(id, type, "corr-$id", "u-1", emptySet(), null, mapOf("id" to id, "type" to type, "userId" to "u-1", "correlationId" to "corr-$id")))
    }

    private fun sagas(type: String) = mongo.find(Query(Criteria.where("type").`is`(type)), SagaDocument::class.java)
    private fun marker(id: String) = mongo.exists(Query(Criteria.where("_id").`is`(id)), "processed_events")

    @Test
    fun `retry applies a parked event once and removes the failure`() {
        deliver(RtOrderPlaced("f1"), "e-f1")
        assertEquals(listOf("rt.Flaky:e-f1"), failures.list("rt.Flaky").map { it.id })
        assertEquals(1, sagas("rt.Order").single().state.getInteger("placed"), "the healthy type handled it at once")
        FlakySwitch.broken.set(false)

        assertTrue(failures.retry("rt.Flaky:e-f1"))

        val saga = sagas("rt.Flaky").single()
        assertEquals(1, saga.state.getInteger("placed"))
        assertEquals("rt.Flaky|orderId=f1", saga.startKey)
        assertEquals(emptyList(), failures.list())
        assertEquals(1, mongo.count(Query(Criteria.where("sagaId").`is`(saga.id)), "saga_dispatch"), "its command was recorded for the relay")
        assertEquals(1, sagas("rt.Order").single().state.getInteger("placed"), "the other saga type was not re-applied")
        assertTrue(marker("saga:rt.Flaky:e-f1"), "the marker of the parking stays")
        assertFalse(failures.retry("rt.Flaky:e-f1"), "applied once: a second retry finds nothing")
        assertEquals(1, sagas("rt.Flaky").size)
    }

    @Test
    fun `a retry that fails again keeps it parked with attempts increased`() {
        deliver(RtOrderPlaced("f2"), "e-f2")
        val before = failures.list("rt.Flaky").single()
        assertEquals(3, before.attempts)
        clock.now = start.plusSeconds(60)

        assertFalse(failures.retry(before.id))

        val after = failures.list("rt.Flaky").single()
        assertEquals(6, after.attempts)
        assertEquals(start.plusSeconds(60), after.lastFailedAt)
        assertEquals(before.firstFailedAt, after.firstFailedAt)
        assertTrue("flaky on f2" in after.error, after.error)
        assertEquals(0, sagas("rt.Flaky").size)
        assertEquals(before.payload, after.payload)
    }

    @Test
    fun `retryAll retries every failure of a type`() {
        deliver(RtOrderPlaced("f3"), "e-f3")
        deliver(RtOrderPlaced("f4"), "e-f4")
        deliver(RtOrderPlaced("f5"), "e-f5")
        mongo.save(failures.list("rt.Flaky").first().copy(id = "rt.Other:e-x", sagaType = "rt.Other")) // another type's failure
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
    fun `retry of an unknown id is false`() {
        assertFalse(failures.retry("rt.Flaky:nope"))
        assertFalse(failures.retry("rt.Gone:e-1"))
        assertEquals(0, failures.retryAll("rt.Flaky"))
        assertNull(failures.list().firstOrNull())
    }
}
