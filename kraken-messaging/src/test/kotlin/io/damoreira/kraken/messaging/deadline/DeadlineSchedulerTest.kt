package io.damoreira.kraken.messaging.deadline

import io.damoreira.kraken.messaging.saga.payloadDoc
import io.damoreira.kraken.messaging.saga.stateDoc
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DeadlineHandler
import io.damoreira.kraken.messaging.saga.DeadlineManager
import io.damoreira.kraken.messaging.saga.DeadlineStatus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.EndSaga
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaCommands
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaIndexes
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.SettableClock
import io.damoreira.kraken.messaging.saga.StartSaga
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
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@MessageType("dl.Placed")
data class DlPlaced(val orderId: String, val seconds: Long = 900) : Event

@MessageType("dl.Schedule")
data class DlSchedule(val orderId: String, val name: String, val seconds: Long) : Event

@MessageType("dl.CancelOne")
data class DlCancelOne(val orderId: String) : Event

@MessageType("dl.CancelAll")
data class DlCancelAll(val orderId: String, val name: String) : Event

@MessageType("dl.Paid")
data class DlPaid(val orderId: String) : Event

@MessageType("dl.CancelCharge")
data class DlCancelCharge(val ref: String) : Command<Unit>

@MessageType("dl.TimeoutPayload")
data class DlTimeoutPayload(val reason: String)

/** Makes the `charge-timeout` handler fail while set, as a saga with a bug does. */
object DlSwitch {
    val broken = AtomicBoolean()
}

@Saga(name = "dl.Charge")
class DlSaga {
    var orderId: String? = null
    var timeoutDeadline: String? = null
    var fired = 0
    var reasons: MutableList<String> = mutableListOf()

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: DlPlaced, deadlines: DeadlineManager) {
        orderId = e.orderId
        timeoutDeadline = deadlines.schedule(Duration.ofSeconds(e.seconds), "charge-timeout", DlTimeoutPayload("no payment"))
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: DlSchedule, deadlines: DeadlineManager) {
        deadlines.schedule(Duration.ofSeconds(e.seconds), e.name, DlTimeoutPayload("scheduled ${e.name}"))
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: DlCancelOne, deadlines: DeadlineManager) {
        timeoutDeadline?.let(deadlines::cancel)
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: DlCancelAll, deadlines: DeadlineManager) {
        deadlines.cancelAll(e.name)
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: DlPaid) = Unit

    @DeadlineHandler("charge-timeout")
    fun timeout(payload: DlTimeoutPayload, commands: SagaCommands) {
        if (DlSwitch.broken.get()) throw IllegalStateException("deadline handler failed on purpose")
        fired++
        reasons += payload.reason
        commands.send(DlCancelCharge("ref-$orderId"))
    }

    @EndSaga
    @DeadlineHandler("end-now")
    fun endNow() {
        fired++
    }
}

private class OverdueMetrics : SagaMetrics {
    val fired = CopyOnWriteArrayList<String>()
    @Volatile var overdue: Map<String, Long> = emptyMap()

    override fun deadlineFired(name: String) { fired += name }
    override fun deadlinesOverdue(byName: Map<String, Long>) { overdue = byName }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadlineSchedulerTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("dl")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private val settings = DeadlineSettings(pollInterval = Duration.ofMillis(50), lease = Duration.ofSeconds(30))
    private lateinit var runtime: SagaRuntime
    private lateinit var metrics: OverdueMetrics

    @BeforeAll
    fun start() {
        mongoContainer.start()
        client = MongoClients.create(mongoContainer.connectionString + "/?directConnection=true")
        val factory = SimpleMongoClientDatabaseFactory(client, "deadline_scheduler_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7)).afterPropertiesSet()
        listOf("processed_events", "saga_failures").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() {
        client.close()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events").forEach { mongo.remove(Query(), it) }
        clock.now = start
        DlSwitch.broken.set(false)
        metrics = OverdueMetrics()
        runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(DlSaga::class)), mongo = mongo, tx = tx, codec = codec, typeResolver = resolver,
            beans = { error("no beans") }, clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
            metrics = metrics,
        )
    }

    private fun scheduler() = DeadlineScheduler(mongo, runtime, clock, settings, metrics)

    private fun deliver(event: Event, id: String = UUID.randomUUID().toString()) {
        val type = resolver.typeNameOf(event::class)
        runtime.process(type, codec.encode(event), MessageContext(id, type, "corr-$id", null, emptySet(), null, mapOf("id" to id, "type" to type)))
    }

    private fun advance(seconds: Long) {
        clock.now = clock.now.plusSeconds(seconds)
    }

    private fun saga(orderId: String) = mongo.findAll(SagaDocument::class.java).single { it.stateDoc.getString("orderId") == orderId }
    private fun deadlines(sagaId: String? = null) = mongo.find(
        if (sagaId == null) Query() else Query(Criteria.where("sagaId").`is`(sagaId)), DeadlineDocument::class.java,
    )
    private fun fired(orderId: String) = saga(orderId).stateDoc.getInteger("fired")

    @Test
    fun `a due deadline fires its handler once with its payload`() {
        deliver(DlPlaced("o1", seconds = 900))
        val scheduler = scheduler()

        scheduler.pollOnce()
        assertEquals(0, fired("o1"), "not due yet")

        advance(901)
        assertEquals(1, scheduler.pollOnce())
        scheduler.pollOnce()

        assertEquals(1, fired("o1"))
        assertEquals(listOf("no payment"), saga("o1").stateDoc.getList("reasons", String::class.java))
        val deadline = deadlines().single()
        assertEquals(DeadlineStatus.FIRED, deadline.status)
        assertEquals(clock.now, deadline.finishedAt)
        assertEquals(listOf("charge-timeout"), metrics.fired)
    }

    @Test
    fun `a deadline fires after a restart`() {
        deliver(DlPlaced("o1", seconds = 60))
        scheduler() // a scheduler that never got to poll: its service instance stopped

        advance(61)
        scheduler().pollOnce() // a new instance of the service

        assertEquals(1, fired("o1"))
    }

    @Test
    fun `a cancelled deadline never fires`() {
        deliver(DlPlaced("o1", seconds = 60))
        deliver(DlCancelOne("o1"))

        advance(3600)
        scheduler().pollOnce()

        assertEquals(0, fired("o1"))
        assertEquals(DeadlineStatus.CANCELLED, deadlines().single().status)
    }

    @Test
    fun `cancelAll cancels every scheduled deadline of that name for the saga`() {
        deliver(DlPlaced("o1", seconds = 60))
        deliver(DlSchedule("o1", "charge-timeout", 120))
        deliver(DlSchedule("o1", "charge-timeout", 180))
        deliver(DlSchedule("o1", "end-now", 240))
        deliver(DlPlaced("o2", seconds = 60))

        deliver(DlCancelAll("o1", "charge-timeout"))

        val byName = deadlines(saga("o1").id).groupBy { it.name }
        assertTrue(byName.getValue("charge-timeout").all { it.status == DeadlineStatus.CANCELLED })
        assertEquals(3, byName.getValue("charge-timeout").size)
        assertEquals(DeadlineStatus.SCHEDULED, byName.getValue("end-now").single().status)
        assertEquals(DeadlineStatus.SCHEDULED, deadlines(saga("o2").id).single().status, "another saga's deadline is untouched")
        advance(3600)
        scheduler().pollOnce()
        assertEquals(1, fired("o1"), "only end-now fired")
    }

    @Test
    fun `a deadline of an ended saga is marked cancelled when found due`() {
        deliver(DlPlaced("o1", seconds = 60))
        deliver(DlPaid("o1"))
        val ended = saga("o1")
        assertTrue(ended.ended)
        // A deadline that survived the end (a race, or one written by an older version): found due, it is cancelled, not run.
        mongo.insert(DeadlineDocument("late", "dl.Charge", ended.id, "charge-timeout", """{"reason":"late"}""", "dl.TimeoutPayload", start.plusSeconds(10)))
        mongo.insert(DeadlineDocument("orphan", "dl.Charge", "no-such-saga", "charge-timeout", """{"reason":"orphan"}""", "dl.TimeoutPayload", start.plusSeconds(10)))

        advance(3600)
        scheduler().pollOnce()

        assertEquals(DeadlineStatus.CANCELLED, mongo.findById("late", DeadlineDocument::class.java)!!.status)
        assertEquals(DeadlineStatus.CANCELLED, mongo.findById("orphan", DeadlineDocument::class.java)!!.status)
        assertEquals(0, fired("o1"))
        assertEquals(emptyList(), metrics.fired)
    }

    @Test
    fun `an expired lease makes a deadline fire again (at least once)`() {
        deliver(DlPlaced("o1", seconds = 60))
        advance(61)
        DlSwitch.broken.set(true) // the first service instance claims it and fails (or crashes) before firing
        val first = scheduler()

        first.pollOnce()
        assertEquals(DeadlineStatus.SCHEDULED, deadlines().single().status)
        assertNotNull(deadlines().single().claimedUntil)

        DlSwitch.broken.set(false)
        advance(10)
        scheduler().pollOnce()
        assertEquals(0, fired("o1"), "the lease of the first claim still holds")

        advance(30)
        scheduler().pollOnce()

        assertEquals(1, fired("o1"))
        assertEquals(DeadlineStatus.FIRED, deadlines().single().status)
    }

    @Test
    fun `two schedulers fire one deadline once while the lease holds`() {
        deliver(DlPlaced("o1", seconds = 60))
        advance(61)
        val barrier = CyclicBarrier(2)
        val claimed = CopyOnWriteArrayList<Int>()
        val threads = List(2) { Thread { barrier.await(); claimed += scheduler().pollOnce() } }

        threads.forEach { it.start() }
        threads.forEach { it.join(20_000) }

        assertEquals(1, fired("o1"))
        assertEquals(1, claimed.sum(), "exactly one of the two claimed it: $claimed")
        assertEquals(DeadlineStatus.FIRED, deadlines().single().status)
    }

    @Test
    fun `EndSaga on a deadline handler ends the saga`() {
        deliver(DlPlaced("o1", seconds = 600))
        deliver(DlSchedule("o1", "end-now", 10))

        advance(11)
        scheduler().pollOnce()

        val saga = saga("o1")
        assertTrue(saga.ended)
        assertEquals(clock.now, saga.endedAt)
        val byName = deadlines(saga.id).associateBy { it.name }
        assertEquals(DeadlineStatus.FIRED, byName.getValue("end-now").status)
        assertEquals(DeadlineStatus.CANCELLED, byName.getValue("charge-timeout").status, "ending the saga cancels its other deadlines")
    }

    @Test
    fun `a deadline handler that sends commands records them for the relay`() {
        deliver(DlPlaced("o1", seconds = 60))
        val sagaId = saga("o1").id
        advance(61)

        scheduler().pollOnce()

        val entry = mongo.find(Query(Criteria.where("sagaId").`is`(sagaId)), SagaDispatchDocument::class.java).single()
        assertEquals(DispatchKind.COMMAND, entry.kind)
        assertEquals("dl.CancelCharge", entry.type)
        assertEquals("ref-o1", entry.payloadDoc.getString("ref"))
        assertEquals("$sagaId:${entry.seq}", entry.id)
        assertEquals((saga("o1").version) * 1000, entry.seq, "seq follows the saga's new version")
        assertNull(entry.dispatchedAt, "recorded, not sent: the relay sends it")
    }

    @Test
    fun `a poll reports the overdue deadlines it could not fire`() {
        deliver(DlPlaced("o1", seconds = 60))
        advance(61)
        DlSwitch.broken.set(true)
        val scheduler = scheduler()

        scheduler.pollOnce()
        assertEquals(mapOf("charge-timeout" to 1L), metrics.overdue)

        DlSwitch.broken.set(false)
        advance(31)
        scheduler.pollOnce()
        assertEquals(emptyMap(), metrics.overdue)
    }
}
