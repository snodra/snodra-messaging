package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.saga.stateDoc
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.LocalRawCommandSender
import io.damoreira.kraken.messaging.saga.MarkFailingTemplate
import io.damoreira.kraken.messaging.saga.RelaySettings
import io.damoreira.kraken.messaging.saga.RlCharge
import io.damoreira.kraken.messaging.saga.RlSaga
import io.damoreira.kraken.messaging.saga.RlStart
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDispatchRelay
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaIndexes
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.SettableClock
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@MessageType("ex.Print")
data class ExPrint(val jobId: String, val name: String) : Command<ExReceipt>

data class ExReceipt(val jobId: String, val ticket: Int)

@MessageType("ex.PrintSafe")
data class ExPrintSafe(val jobId: String) : Command<ExReceipt>

@MessageType("ex.Poll")
data class ExPoll(val jobId: String, val attempt: Int) : Command<Unit>

@MessageType("ex.Stamp")
data class ExStamp(val jobId: String) : io.damoreira.kraken.messaging.api.Event

/** The adapter under test: [behaviour] is the call to the external system; [bodies] counts how often it really ran. */
class ExPrinterAdapter {
    val bodies = AtomicInteger()
    val polls = AtomicInteger()
    val stamps = AtomicInteger()
    @Volatile var behaviour: (ExPrint) -> ExReceipt = { ExReceipt(it.jobId, 1000 + bodies.get()) }

    @CommandHandler
    @IdempotentHandler
    fun handle(c: ExPrint): ExReceipt {
        bodies.incrementAndGet()
        return behaviour(c)
    }

    @CommandHandler
    @IdempotentHandler(externalIdempotent = true)
    fun handle(c: ExPrintSafe): ExReceipt = ExReceipt(c.jobId, bodies.incrementAndGet())

    /** The key includes the poll attempt, as the long-polling pattern wants. */
    @CommandHandler
    @IdempotentHandler(key = "'poll:' + message.jobId + ':' + message.attempt")
    fun handle(c: ExPoll) {
        polls.incrementAndGet()
    }

    @EventHandler
    @IdempotentHandler(key = "'stamp:' + message.jobId")
    fun on(e: ExStamp) {
        stamps.incrementAndGet()
    }
}

/** The idempotent adapter of the saga test: handles the `rl.Charge` a saga sends. */
class ChargeAdapter {
    val bodies = AtomicInteger()

    @CommandHandler
    @IdempotentHandler
    fun handle(c: RlCharge) {
        bodies.incrementAndGet()
    }
}

class SuspendingAdapter {
    @CommandHandler
    @IdempotentHandler
    suspend fun handle(c: ExPrint): ExReceipt = ExReceipt(c.jobId, 1)
}

class BadKeyAdapter {
    @CommandHandler
    @IdempotentHandler(key = "headers[")
    fun handle(c: ExPrint): ExReceipt = ExReceipt(c.jobId, 1)
}

class QueryAdapter {
    @QueryHandler
    @IdempotentHandler
    fun handle(c: io.damoreira.kraken.messaging.fixtures.GetCount): io.damoreira.kraken.messaging.fixtures.Count = io.damoreira.kraken.messaging.fixtures.Count(1)
}

private class ExternalMetrics : SagaMetrics {
    val calls = CopyOnWriteArrayList<Pair<String, SagaMetrics.ExternalOutcome>>()
    override fun externalCall(handler: String, outcome: SagaMetrics.ExternalOutcome) { calls += handler to outcome }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IdempotentHandlerTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MarkFailingTemplate
    private lateinit var tx: TransactionTemplate
    private lateinit var txManager: MongoTransactionManager
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("ex")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private val lease = Duration.ofMinutes(5)
    private lateinit var adapter: ExPrinterAdapter
    private lateinit var registry: HandlerRegistry
    private lateinit var invoker: HandlerInvoker
    private lateinit var metrics: ExternalMetrics

    @BeforeAll
    fun start() {
        mongoContainer.start()
        client = MongoClients.create(mongoContainer.connectionString + "/?directConnection=true")
        val factory = SimpleMongoClientDatabaseFactory(client, "idempotent_handler_test")
        mongo = MarkFailingTemplate(factory)
        txManager = MongoTransactionManager(factory)
        tx = TransactionTemplate(txManager)
        SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7)).afterPropertiesSet()
        ExternalCallIndexes(mongo, Duration.ofDays(30)).afterPropertiesSet()
        listOf("processed_events", "saga_failures", "outbox", "external_calls").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() {
        client.close()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "saga_dispatch_leases", "deadlines", "saga_failures", "processed_events", "outbox", "external_calls").forEach { mongo.remove(Query(), it) }
        clock.now = start
        adapter = ExPrinterAdapter()
        metrics = ExternalMetrics()
        registry = HandlerRegistry(resolver).also { it.register(adapter) }
        invoker = HandlerInvoker(registry, codec, interceptor())
    }

    private fun interceptor(txm: MongoTransactionManager? = txManager) =
        IdempotencyInterceptor(mongo, codec, txm, clock, ExternalCallSettings(lease), metrics)

    private fun call(type: String, payload: String, key: String? = "job-1", on: HandlerInvoker = invoker): InvocationResult {
        val headers = if (key == null) emptyMap() else mapOf("idempotency-key" to key)
        val ctx = MessageContext("m-1", type, "corr-1", null, emptySet(), null, headers)
        return runBlocking { on.invokeCommand(type, payload.toByteArray(), ctx, null) }
    }

    private fun print(key: String? = "job-1", jobId: String = "j1") = call("ex.Print", """{"jobId":"$jobId","name":"hi"}""", key)
    private fun record(key: String) = mongo.findById(key, ExternalCallDocument::class.java)
    private fun receipt(result: InvocationResult) = codec.decode((result as InvocationResult.Success).payload, ExReceipt::class)
    private fun failure(result: InvocationResult) = assertIs<InvocationResult.Failure>(result)

    @Test
    fun `a second call with the same key returns the stored reply without running the body`() {
        val first = receipt(print())
        val second = receipt(print())

        assertEquals(1, adapter.bodies.get(), "the body ran once")
        assertEquals(first, second)
        val stored = record("job-1")!!
        assertEquals(ExternalCallStatus.SUCCEEDED, stored.status)
        assertEquals(1, stored.attempts)
        assertNull(stored.leaseUntil)
        assertEquals(listOf("ExPrinterAdapter.handle" to SagaMetrics.ExternalOutcome.SUCCEEDED, "ExPrinterAdapter.handle" to SagaMetrics.ExternalOutcome.REPLAYED), metrics.calls)
        print(key = "job-2")
        assertEquals(2, adapter.bodies.get(), "another key runs the body")
    }

    @Test
    fun `a failed call re-throws the stored error`() {
        adapter.behaviour = { throw IllegalStateException("printer refused ${it.name}") }

        val first = failure(print())
        val second = failure(print())

        assertEquals(1, adapter.bodies.get(), "the permanent failure is not retried")
        assertEquals(ErrorCode.HANDLER_ERROR, first.code)
        assertEquals(first.code, second.code)
        assertEquals(first.message, second.message)
        assertTrue("printer refused hi" in second.message, second.message)
        val stored = record("job-1")!!
        assertEquals(ExternalCallStatus.FAILED, stored.status)
        assertTrue("printer refused hi" in stored.error!!, stored.error)
        assertTrue(!ExternalFailureMarkers.isRetryable(second.details) && !ExternalFailureMarkers.isUnknownOutcome(second.details))
    }

    @Test
    fun `a retryable failure lets the next call run the body again`() {
        var attempt = 0
        adapter.behaviour = { if (++attempt == 1) throw RetryableExternalException("partner busy") else ExReceipt(it.jobId, 7) }

        val first = failure(print())

        assertTrue(ExternalFailureMarkers.isRetryable(first.details), "the relay can tell it is transient")
        assertNull(record("job-1"), "the record is reset: nothing is remembered of the failed attempt")
        assertEquals(7, receipt(print()).ticket)
        assertEquals(2, adapter.bodies.get())
        print()
        assertEquals(2, adapter.bodies.get(), "now it is stored")
    }

    @Test
    fun `an in-progress record past its lease becomes UNKNOWN and the call fails with UNKNOWN_OUTCOME`() {
        mongo.insert(ExternalCallDocument("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
        clock.now = start.plus(lease).plusSeconds(1) // the first call crashed mid-way, a lease ago

        val first = failure(print())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(first.details), first.message)
        assertEquals(0, adapter.bodies.get(), "never called again automatically")
        assertEquals(ExternalCallStatus.UNKNOWN, record("job-1")!!.status)
        val again = failure(print())
        assertTrue(ExternalFailureMarkers.isUnknownOutcome(again.details))
        assertEquals(0, adapter.bodies.get())
    }

    @Test
    fun `an in-progress record within its lease asks the caller to come back`() {
        mongo.insert(ExternalCallDocument("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))

        val busy = failure(print())

        assertTrue(ExternalFailureMarkers.isRetryable(busy.details), "another call is in flight: transient")
        assertEquals(0, adapter.bodies.get())
        assertEquals(ExternalCallStatus.IN_PROGRESS, record("job-1")!!.status)
    }

    @Test
    fun `with externalIdempotent the expired call is retried`() {
        mongo.insert(ExternalCallDocument("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
        clock.now = start.plus(lease).plusSeconds(1)

        val result = call("ex.PrintSafe", """{"jobId":"j1"}""")

        assertEquals(1, receipt(result).ticket)
        assertEquals(1, adapter.bodies.get())
        val stored = record("job-1")!!
        assertEquals(ExternalCallStatus.SUCCEEDED, stored.status)
        assertEquals(2, stored.attempts, "the second attempt is counted")
    }

    @Test
    fun `a missing key is a call-time error naming the handler`() {
        val result = failure(print(key = null))

        assertEquals(ErrorCode.HANDLER_ERROR, result.code)
        assertTrue("ExPrinterAdapter.handle" in result.message && "idempotency-key" in result.message, result.message)
        assertEquals(0, adapter.bodies.get())
    }

    @Test
    fun `a startup check names the class and method of a suspend, a badly keyed or a query handler`() {
        val suspend = assertFailsWith<IllegalStateException> { HandlerRegistry(resolver).register(SuspendingAdapter()) }
        assertTrue("SuspendingAdapter.handle" in suspend.message!! && "suspend" in suspend.message!!, suspend.message)
        val badKey = assertFailsWith<IllegalStateException> { HandlerRegistry(resolver).register(BadKeyAdapter()) }
        assertTrue("BadKeyAdapter.handle" in badKey.message!! && "headers[" in badKey.message!!, badKey.message)
        val query = assertFailsWith<IllegalStateException> { HandlerRegistry(resolver).register(QueryAdapter()) }
        assertTrue("QueryAdapter.handle" in query.message!!, query.message)
    }

    @Test
    fun `the key can be an expression over the message`() {
        fun poll(job: String, attempt: Int) = call("ex.Poll", """{"jobId":"$job","attempt":$attempt}""", key = null)

        poll("j1", 1)
        poll("j1", 1)
        poll("j1", 2)

        assertEquals(2, adapter.polls.get(), "same job and attempt once, the next attempt again")
        assertNotNull(record("poll:j1:1"))
        assertNotNull(record("poll:j1:2"))
    }

    @Test
    fun `an idempotent event handler runs once per key`() = runBlocking {
        val ctx = MessageContext("e-1", "ex.Stamp", "corr-1", null, emptySet(), null, emptyMap())

        invoker.invokeEvent("ex.Stamp", """{"jobId":"j9"}""".toByteArray(), ctx)
        invoker.invokeEvent("ex.Stamp", """{"jobId":"j9"}""".toByteArray(), ctx)

        assertEquals(1, adapter.stamps.get())
    }

    @Test
    fun `the call record is written in its own transactions, so an outer rollback does not forget the call`() {
        assertFailsWith<IllegalStateException> {
            tx.executeWithoutResult {
                print()
                throw IllegalStateException("the caller's transaction rolls back")
            }
        }

        assertEquals(ExternalCallStatus.SUCCEEDED, record("job-1")!!.status, "the external call happened: the record must stay")
        print()
        assertEquals(1, adapter.bodies.get())
    }

    @Test
    fun `a handler marked idempotent in a service without the interceptor fails loudly`() {
        val bare = HandlerInvoker(registry, codec)

        val result = failure(call("ex.Print", """{"jobId":"j1","name":"hi"}""", on = bare))

        assertTrue("ExPrinterAdapter.handle" in result.message && "idempotency" in result.message.lowercase(), result.message)
        assertEquals(0, adapter.bodies.get())
    }

    // ---- with the saga relay -----------------------------------------------------------------------------------------

    private class Harness(val relay: SagaDispatchRelay, val runtime: SagaRuntime, val charge: ChargeAdapter)

    private fun harness(): Harness {
        val charge = ChargeAdapter()
        val chargeRegistry = HandlerRegistry(MessageTypeResolver("rl")).also { it.register(charge) }
        val chargeInvoker = HandlerInvoker(chargeRegistry, codec, interceptor())
        val sagaResolver = MessageTypeResolver("rl")
        val runtime = SagaRuntime(
            types = SagaModelScanner(sagaResolver, codec).scan(listOf(RlSaga::class)), mongo = mongo, tx = tx, codec = codec, typeResolver = sagaResolver,
            beans = { error("no beans") }, clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        val dispatcher = LocalEventDispatcher(chargeRegistry, chargeInvoker, tx, mongo, runtime)
        val relay = SagaDispatchRelay(
            mongo, tx, LocalRawCommandSender(chargeInvoker), OutboxEventBus(mongo, codec, sagaResolver, TraceHeaders(null, null)), dispatcher, codec, sagaResolver,
            clock, RelaySettings(maxAttempts = 3, initialBackoff = Duration.ofSeconds(1)),
        )
        val start = RlStart("o1")
        runtime.process("rl.Start", codec.encode(start), MessageContext("e-1", "rl.Start", "corr-1", null, emptySet(), null, mapOf("id" to "e-1", "type" to "rl.Start")))
        return Harness(relay, runtime, charge)
    }

    private fun dispatchEntry() = mongo.findAll(SagaDispatchDocument::class.java).single { it.kind == DispatchKind.COMMAND }

    @Test
    fun `a crash between the send and marking it dispatched re-sends with the same key and the adapter runs its body once`() {
        val h = harness()
        val key = dispatchEntry().id
        mongo.failNextMark.set(true)

        h.relay.pollOnce()
        assertNull(dispatchEntry().dispatchedAt, "the mark was lost")
        assertEquals(1, h.charge.bodies.get())
        h.relay.pollOnce()

        assertEquals(1, h.charge.bodies.get(), "the same key: the stored reply, the body did not run again")
        assertNotNull(dispatchEntry().dispatchedAt)
        assertEquals(ExternalCallStatus.SUCCEEDED, record(key)!!.status)
        assertEquals(listOf(key), mongo.findAll(ExternalCallDocument::class.java).map { it.id })
    }

    @Test
    fun `the relay maps UNKNOWN_OUTCOME to SagaCommandFailed(reason = UNKNOWN_OUTCOME)`() {
        val h = harness()
        val entry = dispatchEntry()
        mongo.insert(ExternalCallDocument(entry.id, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
        clock.now = start.plus(lease).plusSeconds(1)

        h.relay.pollOnce()

        val saga = mongo.findAll(SagaDocument::class.java).single()
        assertEquals(listOf("rl.Charge:UNKNOWN_OUTCOME"), saga.stateDoc.getList("failures", String::class.java))
        assertEquals(0, h.charge.bodies.get(), "an unknown outcome is not called again")
        assertNotNull(dispatchEntry().failedAt)
        assertEquals(0, mongo.count(Query(Criteria.where("failedAt").`is`(null).and("dispatchedAt").`is`(null)), "saga_dispatch"), "nothing left pending")
    }
}
