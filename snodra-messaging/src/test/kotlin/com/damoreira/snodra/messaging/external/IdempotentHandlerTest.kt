package com.damoreira.snodra.messaging.external

import com.damoreira.snodra.messaging.saga.stateDoc
import com.damoreira.snodra.messaging.changeExternalCallBehindTheStoresBack
import com.damoreira.snodra.messaging.dispatchEntries
import com.damoreira.snodra.messaging.externalCalls
import com.damoreira.snodra.messaging.removeExternalCallBehindTheStoresBack
import com.damoreira.snodra.messaging.sagas
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.InvocationResult
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.events.OutboxEventBus
import com.damoreira.snodra.messaging.saga.DispatchKind
import com.damoreira.snodra.messaging.saga.LocalRawCommandSender
import com.damoreira.snodra.messaging.FaultyMessageStore
import com.damoreira.snodra.messaging.store.ExternalCallRecord
import com.damoreira.snodra.messaging.saga.RelaySettings
import com.damoreira.snodra.messaging.saga.RlCharge
import com.damoreira.snodra.messaging.saga.RlSaga
import com.damoreira.snodra.messaging.saga.RlStart
import com.damoreira.snodra.messaging.saga.SagaDispatchDocument
import com.damoreira.snodra.messaging.saga.SagaDispatchRelay
import com.damoreira.snodra.messaging.saga.SagaDocument
import com.damoreira.snodra.messaging.saga.SagaMetrics
import com.damoreira.snodra.messaging.saga.SagaModelScanner
import com.damoreira.snodra.messaging.saga.SagaRetry
import com.damoreira.snodra.messaging.saga.SagaRuntime
import com.damoreira.snodra.messaging.saga.SettableClock
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
data class ExStamp(val jobId: String) : com.damoreira.snodra.messaging.api.Event

/** The adapter under test: [behaviour] is the call to the external system; [bodies] counts how often it really ran. */
class ExPrinterAdapter {
    val bodies = AtomicInteger()
    val polls = AtomicInteger()
    val stamps = AtomicInteger()
    @Volatile var behaviour: (ExPrint) -> ExReceipt = { ExReceipt(it.jobId, 1000 + bodies.get()) }

    /** Runs inside the `externalIdempotent` body before it returns: what the partner does, or what happens meanwhile. */
    @Volatile var safeBehaviour: (ExPrintSafe) -> Unit = {}

    @CommandHandler
    @IdempotentHandler
    fun handle(c: ExPrint): ExReceipt {
        bodies.incrementAndGet()
        return behaviour(c)
    }

    @CommandHandler
    @IdempotentHandler(externalIdempotent = true)
    fun handle(c: ExPrintSafe): ExReceipt {
        val ticket = bodies.incrementAndGet()
        safeBehaviour(c)
        return ExReceipt(c.jobId, ticket)
    }

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
    fun handle(c: com.damoreira.snodra.messaging.fixtures.GetCount): com.damoreira.snodra.messaging.fixtures.Count = com.damoreira.snodra.messaging.fixtures.Count(1)
}

private class ExternalMetrics : SagaMetrics {
    val calls = CopyOnWriteArrayList<Pair<String, SagaMetrics.ExternalOutcome>>()
    override fun externalCall(handler: String, outcome: SagaMetrics.ExternalOutcome) { calls += handler to outcome }
}

/** Idempotent handlers (the interceptor and its external call records) over the in-memory message store (no container). */
class IdempotentHandlerTest {
    private lateinit var memory: MemoryMessageStore
    private lateinit var store: FaultyMessageStore
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("ex")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private val lease = Duration.ofMinutes(5)
    private lateinit var adapter: ExPrinterAdapter
    private lateinit var registry: HandlerRegistry
    private lateinit var invoker: HandlerInvoker
    private lateinit var metrics: ExternalMetrics

    @BeforeEach
    fun setUp() {
        memory = MemoryMessageStore()
        store = FaultyMessageStore(memory)
        tx = TransactionTemplate(store.transactions)
        clock.now = start
        adapter = ExPrinterAdapter()
        metrics = ExternalMetrics()
        registry = HandlerRegistry(resolver).also { it.register(adapter) }
        invoker = HandlerInvoker(registry, codec, interceptor())
    }

    private fun interceptor() = IdempotencyInterceptor(store, codec, clock, ExternalCallSettings(lease), metrics)

    private fun call(type: String, payload: String, key: String? = "job-1", on: HandlerInvoker = invoker): InvocationResult {
        val headers = if (key == null) emptyMap() else mapOf("idempotency-key" to key)
        val ctx = MessageContext("m-1", type, "corr-1", null, emptySet(), null, headers)
        return runBlocking { on.invokeCommand(type, payload.toByteArray(), ctx, null) }
    }

    private fun print(key: String? = "job-1", jobId: String = "j1") = call("ex.Print", """{"jobId":"$jobId","name":"hi"}""", key)
    private fun record(key: String) = runBlocking { store.externalCalls.find(key) }
    private fun insertCall(call: ExternalCallRecord) = runBlocking { store.externalCalls.insert(call) }
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
        assertNull(stored.owner, "a settled record has no owner")
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
        val error = String(stored.error!!)
        assertTrue("printer refused hi" in error, error)
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
    fun `an adapter that reports an unknown outcome itself leaves the record UNKNOWN, so a redelivery reports UNKNOWN_OUTCOME too`() {
        adapter.behaviour = { throw ExternalCallUnknownOutcomeException("the connection broke after the request went out") }

        val first = failure(print())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(first.details), first.message)
        assertEquals(ExternalCallStatus.UNKNOWN, record("job-1")!!.status, "not FAILED: the external system may have acted")
        val again = failure(print())
        assertTrue(ExternalFailureMarkers.isUnknownOutcome(again.details), "the redelivery: ${again.message}")
        assertEquals(1, adapter.bodies.get(), "never called again automatically")
    }

    @Test
    fun `a failure to write SUCCEEDED after the body succeeded surfaces as an unknown outcome, never as a failed call`() {
        store.failNextSucceededWrite.set(true)

        val first = failure(print())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(first.details), "the call happened but its record could not say so: ${first.message}")
        assertEquals(1, adapter.bodies.get())
        assertEquals(ExternalCallStatus.UNKNOWN, record("job-1")!!.status, "never FAILED")
        val again = failure(print())
        assertTrue(ExternalFailureMarkers.isUnknownOutcome(again.details))
        assertEquals(1, adapter.bodies.get())
    }

    @Test
    fun `with externalIdempotent a failure to write SUCCEEDED is retryable, the partner de-duplicates the repeat`() {
        store.failNextSucceededWrite.set(true)

        val first = failure(call("ex.PrintSafe", """{"jobId":"j1"}"""))

        assertTrue(ExternalFailureMarkers.isRetryable(first.details), first.message)
        assertEquals(ExternalCallStatus.IN_PROGRESS, record("job-1")!!.status, "left for its lease to settle: never FAILED")
    }

    @Test
    fun `an in-progress record past its lease becomes UNKNOWN and the call fails with UNKNOWN_OUTCOME`() {
        insertCall(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
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
    fun `a lapsed call that settles while its lease is given up is read again, so the caller gets its reply and not an unknown outcome`() {
        insertCall(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
        clock.now = start.plus(lease).plusSeconds(1)
        // The slow first call writes SUCCEEDED between this call's read of the record and its change of it to UNKNOWN.
        store.afterNextExternalCallRead.set {
            memory.changeExternalCallBehindTheStoresBack("job-1") {
                it.copy(status = ExternalCallStatus.SUCCEEDED, reply = codec.encode(ExReceipt("j1", 42)), leaseUntil = null)
            }
        }

        val result = print()

        assertEquals(ExReceipt("j1", 42), receipt(result), "the slow call's reply, read again")
        assertEquals(0, adapter.bodies.get())
        assertEquals(ExternalCallStatus.SUCCEEDED, record("job-1")!!.status, "not turned UNKNOWN over the success")
        assertEquals(listOf("ExPrinterAdapter.handle" to SagaMetrics.ExternalOutcome.REPLAYED), metrics.calls)
    }

    /** Inside call A's body: A's lease runs out and call B takes the record over (attempt 2, its own owner token, a fresh lease), as begin does. */
    private fun takenOverMeanwhile() {
        clock.now = start.plus(lease).plusSeconds(1)
        memory.changeExternalCallBehindTheStoresBack("job-1") {
            it.copy(attempts = it.attempts + 1, owner = "call-B", leaseUntil = clock.now.plus(lease), updatedAt = clock.now)
        }
    }

    private fun printSafe() = call("ex.PrintSafe", """{"jobId":"j1"}""")

    private fun assertStillB() {
        val stored = assertNotNull(record("job-1"), "call B's record is still there")
        assertEquals(ExternalCallStatus.IN_PROGRESS, stored.status, "call B's record is left alone")
        assertEquals(2, stored.attempts)
        assertEquals(clock.now.plus(lease), stored.leaseUntil)
    }

    @Test
    fun `a call whose lease was taken over does not overwrite the new owner's record with its success, nor report a success it cannot record`() {
        adapter.safeBehaviour = { takenOverMeanwhile() }

        val a = failure(printSafe())

        assertTrue(ExternalFailureMarkers.isRetryable(a.details), "the partner de-duplicates, the redelivery finds B's outcome: ${a.message}")
        assertStillB()
        assertTrue(metrics.calls.none { it.second == SagaMetrics.ExternalOutcome.SUCCEEDED }, metrics.calls.toString())
    }

    @Test
    fun `a call whose lease was taken over does not delete the new owner's record on a retryable failure, so no third call runs alongside it`() {
        adapter.safeBehaviour = { takenOverMeanwhile(); throw RetryableExternalException("partner busy") }

        val a = failure(printSafe())

        assertTrue(ExternalFailureMarkers.isRetryable(a.details))
        assertStillB()
        adapter.safeBehaviour = {}
        val c = failure(printSafe()) // B is still within its lease
        assertTrue(ExternalFailureMarkers.isRetryable(c.details), "B is in flight: come back later")
        assertEquals(1, adapter.bodies.get(), "the third call did not run its body alongside B")
    }

    @Test
    fun `a call whose lease was taken over does not mark the new owner's record FAILED`() {
        adapter.safeBehaviour = { takenOverMeanwhile(); throw IllegalStateException("printer refused") }

        val a = failure(printSafe())

        assertTrue("printer refused" in a.message, "the failure is still thrown: ${a.message}")
        assertStillB()
    }

    @Test
    fun `a call whose lease was taken over does not mark the new owner's record UNKNOWN when its adapter reports an unknown outcome`() {
        adapter.safeBehaviour = { takenOverMeanwhile(); throw ExternalCallUnknownOutcomeException("the connection broke") }

        val a = failure(printSafe())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(a.details), a.message)
        assertStillB()
    }

    @Test
    fun `a call whose record was reset and inserted afresh meanwhile does not settle the new call's record (ABA)`() {
        adapter.behaviour = {
            // Inside call A's body: A's lease runs out, B takes the record over and deletes it on a retryable failure, and C
            // inserts it afresh: attempts is 1 again, as it was when A began.
            clock.now = start.plus(lease).plusSeconds(1)
            memory.removeExternalCallBehindTheStoresBack("job-1")
            insertCall(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = clock.now, leaseUntil = clock.now.plus(lease), owner = "call-C"))
            ExReceipt(it.jobId, 1)
        }

        val a = failure(print())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(a.details), "A no longer owns the record: ${a.message}")
        val stored = assertNotNull(record("job-1"))
        assertEquals(ExternalCallStatus.IN_PROGRESS, stored.status, "call C's record is left alone")
        assertEquals("call-C", stored.owner)
        assertTrue(metrics.calls.none { it.second == SagaMetrics.ExternalOutcome.SUCCEEDED }, metrics.calls.toString())
    }

    @Test
    fun `a call whose record another call turned UNKNOWN reports the unknown outcome, not a success it cannot record`() {
        adapter.behaviour = {
            memory.changeExternalCallBehindTheStoresBack("job-1") {
                it.copy(status = ExternalCallStatus.UNKNOWN, leaseUntil = null)
            } // A's lease ran out and call B gave it up as UNKNOWN (and told its caller so)
            ExReceipt(it.jobId, 1)
        }

        val a = failure(print())

        assertTrue(ExternalFailureMarkers.isUnknownOutcome(a.details), a.message)
        assertEquals(ExternalCallStatus.UNKNOWN, record("job-1")!!.status, "B's verdict stays")
        assertTrue(metrics.calls.none { it.second == SagaMetrics.ExternalOutcome.SUCCEEDED }, metrics.calls.toString())
    }

    @Test
    fun `an in-progress record within its lease asks the caller to come back`() {
        insertCall(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))

        val busy = failure(print())

        assertTrue(ExternalFailureMarkers.isRetryable(busy.details), "another call is in flight: transient")
        assertEquals(0, adapter.bodies.get())
        assertEquals(ExternalCallStatus.IN_PROGRESS, record("job-1")!!.status)
    }

    @Test
    fun `with externalIdempotent the expired call is retried`() {
        insertCall(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
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

    @Test
    fun `an invoker without the interceptor refuses to start while the registry holds an idempotent handler`() {
        val e = assertFailsWith<IllegalStateException> { HandlerInvoker(registry, codec).verifyIdempotency() }
        assertTrue("ExPrinterAdapter." in e.message!! && "@IdempotentHandler" in e.message!! && "snodra.messaging.external.calls.enabled" in e.message!!, e.message)

        invoker.verifyIdempotency() // with the interceptor it is fine
        HandlerInvoker(HandlerRegistry(resolver), codec).verifyIdempotency() // and so is a registry without idempotent handlers
    }

    // ---- with the saga relay -----------------------------------------------------------------------------------------

    private class Harness(val relay: SagaDispatchRelay, val runtime: SagaRuntime, val charge: ChargeAdapter)

    private fun harness(): Harness {
        val charge = ChargeAdapter()
        val chargeRegistry = HandlerRegistry(MessageTypeResolver("rl")).also { it.register(charge) }
        val chargeInvoker = HandlerInvoker(chargeRegistry, codec, interceptor())
        val sagaResolver = MessageTypeResolver("rl")
        val runtime = SagaRuntime(
            types = SagaModelScanner(sagaResolver, codec).scan(listOf(RlSaga::class)), ports = store.sagas, tx = tx, codec = codec, typeResolver = sagaResolver,
            beans = { error("no beans") }, clock = clock,
            retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        val dispatcher = LocalEventDispatcher(chargeRegistry, chargeInvoker, store, runtime)
        val relay = SagaDispatchRelay(
            store.sagas.dispatch, tx, LocalRawCommandSender(chargeInvoker), OutboxEventBus(store.outbox, codec, sagaResolver, TraceHeaders(null, null)), dispatcher, codec, sagaResolver,
            clock, RelaySettings(maxAttempts = 3, initialBackoff = Duration.ofSeconds(1)),
        )
        val start = RlStart("o1")
        runtime.process("rl.Start", codec.encode(start), MessageContext("e-1", "rl.Start", "corr-1", null, emptySet(), null, mapOf("id" to "e-1", "type" to "rl.Start")))
        return Harness(relay, runtime, charge)
    }

    private fun dispatchEntry() = memory.dispatchEntries().single { it.kind == DispatchKind.COMMAND }

    @Test
    fun `a crash between the send and marking it dispatched re-sends with the same key and the adapter runs its body once`() {
        val h = harness()
        val key = dispatchEntry().id
        store.failNextMark.set(true)

        h.relay.pollOnce()
        assertNull(dispatchEntry().dispatchedAt, "the mark was lost")
        assertEquals(1, h.charge.bodies.get())
        h.relay.pollOnce()

        assertEquals(1, h.charge.bodies.get(), "the same key: the stored reply, the body did not run again")
        assertNotNull(dispatchEntry().dispatchedAt)
        assertEquals(ExternalCallStatus.SUCCEEDED, record(key)!!.status)
        assertEquals(listOf(key), memory.externalCalls().map { it.id })
    }

    @Test
    fun `the relay maps UNKNOWN_OUTCOME to SagaCommandFailed(reason = UNKNOWN_OUTCOME)`() {
        val h = harness()
        val entry = dispatchEntry()
        insertCall(ExternalCallRecord(entry.id, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = start, leaseUntil = start.plus(lease)))
        clock.now = start.plus(lease).plusSeconds(1)

        h.relay.pollOnce()

        val saga = memory.sagas().single()
        assertEquals(listOf("rl.Charge:UNKNOWN_OUTCOME"), saga.stateDoc.getList("failures", String::class.java))
        assertEquals(0, h.charge.bodies.get(), "an unknown outcome is not called again")
        assertNotNull(dispatchEntry().failedAt)
        assertEquals(0, memory.dispatchEntries().count { it.failedAt == null && it.dispatchedAt == null }, "nothing left pending")
    }
}
