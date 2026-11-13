package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.SAGAS
import io.damoreira.kraken.messaging.SAGA_DISPATCH
import io.damoreira.kraken.messaging.SAGA_FAILURES
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.external.RetryableExternalException
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.RequiresAuthentication
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.FaultyMessageStore
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.saga.SagaCommandFailed.Reason
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import kotlinx.coroutines.currentCoroutineContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@MessageType("rl.Start")
data class RlStart(val orderId: String) : Event

@MessageType("rl.StartAndEnd")
data class RlStartAndEnd(val orderId: String) : Event

@MessageType("rl.Announced")
data class RlAnnounced(val orderId: String) : Event

@MessageType("rl.Charge")
data class RlCharge(val ref: String) : Command<Unit>

@MessageType("rl.Secured")
data class RlSecured(val ref: String) : Command<Unit>

@MessageType("rl.Boom")
data class RlBoom(val ref: String) : Command<Unit>

@MessageType("rl.Busy")
data class RlBusy(val ref: String) : Command<Unit>

/** Adapters of the relay tests: an open one, one that needs a caller, one that fails and one that asks for a retry. */
class RlAdapter {
    val charged = CopyOnWriteArrayList<Pair<String, MessageContext>>()
    val busyCalls = CopyOnWriteArrayList<String>()

    @CommandHandler fun handle(c: RlCharge, ctx: MessageContext) { charged += c.ref to ctx }

    @RequiresAuthentication @CommandHandler fun handle(c: RlSecured) = Unit

    @CommandHandler fun handle(c: RlBoom): Unit = throw IllegalStateException("printer on fire")

    @CommandHandler fun handle(c: RlBusy) {
        busyCalls += c.ref
        throw RetryableExternalException("partner is busy")
    }
}

/** Sends a charge when an order starts and remembers the command failures it is told about. */
@Saga(name = "rl.Order")
class RlSaga {
    var orderId: String? = null
    var failures: MutableList<String> = mutableListOf()

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RlStart, saga: SagaLifecycle, commands: SagaCommands) {
        orderId = e.orderId
        saga.associateWith("sagaId", saga.sagaId)
        commands.send(RlCharge(e.orderId))
    }

    /** Sends and publishes, then ends in the same handling: its entries must still go out. */
    @StartSaga
    @EndSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RlStartAndEnd, commands: SagaCommands) {
        orderId = e.orderId
        commands.send(RlCharge("ended-${e.orderId}"))
        commands.publish(RlAnnounced(e.orderId), "key-${e.orderId}")
    }

    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed) {
        failures += "${e.commandType}:${e.reason}"
    }
}

/** An ordinary `@EventHandler` for the library's failure event that fails its first [failFirst] calls, as a handler with a bug or an outage does. */
class RlFailureWatcher {
    val calls = java.util.concurrent.atomic.AtomicInteger()
    val seen = CopyOnWriteArrayList<String>()
    @Volatile var failFirst = 0

    @io.damoreira.kraken.messaging.handler.EventHandler
    fun on(e: SagaCommandFailed) {
        if (calls.incrementAndGet() <= failFirst) throw IllegalStateException("watcher is broken")
        seen += "${e.commandType}:${e.reason}"
    }
}

@MessageType("rl.QuietStart")
data class RlQuietStart(val orderId: String) : Event

@MessageType("rl.DeafStart")
data class RlDeafStart(val orderId: String) : Event

/** Handles `SagaCommandFailed` by `sagaId` but never associates itself with its own id: the runtime routes the notice by id. */
@Saga(name = "rl.Quiet")
class RlQuietSaga {
    var orderId: String? = null
    var failures: MutableList<String> = mutableListOf()

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RlQuietStart) {
        orderId = e.orderId
    }

    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed) {
        failures += "${e.commandType}:${e.reason}"
    }
}

@MessageType("rl.GrumpyStart")
data class RlGrumpyStart(val orderId: String) : Event

/** Handles `SagaCommandFailed` by `sagaId` with a handler that always fails: the notice is parked for this saga type. */
@Saga(name = "rl.Grumpy")
class RlGrumpySaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RlGrumpyStart, saga: SagaLifecycle) {
        orderId = e.orderId
        saga.associateWith("sagaId", saga.sagaId)
    }

    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed): Unit = throw IllegalStateException("cannot cope with ${e.reason}")
}

/** A saga type with no handler for `SagaCommandFailed`. */
@Saga(name = "rl.Deaf")
class RlDeafSaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RlDeafStart) {
        orderId = e.orderId
    }
}

/** A sender the test scripts: every call is recorded, the outcome comes from [script]. */
private class ScriptedSender : RawCommandSender {
    class Call(val type: String, val payload: String, val headers: Map<String, String>, val correlationId: String?, val retry: RetryPolicy? = null)

    val calls = CopyOnWriteArrayList<Call>()
    @Volatile var script: (Call) -> RawOutcome = { RawOutcome.Success }
    /** What [sendTimeoutFor] answers for a type; null keeps the relay's send timeout (a local handler). */
    @Volatile var timeoutFor: ((String) -> Duration)? = null

    override fun sendTimeoutFor(type: String, sendTimeout: Duration): Duration = timeoutFor?.invoke(type) ?: sendTimeout

    override suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions): RawOutcome {
        val call = Call(type, String(payload, Charsets.UTF_8), options.headers, currentCoroutineContext()[CorrelationContext]?.correlationId, options.retry)
        calls += call
        return script(call)
    }

    fun keys() = calls.map { it.headers["idempotency-key"] }
}

private class RelayMetrics : SagaMetrics {
    val unhandled = CopyOnWriteArrayList<Pair<String, Reason>>()
    override fun commandFailureUnhandled(commandType: String, reason: Reason) { unhandled += commandType to reason }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaDispatchRelayTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MongoTemplate
    private lateinit var store: FaultyMessageStore
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rl")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private val settings = RelaySettings(
        pollInterval = Duration.ofMillis(50), lease = Duration.ofSeconds(30), maxAttempts = 3,
        initialBackoff = Duration.ofSeconds(1), multiplier = 2.0, maxBackoff = Duration.ofMinutes(5),
    )
    private lateinit var adapter: RlAdapter
    private lateinit var watcher: RlFailureWatcher
    private lateinit var registry: HandlerRegistry
    private lateinit var runtime: SagaRuntime
    private lateinit var sender: ScriptedSender
    private lateinit var metrics: RelayMetrics
    private val relays = mutableListOf<SagaDispatchRelay>()

    @BeforeAll
    fun start() {
        mongoContainer.start()
        client = MongoClients.create(mongoContainer.connectionString + "/?directConnection=true")
        val factory = SimpleMongoClientDatabaseFactory(client, "saga_relay_test")
        mongo = MongoTemplate(factory)
        store = FaultyMessageStore(mongo)
        tx = TransactionTemplate(store.transactions)
        listOf("processed_events", "saga_failures", "outbox").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() {
        client.close()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "saga_dispatch_leases", "deadlines", "saga_failures", "processed_events", "outbox").forEach { mongo.remove(Query(), it) }
        clock.now = start
        adapter = RlAdapter()
        watcher = RlFailureWatcher()
        registry = HandlerRegistry(resolver).also { it.register(adapter) }
        runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RlSaga::class, RlQuietSaga::class, RlDeafSaga::class, RlGrumpySaga::class)), ports = store.sagas, tx = tx, codec = codec, typeResolver = resolver,
            beans = { error("no beans") }, clock = clock, retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        sender = ScriptedSender()
        metrics = RelayMetrics()
    }

    @AfterEach
    fun tearDown() {
        relays.forEach { it.stop() }
        relays.clear()
        SecurityContextHolder.clearContext()
    }

    private fun relay(
        commands: RawCommandSender = sender, stage: SagaStage = runtime, settings: RelaySettings = this.settings,
    ): SagaDispatchRelay {
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), store, stage)
        val events = OutboxEventBus(store.outbox, codec, resolver, TraceHeaders(null, null))
        return SagaDispatchRelay(store.sagas.dispatch, tx, commands, events, dispatcher, codec, resolver, clock, settings, metrics).also { relays += it }
    }

    private fun deliver(event: Event, id: String, correlationId: String = "corr-$id") {
        val type = resolver.typeNameOf(event::class)
        runtime.process(type, codec.encode(event), MessageContext(id, type, correlationId, null, emptySet(), null, mapOf("id" to id, "type" to type)))
    }

    private fun sagaOf(orderId: String) = mongo.findAll(SagaDocument::class.java, SAGAS).single { it.stateDoc.getString("orderId") == orderId }
    private fun entries(sagaId: String) = mongo.find(Query(Criteria.where("sagaId").`is`(sagaId)).with(Sort.by("seq")), SagaDispatchDocument::class.java, SAGA_DISPATCH)
    private fun entry(id: String) = mongo.findById(id, SagaDispatchDocument::class.java, SAGA_DISPATCH)!!
    private fun failuresOf(orderId: String): List<String> = sagaOf(orderId).stateDoc.getList("failures", String::class.java) ?: emptyList()

    /** Records an entry as a saga transaction would have; [seq] decides its place in the saga's order. */
    private fun record(
        sagaId: String, seq: Long, type: String = "rl.Charge", payload: String = """{"ref":"r$seq"}""",
        kind: DispatchKind = DispatchKind.COMMAND, routingKey: String? = null,
    ): SagaDispatchDocument = SagaDispatchDocument(
        id = "$sagaId:$seq", sagaId = sagaId, seq = seq, kind = kind, type = type, payload = payload,
        headers = mapOf("correlation-id" to "corr-$sagaId"), routingKey = routingKey, createdAt = clock.now,
    ).also { mongo.insert(it, SAGA_DISPATCH) }

    @Test
    fun `entries of one saga are dispatched in seq order`() {
        record("s1", 3000)
        record("s1", 1000)
        record("s1", 2000)

        relay().pollOnce()

        assertEquals(listOf("s1:1000", "s1:2000", "s1:3000"), sender.keys())
        assertTrue(entries("s1").all { it.dispatchedAt != null })
    }

    @Test
    fun `an event the outbox refuses is given up as INVALID once, and the saga's later entries go on`() {
        // Recorded behind the store's back (the store's record refuses it): the relay's backstop.
        record("s1", 1000, type = "rl.Announced", payload = "[1,2]", kind = DispatchKind.EVENT)
        record("s1", 2000)

        relay().pollOnce()

        val refused = entry("s1:1000")
        assertNotNull(refused.failedAt)
        assertEquals(Reason.INVALID, refused.failureReason)
        assertNull(refused.noticePending, "no SagaCommandFailed for an event")
        assertEquals(0, mongo.count(Query(), "outbox"))
        assertEquals(listOf("rl.Announced" to Reason.INVALID), metrics.unhandled.toList(), "counted")
        assertEquals(listOf("s1:2000"), sender.keys(), "the later command was not held back")
        relay().pollOnce()
        assertEquals(listOf("s1:2000"), sender.keys(), "never retried")
        assertEquals(1, metrics.unhandled.size)
    }

    @Test
    fun `a pending notice of an event, left by a crash before the notice was cleared, is dropped without a SagaCommandFailed`() {
        // The backstop marked the event failed, then the instance crashed before clearNotice: the notice is pending and due.
        mongo.insert(
            SagaDispatchDocument(
                id = "s1:1000", sagaId = "s1", seq = 1000, kind = DispatchKind.EVENT, type = "rl.Announced", payload = "[1,2]",
                createdAt = clock.now, failedAt = clock.now, failureReason = Reason.INVALID, failureMessage = "not a JSON object",
                noticePending = true, noticeAttempts = 0, noticeNextAt = clock.now,
            ),
            SAGA_DISPATCH,
        )
        registry.register(watcher)

        relay().pollOnce()

        assertEquals(0, watcher.calls.get(), "SagaCommandFailed is about commands: never sent for an event")
        assertEquals(emptyList(), metrics.unhandled, "nothing to count: the event was counted when it was given up")
        assertNull(entry("s1:1000").noticePending, "the notice is cleared")
    }

    @Test
    fun `the oldest work is dispatched first when more sagas are due than one pass takes`() {
        val names = listOf("h", "g", "f", "e", "d", "c", "b", "a") // created in this order: the opposite of their ids
        names.forEachIndexed { i, name ->
            clock.now = start.plusSeconds(i.toLong())
            record(name, 1000)
        }
        clock.now = start.plusSeconds(100)
        val oneAtATime = settings.copy(parallelism = 1, batchSize = 3)
        val relay = relay(settings = oneAtATime)

        relay.pollOnce()
        assertEquals(listOf("h:1000", "g:1000", "f:1000"), sender.keys(), "the three oldest, oldest first")

        relay.pollOnce()
        assertEquals(listOf("h:1000", "g:1000", "f:1000", "e:1000", "d:1000", "c:1000"), sender.keys())
    }

    @Test
    fun `the relay sends with RetryPolicy NONE so the bus's own retry never stacks under the relay's`() {
        record("s1", 1000)

        relay().pollOnce()

        assertEquals(RetryPolicy.NONE, sender.calls.single().retry)
    }

    @Test
    fun `entries of different sagas are dispatched in parallel`() {
        record("a", 1000)
        record("b", 1000)
        val both = CountDownLatch(2)
        sender.script = {
            both.countDown()
            // Each send waits for the other saga's send: only parallel dispatch lets both through.
            if (both.await(5, TimeUnit.SECONDS)) RawOutcome.Success else RawOutcome.Transient("the other saga was never dispatched meanwhile")
        }

        relay().pollOnce()

        assertEquals(setOf("a:1000", "b:1000"), sender.keys().toSet())
        assertTrue(listOf("a", "b").all { entries(it).single().dispatchedAt != null })
    }

    private fun awaitTrue(what: String, timeoutMs: Long = 10_000, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!check()) {
            if (System.currentTimeMillis() > deadline) throw AssertionError("timed out waiting for: $what")
            Thread.sleep(20)
        }
    }

    @Test
    fun `a saga whose adapter call hangs does not hold back the dispatch of another saga`() {
        record("hung", 1000)
        val release = CountDownLatch(1)
        sender.script = { call ->
            if (call.headers["saga-id"] == "hung") release.await(30, TimeUnit.SECONDS)
            RawOutcome.Success
        }
        val relay = relay()
        try {
            relay.start()
            awaitTrue("the hung saga's send to start") { sender.keys().contains("hung:1000") }

            record("fine", 1000) // becomes due while the first send is still alive

            awaitTrue("the other saga to be dispatched meanwhile") { entry("fine:1000").dispatchedAt != null }
            assertNull(entry("hung:1000").dispatchedAt)
            assertEquals(listOf("hung:1000", "fine:1000"), sender.keys(), "the hung saga was not sent a second time either")
        } finally {
            release.countDown()
        }
        awaitTrue("the released send to be marked") { entry("hung:1000").dispatchedAt != null }
    }

    @Test
    fun `a send that outlives the send timeout is abandoned, its thread interrupted, and the entry retried later with its key`() {
        record("stuck", 1000)
        val interrupted = CountDownLatch(1)
        sender.script = {
            try {
                Thread.sleep(30_000) // a local adapter that never returns
            } catch (e: InterruptedException) {
                interrupted.countDown()
                throw e
            }
            RawOutcome.Success
        }
        val started = System.nanoTime()

        relay(settings = settings.copy(sendTimeout = Duration.ofMillis(300))).pollOnce()

        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(10), "the pass did not wait for the hung call")
        val stuck = entry("stuck:1000")
        assertNull(stuck.dispatchedAt)
        assertNull(stuck.failedAt, "a timeout is transient: whether the adapter acted is unknown, its idempotency key decides on the retry")
        assertEquals(1, stuck.attempts)
        assertTrue(stuck.nextAttemptAt!! > clock.now)
        assertTrue(interrupted.await(5, TimeUnit.SECONDS), "the abandoned call's thread was interrupted")
        assertEquals(null, mongo.findById("stuck", org.bson.Document::class.java, "saga_dispatch_leases"), "the saga's lease was released")
    }

    @Test
    fun `a send through the hub is not abandoned before the hub gave up on it, whatever the send timeout`() {
        record("remote", 1000)
        sender.timeoutFor = { Duration.ofSeconds(10) } // the hub's deadline for the type plus the guard
        sender.script = { Thread.sleep(800); RawOutcome.Success } // longer than the send timeout, within the hub's deadline

        relay(settings = settings.copy(sendTimeout = Duration.ofMillis(300))).pollOnce()

        val remote = entry("remote:1000")
        assertNotNull(remote.dispatchedAt, "the hub's answer was awaited: no second send while the hub may still run the first")
        assertEquals(0, remote.attempts)
        assertEquals(listOf("remote:1000"), sender.keys())
    }

    @Test
    fun `the lease of a saga is renewed while its send is alive, so another relay does not take it over`() {
        record("slow", 1000)
        val short = settings.copy(lease = Duration.ofMillis(600))
        val release = CountDownLatch(1)
        sender.script = { release.await(30, TimeUnit.SECONDS); RawOutcome.Success }
        val first = relay(settings = short)
        val second = relay(settings = short)
        try {
            first.start()
            awaitTrue("the send to start") { sender.calls.size == 1 }

            clock.now = clock.now.plus(Duration.ofHours(1)) // far past the lease the send started with
            awaitTrue("the lease to be renewed to the new time") { mongo.findById("slow", org.bson.Document::class.java, "saga_dispatch_leases")!!.getDate("until").toInstant().isAfter(clock.now) }

            second.pollOnce()
            assertEquals(1, sender.calls.size, "the live send still owns the saga: no second send of the same key")
        } finally {
            release.countDown()
        }
        awaitTrue("the entry to be marked") { entry("slow:1000").dispatchedAt != null }
    }

    @Test
    fun `a step in retry holds back that saga's later steps only`() {
        record("a", 1000, type = "rl.First")
        record("a", 2000, type = "rl.Second")
        record("b", 1000, type = "rl.Other")
        var firstFails = true
        sender.script = { if (it.type == "rl.First" && firstFails) RawOutcome.Transient("hub down") else RawOutcome.Success }
        val relay = relay()

        relay.pollOnce()

        assertEquals(setOf("rl.First", "rl.Other"), sender.calls.map { it.type }.toSet(), "the second step of saga a waits, saga b goes on")
        assertEquals(1, entry("a:1000").attempts)
        assertTrue(entry("a:1000").nextAttemptAt!! > clock.now)
        assertNull(entry("a:2000").dispatchedAt)
        assertNotNull(entry("b:1000").dispatchedAt)

        relay.pollOnce()
        assertEquals(2, sender.calls.size, "still backing off")

        firstFails = false
        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()
        assertEquals(listOf("rl.First", "rl.First", "rl.Second"), sender.calls.filter { it.headers["saga-id"] == "a" }.map { it.type }, "in order, the retried step first")
        assertNotNull(entry("a:2000").dispatchedAt)
    }

    @Test
    fun `an event entry reaches the outbox exactly once`() {
        record("s1", 1000, type = "rl.Announced", payload = """{"orderId":"o1"}""", kind = DispatchKind.EVENT, routingKey = "key-o1")
        val relay = relay()
        store.failNextMark.set(true) // the crash between inserting into the outbox and marking: one transaction, so neither survives

        relay.pollOnce()
        assertEquals(0, mongo.count(Query(), "outbox"), "the outbox insert rolled back with the failed mark")
        assertNull(entry("s1:1000").dispatchedAt)

        relay.pollOnce()
        relay.pollOnce()

        val outbox = mongo.findAll(org.bson.Document::class.java, "outbox").single()
        assertEquals("rl.Announced", outbox.getString("type"))
        assertEquals("key-o1", outbox.getString("key"))
        assertEquals("o1", outbox.get("payload", org.bson.Document::class.java).getString("orderId"))
        assertEquals("corr-s1", outbox.get("headers", org.bson.Document::class.java)["correlationId"])
        assertNotNull(entry("s1:1000").dispatchedAt)
    }

    @Test
    fun `a command carries idempotency-key sagaId-seq, saga-id and the correlation id of its cause`() {
        deliver(RlStart("o1"), "e-1", correlationId = "corr-cause")
        val saga = sagaOf("o1")

        relay().pollOnce()

        val call = sender.calls.single()
        assertEquals("rl.Charge", call.type)
        assertEquals("o1", codec.decodeTree(call.payload.toByteArray()).get("ref").asString())
        assertEquals("${saga.id}:${entries(saga.id).single().seq}", call.headers["idempotency-key"])
        assertEquals(saga.id, call.headers["saga-id"])
        assertEquals("corr-cause", call.correlationId)
    }

    @Test
    fun `a crash after the send and before marking re-sends with the same key`() {
        record("s1", 1000)
        val relay = relay()
        store.failNextMark.set(true)

        relay.pollOnce()
        assertNull(entry("s1:1000").dispatchedAt, "the mark failed")

        relay.pollOnce()

        assertEquals(listOf("s1:1000", "s1:1000"), sender.keys())
        assertNotNull(entry("s1:1000").dispatchedAt)
        relay.pollOnce()
        assertEquals(2, sender.calls.size, "marked dispatched: never sent again")
    }

    @Test
    fun `transient failures back off and become EXHAUSTED after the limit`() {
        deliver(RlStart("o1"), "e-1")
        val saga = sagaOf("o1")
        val key = "${saga.id}:${entries(saga.id).single().seq}"
        sender.script = { RawOutcome.Transient("hub down") }
        val relay = relay()

        relay.pollOnce()
        assertEquals(1, entry(key).attempts)
        relay.pollOnce()
        assertEquals(1, sender.calls.size, "no second attempt before the backoff passed")

        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()
        assertEquals(2, entry(key).attempts)
        assertNull(entry(key).failedAt)
        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()

        assertEquals(3, sender.calls.size)
        assertNotNull(entry(key).failedAt, "the limit of 3 attempts is reached")
        assertEquals(listOf("rl.Charge:EXHAUSTED"), failuresOf("o1"))
        relay.pollOnce()
        assertEquals(3, sender.calls.size, "given up: never sent again")
    }

    @Test
    fun `an adapter failure marked retryable backs off like a transient one`() {
        deliver(RlStart("o1"), "e-1")
        val sagaId = sagaOf("o1").id
        record(sagaId, 9000, type = "rl.Busy", payload = """{"ref":"b"}""")
        val relay = relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec)))

        relay.pollOnce()

        assertEquals(listOf("b"), adapter.busyCalls)
        assertEquals(1, entry("$sagaId:9000").attempts)
        assertNull(entry("$sagaId:9000").failedAt, "retried later, not failed")
        assertEquals(emptyList(), failuresOf("o1"))
    }

    @Test
    fun `NO_HANDLER, DENIED, INVALID and FAILED become SagaCommandFailed at once and a saga handling it by sagaId receives it`() {
        deliver(RlStart("o1"), "e-1")
        val sagaId = sagaOf("o1").id
        record(sagaId, 9001, type = "rl.Nobody", payload = """{"ref":"n"}""")
        record(sagaId, 9002, type = "rl.Secured", payload = """{"ref":"s"}""")
        record(sagaId, 9003, type = "rl.Charge", payload = """{"bad":1}""")
        record(sagaId, 9004, type = "rl.Boom", payload = """{"ref":"b"}""")
        record(sagaId, 9005, type = "rl.Charge", payload = """{"ref":"after"}""")
        val relay = relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec)))

        relay.pollOnce()

        assertEquals(
            listOf("rl.Nobody:NO_HANDLER", "rl.Secured:DENIED", "rl.Charge:INVALID", "rl.Boom:FAILED"),
            failuresOf("o1"),
        )
        assertTrue((9001..9004).all { entry("$sagaId:$it").failedAt != null })
        assertEquals(listOf("o1", "after"), adapter.charged.map { it.first }, "a failed step does not hold back the saga's later steps")
        relay.pollOnce()
        assertEquals(2, adapter.charged.size, "failed steps are not sent again")
        assertEquals(4, failuresOf("o1").size, "nor their failure events delivered again")
    }

    @Test
    fun `a command whose outcome is known is never re-sent while its failure notice is retried`() {
        deliver(RlStart("o1"), "e-1")
        val saga = sagaOf("o1")
        val key = "${saga.id}:${entries(saga.id).single().seq}"
        sender.script = { RawOutcome.Permanent(Reason.FAILED, "printer on fire") }
        watcher.failFirst = 2
        registry.register(watcher)
        val relay = relay()

        relay.pollOnce()
        assertEquals(1, sender.calls.size)
        assertNotNull(entry(key).failedAt, "the permanent outcome is persisted before anything else can fail")
        assertEquals(1, watcher.calls.get())

        relay.pollOnce()
        assertEquals(1, watcher.calls.get(), "the notice waits for its own backoff")
        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()
        assertEquals(2, watcher.calls.get(), "the notice is retried")
        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()
        assertEquals(3, watcher.calls.get())
        assertEquals(listOf("rl.Charge:FAILED"), watcher.seen.toList(), "the ordinary handler finally got it")
        assertEquals(listOf("rl.Charge:FAILED"), failuresOf("o1"), "the saga got it once, however often the notice was retried")

        clock.now = clock.now.plus(Duration.ofHours(1))
        relay.pollOnce()
        assertEquals(1, sender.calls.size, "the command ran once: its outcome was known")
        assertEquals(3, watcher.calls.get(), "the notice was delivered: nothing is pending")
        assertEquals(0, mongo.count(Query(Criteria.where("noticePending").`is`(true)), "saga_dispatch"))
    }

    @Test
    fun `a failure notice that keeps failing is given up after the relay's attempts, logged at error and counted`() {
        deliver(RlStart("o1"), "e-1")
        val saga = sagaOf("o1")
        sender.script = { RawOutcome.Permanent(Reason.FAILED, "printer on fire") }
        watcher.failFirst = Int.MAX_VALUE
        registry.register(watcher)
        val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start(); log.addAppender(it) }
        val relay = relay()

        try {
            repeat(5) {
                relay.pollOnce()
                clock.now = clock.now.plus(Duration.ofHours(1))
            }
        } finally {
            log.detachAppender(appender)
        }

        assertEquals(settings.maxAttempts, watcher.calls.get(), "bounded: 3 attempts in all")
        assertEquals(1, sender.calls.size)
        assertEquals(listOf("rl.Charge" to Reason.FAILED), metrics.unhandled)
        val giveUp = appender.list.single { it.level == Level.ERROR && saga.id in it.formattedMessage }.formattedMessage
        assertTrue("the saga may have applied it" in giveUp && "never heard" !in giveUp, "the saga stage runs even when an ordinary handler fails: $giveUp")
        assertEquals(listOf("rl.Charge:FAILED"), failuresOf("o1"), "and so the saga did hear of it")
        assertEquals(0, mongo.count(Query(Criteria.where("noticePending").`is`(true)), "saga_dispatch"))
    }

    @Test
    fun `an unhandled SagaCommandFailed is logged at error and counted`() {
        record("lost", 1000, type = "rl.Nobody")
        val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start(); log.addAppender(it) }
        val relay = relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec)), stage = SagaStage.NONE)

        try {
            relay.pollOnce()
        } finally {
            log.detachAppender(appender)
        }

        assertEquals(listOf("rl.Nobody" to Reason.NO_HANDLER), metrics.unhandled)
        val error = appender.list.single { it.level == Level.ERROR }
        assertTrue("lost:1000" in error.formattedMessage && "NO_HANDLER" in error.formattedMessage, error.formattedMessage)
        assertTrue("nothing in this service handles SagaCommandFailed" in error.formattedMessage, error.formattedMessage)
        assertNotNull(entry("lost:1000").failedAt)
    }

    @Test
    fun `a SagaCommandFailed whose saga handler fails is parked for that saga, not logged and counted as unhandled`() {
        deliver(RlGrumpyStart("g1"), "e-g1")
        val saga = sagaOf("g1")
        record(saga.id, 9001, type = "rl.Nobody", payload = """{"ref":"n"}""")
        val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start(); log.addAppender(it) }

        try {
            relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec))).pollOnce()
        } finally {
            log.detachAppender(appender)
        }

        val parked = mongo.findAll(SagaFailureDocument::class.java, SAGA_FAILURES).single()
        assertEquals("rl.Grumpy", parked.sagaType)
        assertEquals(emptyList(), metrics.unhandled, "it reached the saga: the park is what a person looks at")
        assertEquals(emptyList(), appender.list.filter { it.level == Level.ERROR }.map { it.formattedMessage })
        assertEquals(0, mongo.count(Query(Criteria.where("noticePending").`is`(true)), "saga_dispatch"))
    }

    @Test
    fun `a store failure after a failure notice was delivered is logged as such and the saga's later entries go on`() {
        record("n", 1000, type = "rl.Nobody")
        record("n", 2000)
        store.failNextNoticeClear.set(true)
        val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start(); log.addAppender(it) }

        try {
            relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec))).pollOnce()
        } finally {
            log.detachAppender(appender)
        }

        assertNotNull(entry("n:1000").failedAt)
        assertNotNull(entry("n:2000").dispatchedAt, "the failed entry is settled: the saga's next entry went out in the same pass")
        val warnings = appender.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
        assertTrue(warnings.any { "n:1000" in it && "could not be settled" in it }, warnings.toString())
        assertTrue(warnings.none { "Dispatching saga" in it }, "nothing about the dispatch failed: $warnings")
        assertEquals(true, entry("n:1000").noticePending, "the notice is delivered again when its claim expires")
    }

    @Test
    fun `SagaCommandFailed reaches a saga that handles it by sagaId without associating itself with its own id`() {
        deliver(RlQuietStart("q1"), "e-q1")
        val sagaId = sagaOf("q1").id
        assertEquals(setOf(Association("orderId", "q1")), mongo.findById(sagaId, SagaDocument::class.java, SAGAS)!!.associations.toSet(), "the saga never associated with its own id")
        record(sagaId, 9001, type = "rl.Nobody", payload = """{"ref":"n"}""")

        relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec))).pollOnce()

        assertEquals(listOf("rl.Nobody:NO_HANDLER"), failuresOf("q1"))
        assertEquals(emptyList(), metrics.unhandled)
    }

    @Test
    fun `SagaCommandFailed for a saga that has ended, or whose type has no handler, is logged at error and counted`() {
        deliver(RlStartAndEnd("o9"), "e-9")
        val ended = sagaOf("o9")
        assertTrue(ended.ended)
        deliver(RlDeafStart("d1"), "e-d1")
        val deaf = sagaOf("d1")
        record(ended.id, 9001, type = "rl.Nobody", payload = """{"ref":"n"}""")
        record(deaf.id, 9001, type = "rl.Nobody", payload = """{"ref":"n"}""")
        val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().also { it.start(); log.addAppender(it) }
        val relay = relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec)))

        try {
            relay.pollOnce()
        } finally {
            log.detachAppender(appender)
        }

        assertEquals(listOf("rl.Nobody" to Reason.NO_HANDLER, "rl.Nobody" to Reason.NO_HANDLER), metrics.unhandled)
        val errors = appender.list.filter { it.level == Level.ERROR }.map { it.formattedMessage }
        assertEquals(2, errors.size, errors.toString())
        assertTrue(errors.any { "${ended.id}:9001" in it } && errors.any { "${deaf.id}:9001" in it }, errors.toString())
        assertTrue(errors.all { "no live instance of a saga took it" in it }, errors.toString())
        assertNotNull(entry("${ended.id}:9001").failedAt)
    }

    @Test
    fun `entries recorded before a saga ended are still dispatched`() {
        deliver(RlStartAndEnd("o9"), "e-9")
        val saga = sagaOf("o9")
        assertTrue(saga.ended)

        relay().pollOnce()

        assertEquals(listOf("rl.Charge"), sender.calls.map { it.type })
        assertEquals("ended-o9", adapterlessRef(sender.calls.single().payload))
        assertEquals("rl.Announced", mongo.findAll(org.bson.Document::class.java, "outbox").single().getString("type"))
        assertTrue(entries(saga.id).all { it.dispatchedAt != null })
    }

    private fun adapterlessRef(payload: String) = codec.decodeTree(payload.toByteArray()).get("ref").asString()

    @Test
    fun `two relays never dispatch one saga's entries concurrently`() {
        record("s1", 1000)
        record("s1", 2000)
        val inFlight = CountDownLatch(1)
        val release = CountDownLatch(1)
        sender.script = { call ->
            if (call.headers["idempotency-key"] == "s1:1000") {
                inFlight.countDown()
                release.await(10, TimeUnit.SECONDS)
            }
            RawOutcome.Success
        }
        val first = relay()
        val second = relay()
        val running = Thread { first.pollOnce() }.also { it.start() }
        assertTrue(inFlight.await(10, TimeUnit.SECONDS), "the first relay is inside the first step")

        second.pollOnce()

        assertEquals(listOf("s1:1000"), sender.keys(), "the second relay must not send the later step past the first")
        release.countDown()
        running.join(10_000)
        assertEquals(listOf("s1:1000", "s1:2000"), sender.keys())
        assertEquals(0, mongo.count(Query(), "saga_dispatch_leases"), "the lease is released when the saga's turn is done")
    }

    @Test
    fun `a saga command reaches a handler without @RequiresAuthentication and is DENIED by one that requires it`() {
        deliver(RlStart("o1"), "e-1")
        val sagaId = sagaOf("o1").id
        record(sagaId, 9002, type = "rl.Secured", payload = """{"ref":"s"}""")
        // Whoever signed in on the thread that runs the relay is not the saga's caller.
        SecurityContextHolder.getContext().authentication =
            JwtAuthenticationToken(Jwt.withTokenValue("the-user-token").header("alg", "none").subject("user-7").build())
        val relay = relay(commands = LocalRawCommandSender(HandlerInvoker(registry, codec)))

        relay.pollOnce()

        assertEquals(listOf("o1"), adapter.charged.map { it.first }, "the open handler ran")
        val ctx = adapter.charged.single().second
        assertNull(ctx.userId, "no user: ${ctx.userId}")
        assertNull(ctx.authorization)
        assertEquals(listOf("rl.Secured:DENIED"), failuresOf("o1"))
    }
}
