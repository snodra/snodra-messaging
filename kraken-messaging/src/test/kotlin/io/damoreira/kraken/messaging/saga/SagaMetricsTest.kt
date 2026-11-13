package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.mongoStore
import io.damoreira.kraken.messaging.SAGA_DISPATCH
import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.deadline.DeadlineScheduler
import io.damoreira.kraken.messaging.deadline.DeadlineSettings
import io.damoreira.kraken.messaging.deadline.DlPlaced
import io.damoreira.kraken.messaging.deadline.DlSaga
import io.damoreira.kraken.messaging.deadline.DlSwitch
import io.damoreira.kraken.messaging.external.ExPrint
import io.damoreira.kraken.messaging.external.ExPrinterAdapter
import io.damoreira.kraken.messaging.external.ExternalCallSettings
import io.damoreira.kraken.messaging.external.IdempotencyInterceptor
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The Micrometer meters of the saga runtime, relay, deadline scheduler and idempotent adapters against a real runtime on
 * MongoDB: each meter of the spec's Observability list moves on the matching action. The first tests drive the hooks alone.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaMetricsTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private lateinit var txManager: MongoTransactionManager
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")
    private val start = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = SettableClock(start)
    private lateinit var registry: SimpleMeterRegistry
    private lateinit var store: MessageStore
    private lateinit var metrics: MicrometerSagaMetrics

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_metrics_test")
        mongo = MongoTemplate(factory)
        txManager = MongoTransactionManager(factory)
        tx = TransactionTemplate(txManager)
        store = mongoStore(mongo)
        listOf("processed_events", "saga_failures", "outbox", "external_calls").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "saga_dispatch_leases", "deadlines", "saga_failures", "processed_events", "outbox", "external_calls").forEach { mongo.remove(Query(), it) }
        RtGate.reset()
        DlSwitch.broken.set(false)
        clock.now = start
        registry = SimpleMeterRegistry()
        metrics = MicrometerSagaMetrics(registry, listOf("rt.Order", "dl.Charge"))
    }

    private fun runtime(vararg classes: kotlin.reflect.KClass<*>) = SagaRuntime(
        types = SagaModelScanner(resolver, codec).scan(classes.toList()), ports = store.sagas, tx = tx, codec = codec, typeResolver = resolver,
        beans = { type -> if (type == RtBean::class) RtBean("the-bean") else error("no bean $type") },
        clock = clock,
        retry = SagaRetry(RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(2), multiplier = 1.0, maxBackoff = Duration.ofMillis(5))),
        metrics = metrics,
    )

    private fun deliver(on: SagaRuntime, event: Event, id: String = UUID.randomUUID().toString()) {
        val type = resolver.typeNameOf(event::class)
        on.process(type, codec.encode(event), MessageContext(id, type, "corr-$id", null, emptySet(), null, mapOf("id" to id, "type" to type)))
    }

    private fun counter(name: String, vararg tags: String) = registry.find(name).tags(*tags).counter()?.count()
    private fun gauge(name: String, vararg tags: String) = registry.find(name).tags(*tags).gauge()?.value()

    @Test
    fun `the saga lifecycle and event outcomes are counted`() {
        val runtime = runtime(RtOrderSaga::class)

        deliver(runtime, RtOrderPlaced("o1"))
        deliver(runtime, RtBump("nobody", "x")) // no instance, no start handler: a broken association shows up here
        deliver(runtime, RtFinish("o1"))

        assertEquals(1.0, counter("kraken.sagas.started", "type", "rt.Order"))
        assertEquals(1.0, counter("kraken.sagas.ended", "type", "rt.Order"))
        assertEquals(2.0, counter("kraken.saga.events", "type", "rt.Order", "outcome", "handled"))
        assertEquals(1.0, counter("kraken.saga.events", "type", "rt.Order", "outcome", "no_match"))
    }

    @Test
    fun `a version conflict and a parked event are counted`() {
        val runtime = runtime(RtOrderSaga::class)
        deliver(runtime, RtOrderPlaced("o2"))
        RtGate.barrier = CyclicBarrier(2)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val both = listOf(RtBump("o2", "x"), RtBump("o2", "y")).map { e -> pool.submit { deliver(runtime, e) } }
            both.forEach { it.get(20, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
        assertEquals(true, (counter("kraken.saga.events", "type", "rt.Order", "outcome", "conflict") ?: 0.0) >= 1.0)

        val broken = runtime(RtBrokenSaga::class)
        deliver(broken, RtOrderPlaced("o3"))
        assertEquals(1.0, counter("kraken.saga.events", "type", "rt.Broken", "outcome", "parked"))
    }

    @Test
    fun `deadlines fired and overdue are metered`() {
        val runtime = runtime(DlSaga::class)
        deliver(runtime, DlPlaced("d1", seconds = 60))
        val scheduler = DeadlineScheduler(store.sagas.deadlines, runtime, clock, DeadlineSettings(pollInterval = Duration.ofMillis(50), lease = Duration.ofSeconds(30)), metrics)

        clock.now = start.plusSeconds(61)
        DlSwitch.broken.set(true)
        scheduler.pollOnce()
        assertEquals(1.0, gauge("kraken.deadlines.due", "name", "charge-timeout"), "overdue and not fired")
        assertNull(counter("kraken.deadlines.fired", "name", "charge-timeout"))

        DlSwitch.broken.set(false)
        clock.now = clock.now.plusSeconds(31) // the lease of the failed attempt expired
        scheduler.pollOnce()
        assertEquals(1.0, counter("kraken.deadlines.fired", "name", "charge-timeout"))
        assertEquals(0.0, gauge("kraken.deadlines.due", "name", "charge-timeout"), "the gauge goes back to zero when nothing is overdue")
    }

    @Test
    fun `the pending dispatch and active saga gauges follow the stored state`() {
        val runtime = runtime(RtOrderSaga::class)
        val sampler = SagaGaugeSampler(store.sagas, metrics)
        assertEquals(0.0, gauge("kraken.sagas.active", "type", "rt.Order"), "a known saga type has a gauge before its first instance")

        deliver(runtime, RtOrderPlaced("o4")) // records a command and an event
        deliver(runtime, RtOrderPlaced("o5"))
        sampler.sampleOnce()
        assertEquals(2.0, gauge("kraken.sagas.active", "type", "rt.Order"))
        assertEquals(4.0, gauge("kraken.saga.dispatch.pending"))

        deliver(runtime, RtFinish("o4"))
        mongo.updateMulti(Query(), org.springframework.data.mongodb.core.query.Update().set("dispatchedAt", start), SagaDispatchDocument::class.java, SAGA_DISPATCH)
        sampler.sampleOnce()
        assertEquals(1.0, gauge("kraken.sagas.active", "type", "rt.Order"))
        assertEquals(0.0, gauge("kraken.saga.dispatch.pending"), "dispatched entries are not pending")
    }

    @Test
    fun `an unhandled command failure is counted by command type and reason`() {
        val runtime = runtime(RtOrderSaga::class)
        val handlers = HandlerRegistry(resolver) // nobody handles rt.DoIt
        val invoker = HandlerInvoker(handlers, codec)
        val relay = SagaDispatchRelay(
            store.sagas.dispatch, tx, LocalRawCommandSender(invoker), OutboxEventBus(store.outbox, codec, resolver, TraceHeaders(null, null)),
            LocalEventDispatcher(handlers, invoker, store, runtime), codec, resolver, clock,
            RelaySettings(pollInterval = Duration.ofMillis(50), maxAttempts = 2), metrics,
        )
        deliver(runtime, RtOrderPlaced("o6"))

        relay.pollOnce()

        assertEquals(1.0, counter("kraken.saga.command.failures", "type", "rt.DoIt", "reason", "NO_HANDLER"))
    }

    @Test
    fun `adapter calls are counted by handler and outcome`() {
        val adapter = ExPrinterAdapter()
        val handlers = HandlerRegistry(MessageTypeResolver("ex")).also { it.register(adapter) }
        val interceptor = IdempotencyInterceptor(store, codec, clock, ExternalCallSettings(Duration.ofMinutes(5)), metrics)
        val invoker = HandlerInvoker(handlers, codec, interceptor)
        val ctx = MessageContext("m-1", "ex.Print", "corr-1", null, emptySet(), null, mapOf("idempotency-key" to "k-1"))

        runBlocking {
            invoker.invokeCommand("ex.Print", codec.encode(ExPrint("j1", "hi")), ctx, null)
            invoker.invokeCommand("ex.Print", codec.encode(ExPrint("j1", "hi")), ctx, null)
        }

        assertEquals(1.0, counter("kraken.external.calls", "handler", "ExPrinterAdapter.handle", "outcome", "succeeded"))
        assertEquals(1.0, counter("kraken.external.calls", "handler", "ExPrinterAdapter.handle", "outcome", "replayed"))
    }
}
