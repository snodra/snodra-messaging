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
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

private class RecordingMetrics : SagaMetrics {
    val started = CopyOnWriteArrayList<String>()
    val ended = CopyOnWriteArrayList<String>()
    val events = CopyOnWriteArrayList<Pair<String, SagaMetrics.Outcome>>()

    override fun sagaStarted(type: String) { started += type }
    override fun sagaEnded(type: String) { ended += type }
    override fun event(type: String, outcome: SagaMetrics.Outcome) { events += type to outcome }
    fun count(type: String, outcome: SagaMetrics.Outcome) = events.count { it == type to outcome }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaRuntimeTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")
    private val now = Instant.parse("2026-11-03T10:00:00Z")
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private lateinit var metrics: RecordingMetrics
    private lateinit var runtime: SagaRuntime

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_runtime_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7), Duration.ofDays(30)).afterPropertiesSet()
        listOf("processed_events", "saga_failures").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events").forEach { mongo.remove(Query(), it) }
        RtGate.reset()
        metrics = RecordingMetrics()
        runtime = runtime(RtOrderSaga::class, RtHeaderSaga::class)
    }

    private fun runtime(vararg classes: kotlin.reflect.KClass<*>) = SagaRuntime(
        types = SagaModelScanner(resolver, codec).scan(classes.toList()),
        mongo = mongo,
        tx = tx,
        codec = codec,
        typeResolver = resolver,
        beans = { type -> if (type == RtBean::class) RtBean("the-bean") else error("no bean $type") },
        clock = clock,
        retry = SagaRetry(RetryPolicy(maxAttempts = 5, initialBackoff = Duration.ofMillis(5), multiplier = 1.0, maxBackoff = Duration.ofMillis(20))),
        metrics = metrics,
    )

    /** Delivers [event] to the saga stage as the dispatcher would after stage 1. */
    private fun deliver(event: Event, id: String = UUID.randomUUID().toString(), headers: Map<String, String> = emptyMap(), on: SagaRuntime = runtime) {
        val type = resolver.typeNameOf(event::class)
        val ctx = MessageContext(id, type, "corr-$id", null, emptySet(), null, headers + mapOf("id" to id, "type" to type))
        on.process(type, codec.encode(event), ctx)
    }

    private fun sagas(type: String = "rt.Order") = mongo.find(Query(Criteria.where("type").`is`(type)), SagaDocument::class.java)
    private fun saga(orderId: String) = sagas().single { it.stateDoc.getString("orderId") == orderId }
    private fun dispatch(sagaId: String) =
        mongo.find(Query(Criteria.where("sagaId").`is`(sagaId)).with(Sort.by("seq")), SagaDispatchDocument::class.java)
    private fun deadlines() = mongo.findAll(DeadlineDocument::class.java)
    private fun log(doc: SagaDocument): List<String> = doc.stateDoc.getList("log", String::class.java) ?: emptyList()
    private fun marker(id: String) = mongo.exists(Query(Criteria.where("_id").`is`(id)), "processed_events")

    @Test
    fun `state and dispatched payloads keep BigDecimal scale and map keys with dots or a leading dollar exactly`() {
        val priced = runtime(RtPriceSaga::class)
        val tags = mapOf("a.b" to "x", "\$c" to "y")
        deliver(RtPriced("p1", BigDecimal("12.30"), tags), on = priced)
        deliver(RtPriceCheck("p1"), on = priced)

        val doc = sagas("rt.Price").single()
        val sent = dispatch(doc.id).map { it.payload }
        assertEquals(2, sent.size)
        sent.forEach {
            assertTrue("12.30" in it, "the amount keeps its scale in the dispatched command: $it")
            assertTrue("\"a.b\"" in it && "\"\$c\"" in it, "the map keys survive in the dispatched command: $it")
        }
        val state = codec.decode(doc.state.toByteArray(), RtPriceSaga::class)
        assertEquals(BigDecimal("12.30"), state.amount)
        assertEquals(tags, state.tags)
    }

    @Test
    fun `a start event creates an instance with its association and state`() {
        deliver(RtOrderPlaced("o1"))

        val doc = saga("o1")
        assertEquals("rt.Order|orderId=o1", doc.startKey)
        assertEquals(setOf(Association("orderId", "o1"), Association("paymentRef", "pay-o1")), doc.associations.toSet())
        assertEquals(1, doc.stateDoc.getInteger("placed"))
        assertEquals(false, doc.ended)
        assertEquals(0L, doc.version)
        assertEquals(now, doc.createdAt)
        assertEquals(listOf("rt.Order"), metrics.started)
        assertEquals(1, metrics.count("rt.Order", SagaMetrics.Outcome.HANDLED))
    }

    @Test
    fun `a later event is routed by association to that instance`() {
        deliver(RtOrderPlaced("o2"))
        deliver(RtBump("o2", "a"))

        val doc = saga("o2")
        assertEquals(listOf("a"), log(doc))
        assertEquals(1L, doc.version, "every handled event bumps the version")
        assertEquals(1, sagas().size)
    }

    @Test
    fun `an event that matches an instance and a start handler is handled once by the instance`() {
        deliver(RtOrderPlaced("o3"))
        deliver(RtOrderPlaced("o3"))

        val doc = saga("o3")
        assertEquals(1, sagas().size, "no second instance")
        assertEquals(2, doc.stateDoc.getInteger("placed"), "the existing instance handled the second start event, once")
        assertEquals(1, metrics.started.size)
    }

    @Test
    fun `forceNew always creates a new instance without a startKey`() {
        deliver(RtForced("o4"))
        deliver(RtForced("o4"))

        val all = sagas()
        assertEquals(2, all.size)
        assertTrue(all.all { it.startKey == null })
        assertEquals(3, all.sumOf { it.stateDoc.getInteger("forced") }, "the first instance handled both, the second only its own")
    }

    @Test
    fun `two concurrent start events for one key end in one instance that handled both`() {
        RtGate.barrier = CyclicBarrier(2)
        runInParallel({ deliver(RtOrderPlaced("o5")) }, { deliver(RtOrderPlaced("o5")) })

        val doc = saga("o5")
        assertEquals(1, sagas().size)
        assertEquals(2, doc.stateDoc.getInteger("placed"))
        assertTrue(metrics.count("rt.Order", SagaMetrics.Outcome.CONFLICT) >= 1, "the loser conflicted and retried")
        assertEquals(0, mongo.count(Query(), "saga_failures"))
        assertEquals(1, metrics.started.size)
    }

    @Test
    fun `two concurrent events for one instance never interleave and both apply`() {
        deliver(RtOrderPlaced("o6"))
        RtGate.barrier = CyclicBarrier(2)
        runInParallel({ deliver(RtBump("o6", "x")) }, { deliver(RtBump("o6", "y")) })

        val doc = saga("o6")
        assertEquals(setOf("x", "y"), log(doc).toSet())
        assertEquals(2, log(doc).size, "neither lost nor applied twice")
        assertEquals(2L, doc.version)
        assertTrue(metrics.count("rt.Order", SagaMetrics.Outcome.CONFLICT) >= 1)
        assertEquals(2, dispatch(doc.id).count { it.type == "rt.DoIt" && it.payloadDoc.getString("orderId") != "o6" }, "one command per applied bump")
    }

    @Test
    fun `associateWith and removeAssociationWith change routing for later events`() {
        deliver(RtOrderPlaced("o7"))
        deliver(RtByRef("r7", "before"))
        deliver(RtAssoc("o7", "ref", "r7"))
        deliver(RtByRef("r7", "while"))
        deliver(RtAssoc("o7", "ref", "r7", remove = true))
        deliver(RtByRef("r7", "after"))

        assertEquals(listOf("while"), log(saga("o7")))
        assertEquals(setOf(Association("orderId", "o7"), Association("paymentRef", "pay-o7")), saga("o7").associations.toSet())
    }

    @Test
    fun `EndSaga ends the instance and cancels its scheduled deadlines`() {
        deliver(RtOrderPlaced("o8"))
        assertEquals(2, deadlines().count { it.status == DeadlineStatus.SCHEDULED })

        deliver(RtPaymentTaken("pay-o8"))

        val doc = saga("o8")
        assertEquals(true, doc.ended)
        assertEquals(now, doc.endedAt)
        assertEquals(listOf("paid"), log(doc))
        assertTrue(deadlines().all { it.status == DeadlineStatus.CANCELLED && it.finishedAt == now })
        assertEquals(listOf("rt.Order"), metrics.ended)
    }

    @Test
    fun `end() from code ends the instance`() {
        deliver(RtOrderPlaced("o9"))
        deliver(RtFinish("o9"))

        assertEquals(true, saga("o9").ended)
        deliver(RtBump("o9", "late")) // an ended instance routes nothing
        assertEquals(emptyList(), log(saga("o9")))
    }

    @Test
    fun `commands and events are recorded in saga_dispatch in call order and not sent`() {
        deliver(RtOrderPlaced("o10"), id = "e-10")
        deliver(RtBump("o10", "a"))

        val doc = saga("o10")
        val entries = dispatch(doc.id)
        assertEquals(listOf(0L, 1L, 1000L), entries.map { it.seq })
        assertEquals(listOf(DispatchKind.COMMAND, DispatchKind.EVENT, DispatchKind.COMMAND), entries.map { it.kind })
        assertEquals(listOf("rt.DoIt", "rt.Done", "rt.DoIt"), entries.map { it.type })
        assertEquals("o10", entries[0].payloadDoc.getString("orderId"))
        assertEquals("k-o10", entries[1].routingKey)
        assertEquals("o10-a", entries[2].payloadDoc.getString("orderId"))
        assertTrue(entries.all { it.dispatchedAt == null && it.failedAt == null && it.attempts == 0 && it.createdAt == now })
        assertEquals("corr-e-10", entries[0].headers["correlation-id"])
    }

    @Test
    fun `deadlines are recorded with due time, payload and payload type name`() {
        deliver(RtOrderPlaced("o11"))

        val byName = deadlines().associateBy { it.name }
        val timeout = byName.getValue("timeout")
        assertEquals(saga("o11").id, timeout.sagaId)
        assertEquals("rt.Order", timeout.sagaType)
        assertEquals(now.plus(Duration.ofMinutes(10)), timeout.dueAt)
        assertEquals("\"p-o11\"", timeout.payload)
        assertEquals("java.lang.String", timeout.payloadType, "no @MessageType: the FQCN (R7)")
        assertEquals(DeadlineStatus.SCHEDULED, timeout.status)
        val nudge = byName.getValue("nudge")
        assertEquals(Instant.parse("2026-11-03T12:00:00Z"), nudge.dueAt)
        assertEquals("rt.Nudge", nudge.payloadType, "@MessageType name (R7)")
        assertEquals("{\"orderId\":\"o11\"}", nudge.payload)
    }

    @Test
    fun `a handler exception rolls back the state, the dispatch entries and the deadlines of that attempt`() {
        RtGate.failFirst.set(1) // the first attempt fails after it sent, published and scheduled

        deliver(RtOrderPlaced("o12"))

        val doc = saga("o12")
        assertEquals(1, sagas().size)
        assertEquals(1, doc.stateDoc.getInteger("placed"))
        assertEquals(listOf("rt.DoIt", "rt.Done"), dispatch(doc.id).map { it.type }, "the failed attempt left no entries")
        assertEquals(2, deadlines().size, "nor deadlines")
        assertEquals(0, mongo.count(Query(), "saga_failures"))
        assertEquals(1, metrics.count("rt.Order", SagaMetrics.Outcome.HANDLED))
        assertEquals(0, metrics.count("rt.Order", SagaMetrics.Outcome.PARKED))
    }

    @Test
    fun `a redelivered event is skipped by its marker`() {
        deliver(RtOrderPlaced("o13"))
        deliver(RtBump("o13", "a"), id = "bump-1")
        deliver(RtBump("o13", "a"), id = "bump-1")

        val doc = saga("o13")
        assertEquals(listOf("a"), log(doc))
        assertEquals(1L, doc.version)
        assertTrue(marker("saga:rt.Order:bump-1"))
        assertEquals(2, dispatch(doc.id).count { it.type == "rt.DoIt" }, "start command and one bump command, no second bump")
    }

    @Test
    fun `an event no instance matches and no start handler takes is counted no_match`() {
        deliver(RtBump("nobody", "a"), id = "lost-1")
        deliver(RtByRef("unknown-ref", "a"))

        assertEquals(0, sagas().size)
        assertEquals(2, metrics.count("rt.Order", SagaMetrics.Outcome.NO_MATCH))
        assertEquals(0, mongo.count(Query(), "saga_failures"))
        assertTrue(marker("saga:rt.Order:lost-1"), "a no-match event is final: its marker is written")
    }

    @Test
    fun `a saga type that throws on every attempt is parked in saga_failures while another saga type handles the same event`() {
        val both = runtime(RtOrderSaga::class, RtBrokenSaga::class)

        deliver(RtOrderPlaced("o14"), id = "e-14", headers = mapOf("userId" to "u-1"), on = both)

        assertEquals(1, sagas().size, "the healthy type handled it")
        assertEquals(0, sagas("rt.Broken").size)
        val failure = assertNotNull(mongo.findById("rt.Broken:e-14", SagaFailureDocument::class.java))
        assertEquals("rt.Broken", failure.sagaType)
        assertEquals("e-14", failure.eventId)
        assertEquals("rt.OrderPlaced", failure.eventType)
        assertEquals("o14", failure.payloadDoc.getString("orderId"))
        assertEquals("u-1", failure.headers["userId"])
        assertEquals("corr-e-14", failure.headers["correlationId"])
        assertEquals(2, failure.attempts, "a handler exception gets two quick attempts, not the conflict backoff")
        assertTrue("broken on o14" in failure.error, failure.error)
        assertEquals(now, failure.firstFailedAt)
        assertEquals(now, failure.lastFailedAt)
        assertTrue(marker("saga:rt.Broken:e-14") && marker("saga:rt.Order:e-14"), "parking writes the marker with the failure")
        assertEquals(1, metrics.count("rt.Broken", SagaMetrics.Outcome.PARKED))
        assertEquals(1, metrics.count("rt.Order", SagaMetrics.Outcome.HANDLED))

        deliver(RtOrderPlaced("o14"), id = "e-14", on = both) // redelivered: both markers hold
        assertEquals(1, mongo.count(Query(), "saga_failures"))
        assertEquals(1, saga("o14").stateDoc.getInteger("placed"))
    }

    @Test
    fun `removing the start association frees the start key, so a later start event creates a new instance`() {
        deliver(RtOrderPlaced("o31"))
        assertEquals("rt.Order|orderId=o31", saga("o31").startKey)

        deliver(RtAssoc("o31", "orderId", "o31", remove = true)) // the start association (key orderId, value o31) goes
        assertEquals(null, sagas().single().startKey, "the start key is released with its association")

        deliver(RtOrderPlaced("o31")) // no live instance is associated with orderId=o31 any more

        assertEquals(2, sagas().size, "a second instance started under the freed key")
        assertEquals(1, sagas().count { it.startKey == "rt.Order|orderId=o31" })
        assertEquals(0, mongo.count(Query(), "saga_failures"), "nothing was parked on a duplicate start key")
    }

    @Test
    fun `a start handler that removes its own start association leaves no start key, so the next start event creates a new instance`() {
        val handover = runtime(RtHandoverSaga::class)
        deliver(RtHandedOver("o32"), on = handover)

        val first = sagas("rt.Handover").single()
        assertEquals(listOf(Association("paymentRef", "pay-o32")), first.associations)
        assertEquals(null, first.startKey, "the start association went in the start handler, and the start key with it")

        deliver(RtHandedOver("o32"), on = handover) // no live instance is associated with orderId=o32

        assertEquals(2, sagas("rt.Handover").size, "a second instance started under the same key")
        assertEquals(0, mongo.count(Query(), "saga_failures"), "nothing was parked on a duplicate start key")
    }

    @Test
    fun `a saga type whose handler always throws is parked within well under a second, after two attempts`() {
        val broken = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RtBrokenSaga::class)), mongo = mongo, tx = tx, codec = codec, typeResolver = resolver,
            beans = { error("no beans") }, clock = clock, retry = SagaRetry(), // the real policy: 5 attempts, 200 ms doubling
        )

        val started = System.nanoTime()
        deliver(RtOrderPlaced("o30"), id = "e-30", on = broken)
        val tookMs = (System.nanoTime() - started) / 1_000_000

        assertTrue(tookMs < 1000, "parking took $tookMs ms: a handler exception must not back off like a conflict")
        assertEquals(2, RtGate.brokenCalls.get())
        assertNotNull(mongo.findById("rt.Broken:e-30", SagaFailureDocument::class.java))
    }

    @Test
    fun `handler parameters are resolved - lifecycle, commands, deadlines, MessageContext and a Spring bean`() {
        deliver(RtOrderPlaced("o15"), id = "e-15")

        val doc = saga("o15")
        assertEquals(doc.id, doc.stateDoc.getString("sawSagaId"))
        assertEquals("e-15", doc.stateDoc.getString("sawMessageId"))
        assertEquals("the-bean", doc.stateDoc.getString("sawBean"))
        assertEquals(2, dispatch(doc.id).size, "commands recorded")
        assertEquals(2, deadlines().size, "deadlines recorded")
    }

    @Test
    fun `handlers run with sagaType, sagaId and correlationId in the MDC`() {
        deliver(RtOrderPlaced("o16"), id = "e-16")

        val doc = saga("o16")
        assertEquals("rt.Order", doc.stateDoc.getString("mdcSagaType"))
        assertEquals(doc.id, doc.stateDoc.getString("mdcSagaId"))
        assertEquals("corr-e-16", doc.stateDoc.getString("mdcCorrelationId"))
        assertNull(org.slf4j.MDC.get("sagaType"), "and not after")
    }

    @Test
    fun `start keys whose parts contain the separators stay apart`() {
        val escaping = runtime(RtEscapeSaga::class)
        deliver(RtNote("x=1"), on = escaping) // k = "x=1"
        deliver(RtBump("o", "1"), on = escaping) // "k=x" = 1: the same text once joined by "="

        val docs = sagas("rt.Escape").sortedBy { it.startKey }
        assertEquals(listOf("rt.Escape|k%3Dx=1", "rt.Escape|k=x%3D1"), docs.map { it.startKey }, "two instances, each with its own key")
        assertEquals(listOf(1, 1), docs.map { it.stateDoc.getInteger("starts") })
        assertEquals("rt.Escape%7Ca%25b|k=v", SagaRuntime.startKey("rt.Escape|a%b", "k", "v"), "the type name and a percent sign are escaped too")
        assertEquals(0, mongo.count(Query(), "saga_failures"), "nothing was parked on a shared key")
    }

    @Test
    fun `associationHeader routes by the message header`() {
        deliver(RtNote("one"), headers = mapOf("x-order" to "h1"))
        deliver(RtNote("two"), headers = mapOf("x-order" to "h1"))
        deliver(RtNote("three"), headers = mapOf("x-order" to "h2"))
        deliver(RtNote("no header"))

        val docs = sagas("rt.Header").sortedBy { it.startKey }
        assertEquals(listOf("rt.Header|x-order=h1", "rt.Header|x-order=h2"), docs.map { it.startKey })
        assertEquals(listOf(2, 1), docs.map { it.stateDoc.getInteger("notes") })
        assertEquals(listOf(Association("x-order", "h1")), docs[0].associations)
        assertEquals(1, metrics.count("rt.Header", SagaMetrics.Outcome.NO_MATCH))
    }

    private fun runInParallel(vararg blocks: () -> Unit) {
        val pool = Executors.newFixedThreadPool(blocks.size)
        try {
            blocks.map { b -> pool.submit { b() } }.forEach { it.get(30, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }
}
