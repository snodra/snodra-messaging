package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.StartSaga
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@MessageType("mt.Placed")
data class MtPlaced(val orderId: String) : Event

@MessageType("mt.Bump")
data class MtBump(val orderId: String, val tag: String) : Event

/** Holds the first two handler calls until both arrived, so two saga transactions are open at once; retries pass through. */
object MtGate {
    @Volatile var barrier: CyclicBarrier? = null
    private val arrivals = AtomicInteger()

    fun reset(parties: Int?) {
        arrivals.set(0)
        barrier = parties?.let { CyclicBarrier(it) }
    }

    fun meet() {
        val b = barrier ?: return
        if (arrivals.incrementAndGet() <= b.parties) b.await(10, TimeUnit.SECONDS)
    }
}

@Saga(name = "mt.Order")
class MtOrderSaga {
    var orderId: String? = null
    var placed = 0
    var log: MutableList<String> = mutableListOf()

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: MtPlaced) {
        MtGate.meet()
        orderId = e.orderId
        placed++
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: MtBump) {
        MtGate.meet()
        log += e.tag
    }
}

/**
 * The saga runtime's concurrent writes on a MongoDB replica set, end to end: the add-on reports a write conflict raised at a
 * write (error 112, `TransientTransactionError`) and the partial unique index on a live start key so that the runtime retries
 * the losing transaction. The store-neutral checks are the conformance kit's; these keep the real race on MongoDB.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoSagaConcurrencyTest {
    private val container = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MongoTemplate
    private lateinit var store: MongoMessageStore
    private lateinit var runtime: SagaRuntime
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("mt")
    private val outcomes = CopyOnWriteArrayList<Pair<String, SagaMetrics.Outcome>>()

    @BeforeAll
    fun start() {
        container.start()
        client = MongoClients.create(container.connectionString + "/?directConnection=true")
    }

    @AfterAll
    fun stop() {
        client.close()
        container.stop()
    }

    @BeforeEach
    fun setUp() {
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(client, "saga_concurrency_" + UUID.randomUUID().toString().replace("-", "")))
        store = MongoMessageStore(mongo).also { it.ensureIndexes() }
        // A transaction's first write must not create a collection while the other's is open.
        for (name in listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events")) mongo.createCollection(name)
        outcomes.clear()
        MtGate.reset(null)
        runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(MtOrderSaga::class)),
            ports = store.sagas,
            tx = TransactionTemplate(store.transactions),
            codec = codec,
            typeResolver = resolver,
            beans = { error("no beans") },
            retry = SagaRetry(RetryPolicy(maxAttempts = 5, initialBackoff = Duration.ofMillis(5), multiplier = 1.0, maxBackoff = Duration.ofMillis(20))),
            metrics = object : SagaMetrics {
                override fun sagaStarted(type: String) = Unit
                override fun sagaEnded(type: String) = Unit
                override fun event(type: String, outcome: SagaMetrics.Outcome) {
                    outcomes += type to outcome
                }
            },
        )
    }

    private fun deliver(event: Event) {
        val id = UUID.randomUUID().toString()
        val type = resolver.typeNameOf(event::class)
        runtime.process(type, codec.encode(event), MessageContext(id, type, "corr-$id", null, emptySet(), null, mapOf("id" to id, "type" to type)))
    }

    private fun runInParallel(vararg blocks: () -> Unit) {
        val pool = Executors.newFixedThreadPool(blocks.size)
        try {
            blocks.map { b -> pool.submit { b() } }.forEach { it.get(60, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private fun sagas(): List<SagaDocument> = store.sagas.sagas.findLive("mt.Order", "orderId", "o1")

    private fun state(doc: SagaDocument) = codec.decodeTree(doc.state.toByteArray())

    private fun conflicts() = outcomes.count { it.second == SagaMetrics.Outcome.CONFLICT }

    @Test
    fun `two concurrent start events for one key end in one instance that handled both`() {
        MtGate.reset(2)
        runInParallel({ deliver(MtPlaced("o1")) }, { deliver(MtPlaced("o1")) })

        val live = sagas()
        assertEquals(1, live.size, "one instance: the start key's unique index refused the second, which retried as a conflict")
        assertEquals(2, state(live.single())["placed"].asInt(), "the existing instance handled the second start event")
        assertTrue(conflicts() >= 1, "the loser conflicted and retried: $outcomes")
        assertEquals(0, store.sagas.failures.list(null).size, "nothing parked")
    }

    @Test
    fun `two concurrent events for one instance never interleave and both apply`() {
        deliver(MtPlaced("o1"))
        MtGate.reset(2)
        runInParallel({ deliver(MtBump("o1", "x")) }, { deliver(MtBump("o1", "y")) })

        val doc = sagas().single()
        val log = state(doc)["log"].values().map { it.asString() }
        assertEquals(setOf("x", "y"), log.toSet())
        assertEquals(2, log.size, "neither lost nor applied twice")
        assertEquals(2L, doc.version)
        assertTrue(conflicts() >= 1, "the write conflict was retried: $outcomes")
        assertEquals(0, store.sagas.failures.list(null).size, "nothing parked")
    }
}
