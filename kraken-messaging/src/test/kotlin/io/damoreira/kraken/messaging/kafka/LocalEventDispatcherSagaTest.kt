package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.mongoStore
import io.damoreira.kraken.messaging.SAGAS
import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.saga.RtBean
import io.damoreira.kraken.messaging.saga.RtOrderPlaced
import io.damoreira.kraken.messaging.saga.RtOrderSaga
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.stateDoc
import io.damoreira.kraken.messaging.test.KrakenContainers
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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** An ordinary handler of the saga's start event that always fails, as one with a bug or a dependency outage does. */
class FailingOrderHandler {
    val calls = AtomicInteger()

    @EventHandler
    fun on(e: RtOrderPlaced) {
        calls.incrementAndGet()
        throw IllegalStateException("ordinary handler is broken on ${e.orderId}")
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalEventDispatcherSagaTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private lateinit var store: MessageStore
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "dispatcher_saga_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        store = mongoStore(mongo)
        listOf("processed_events", "saga_failures").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events").forEach { mongo.remove(Query(), it) }
    }

    @Test
    fun `a throwing ordinary handler does not stop a saga from applying the event exactly once across redeliveries`() {
        val handler = FailingOrderHandler()
        val registry = HandlerRegistry(resolver).also { it.register(handler) }
        val runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RtOrderSaga::class)), ports = store.sagas, tx = tx, codec = codec, typeResolver = resolver,
            beans = { RtBean("bean") }, retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), store, runtime)
        val type = resolver.typeNameOf(RtOrderPlaced::class)
        val ctx = MessageContext("e-1", type, "corr-1", null, emptySet(), null, mapOf("id" to "e-1", "type" to type))

        repeat(3) { assertFailsWith<IllegalStateException> { dispatcher.dispatch("e-1", type, codec.encode(RtOrderPlaced("o1")), ctx) } }

        assertEquals(3, handler.calls.get(), "stage 1 ran on every delivery and failed")
        val saga = mongo.findAll(SagaDocument::class.java, SAGAS).single()
        assertEquals(1, saga.stateDoc.getInteger("placed"), "the saga applied the event once, however often the record came back")
    }
}
