package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.mongoStore
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics
import com.damoreira.snodra.messaging.metrics.RecordingMessagingMetrics
import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.test.SnodraContainers
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import com.damoreira.snodra.messaging.kafka.KafkaEventStore
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreSettings
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RecordingHandlers {
    val seen = CopyOnWriteArrayList<String>()
    val contexts = CopyOnWriteArrayList<MessageContext>()
    val failuresLeft = AtomicInteger(0)
    val failAlways = java.util.concurrent.atomic.AtomicReference<String?>(null)
    val duplicateKeyFailuresLeft = AtomicInteger(0)

    @EventHandler
    fun on(event: Happened, ctx: MessageContext) {
        seen += event.id
        contexts += ctx
        if (event.id == failAlways.get()) throw IllegalStateException("poison")
        if (event.id.startsWith("flaky") && failuresLeft.getAndDecrement() > 0) throw IllegalStateException("transient")
        if (event.id.startsWith("dupkey") && duplicateKeyFailuresLeft.getAndDecrement() > 0) {
            throw org.springframework.dao.DuplicateKeyException("handler's own unique index")
        }
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventConsumerTest {
    private val kafkaContainer = SnodraContainers.kafka()
    private val mongoContainer = SnodraContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private lateinit var kafka: KafkaProducer<String, ByteArray>
    private lateinit var events: KafkaEventStore
    private val codec = JsonCodec()
    private lateinit var topic: String
    private lateinit var handlers: RecordingHandlers
    private lateinit var registry: HandlerRegistry
    private var consumer: EventConsumer? = null

    @BeforeAll
    fun start() {
        kafkaContainer.start()
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "consumer_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        kafka = KafkaProducer(
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ),
            StringSerializer(),
            ByteArraySerializer(),
        )
        events = KafkaEventStore(KafkaEventStoreSettings(kafkaContainer.bootstrapServers))
    }

    @AfterAll
    fun stop() {
        events.close()
        kafka.close()
        kafkaContainer.stop()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        mongo.remove(Query(), "processed_events")
        topic = "snodra.events.consumer-" + UUID.randomUUID()
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafkaContainer.bootstrapServers)).use {
            try {
                it.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
            } catch (e: java.util.concurrent.ExecutionException) {
                if (e.cause !is org.apache.kafka.common.errors.TopicExistsException) throw e
            }
        }
        handlers = RecordingHandlers()
        registry = HandlerRegistry(MessageTypeResolver("test"))
        registry.register(handlers)
    }

    @AfterEach
    fun tearDown() {
        consumer?.stop()
        consumer = null
    }

    private fun startConsumer(maxAttempts: Int = 5, backOffMs: Long = 100, metrics: MessagingMetrics = NoopMessagingMetrics) {
        val invoker = HandlerInvoker(registry, codec)
        consumer = EventConsumer(
            events, SubscriptionSpec("group-" + UUID.randomUUID(), topic), LocalEventDispatcher(registry, invoker, mongoStore(mongo), metrics = metrics),
            maxAttempts, backOffMs = backOffMs, metrics = metrics,
        ).also { it.start() }
    }

    private fun send(id: String?, type: String?, payload: ByteArray = codec.encode(Happened(id ?: "none", Instant.now())), extra: Map<String, String> = emptyMap()) {
        val record = ProducerRecord<String, ByteArray>(topic, "k", payload)
        if (type != null) record.headers().add(RecordHeader("type", type.toByteArray()))
        if (id != null) record.headers().add(RecordHeader("id", id.toByteArray()))
        extra.forEach { (k, v) -> record.headers().add(RecordHeader(k, v.toByteArray())) }
        kafka.send(record).get()
    }

    private fun committedOffset(): Long? =
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafkaContainer.bootstrapServers)).use { admin ->
            admin.listConsumerGroupOffsets(consumer!!.subscription).partitionsToOffsetAndMetadata().get().values.singleOrNull()?.offset()
        }

    private fun awaitSeen(vararg ids: String) =
        await().atMost(Duration.ofSeconds(10)).untilAsserted { assertTrue(handlers.seen.containsAll(ids.toList()), "seen=${handlers.seen}") }

    @Test
    fun `handled type is invoked once and offset committed`() {
        startConsumer()
        send("e-1", "test.Happened", extra = mapOf("correlationId" to "corr-1", "userId" to ""))
        awaitSeen("e-1")
        assertEquals(listOf("e-1"), handlers.seen)
        val ctx = handlers.contexts.single()
        assertEquals("e-1", ctx.messageId)
        assertEquals("corr-1", ctx.correlationId)
        assertEquals(null, ctx.userId)
        assertEquals(1, mongo.count(Query(), "processed_events"))
        consumer!!.stop()
        assertEquals(1L, committedOffset())
    }

    @Test
    fun `unhandled type is skipped`() {
        startConsumer()
        send("u-1", "test.Unknown")
        send("e-2", "test.Happened")
        awaitSeen("e-2")
        assertEquals(listOf("e-2"), handlers.seen)
        assertEquals(1, mongo.count(Query(), "processed_events"))
    }

    @Test
    fun `duplicate event id is processed once`() {
        val metrics = RecordingMessagingMetrics()
        startConsumer(metrics = metrics)
        send("d-1", "test.Happened")
        send("d-1", "test.Happened")
        send("d-2", "test.Happened")
        awaitSeen("d-2")
        assertEquals(listOf("d-1", "d-2"), handlers.seen.toList())
        assertEquals(2, mongo.count(Query(), "processed_events"))
        await().atMost(Duration.ofSeconds(5)).until { metrics.consumed.size == 3 }
        val happened = "test.Happened"
        assertEquals(
            listOf(happened to MessagingMetrics.Consumed.HANDLED, happened to MessagingMetrics.Consumed.DUPLICATE, happened to MessagingMetrics.Consumed.HANDLED),
            metrics.outcomes("default"),
        )
        assertEquals(3, metrics.handled.size)
        assertTrue(metrics.handled.all { it.processor == "default" && it.type == happened && it.nanos > 0 })
    }

    @Test
    fun `handler failure rolls back processed marker and retries up to max attempts`() {
        handlers.failuresLeft.set(2)
        startConsumer()
        send("flaky-1", "test.Happened")
        await().atMost(Duration.ofSeconds(10)).untilAsserted { assertEquals(3, handlers.seen.size) }
        await().atMost(Duration.ofSeconds(10)).untilAsserted { assertEquals(1, mongo.count(Query(), "processed_events")) }
        Thread.sleep(500)
        assertEquals(3, handlers.seen.size)
        assertEquals(1, mongo.count(Query(), "processed_events"))
    }

    @Test
    fun `handler duplicate key error is retried not swallowed`() {
        handlers.duplicateKeyFailuresLeft.set(2)
        startConsumer()
        send("dupkey-1", "test.Happened")
        await().atMost(Duration.ofSeconds(10)).untilAsserted { assertEquals(3, handlers.seen.size) }
        await().atMost(Duration.ofSeconds(10)).untilAsserted { assertEquals(1, mongo.count(Query(), "processed_events")) }
        Thread.sleep(500)
        assertEquals(listOf("dupkey-1", "dupkey-1", "dupkey-1"), handlers.seen.toList())
        assertEquals(1, mongo.count(Query(), "processed_events"))
    }

    @Test
    fun `payload not matching the handler class is not retried`() {
        // Retried, the bad record would hold the partition for 4 x 5 s before e-5; awaitSeen gives up after 10 s.
        startConsumer(maxAttempts = 5, backOffMs = 5_000)
        send("bad-1", "test.Happened", payload = "{\"unexpected\":true}".toByteArray())
        send("e-5", "test.Happened")
        awaitSeen("e-5")
        assertEquals(listOf("e-5"), handlers.seen.toList())
        assertEquals(1, mongo.count(Query(), "processed_events"))
    }

    @Test
    fun `poison record is skipped after max attempts`() {
        handlers.failAlways.set("poison-1")
        startConsumer(maxAttempts = 5)
        send("poison-1", "test.Happened")
        send("good-1", "test.Happened")
        awaitSeen("good-1")
        assertEquals(5, handlers.seen.count { it == "poison-1" })
        assertEquals(0, mongo.count(Query(org.springframework.data.mongodb.core.query.Criteria.where("_id").`is`("processor:default:poison-1")), "processed_events"))
        Thread.sleep(500)
        assertEquals(5, handlers.seen.count { it == "poison-1" })
        consumer!!.stop()
        // both records (offsets 0 and 1) are committed, so the poison offset moved under manual ack
        assertEquals(2L, committedOffset())
    }

    @Test
    fun `record without type header is skipped`() {
        startConsumer()
        send("n-1", null)
        send("e-3", "test.Happened")
        awaitSeen("e-3")
        assertEquals(listOf("e-3"), handlers.seen)
    }

    @Test
    fun `non json payload is skipped with warning`() {
        startConsumer()
        send("j-1", "test.Happened", payload = "not json".toByteArray())
        send("e-4", "test.Happened")
        awaitSeen("e-4")
        assertEquals(listOf("e-4"), handlers.seen)
        assertEquals(1, mongo.count(Query(), "processed_events"))
    }
}
