package io.damoreira.kraken.messaging.kafka

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
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

    @EventHandler
    fun on(event: Happened, ctx: MessageContext) {
        seen += event.id
        contexts += ctx
        if (event.id == failAlways.get()) throw IllegalStateException("poison")
        if (event.id.startsWith("flaky") && failuresLeft.getAndDecrement() > 0) throw IllegalStateException("transient")
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventConsumerTest {
    private val kafkaContainer = KrakenContainers.kafka()
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private lateinit var producerFactory: DefaultKafkaProducerFactory<String, ByteArray>
    private lateinit var kafka: KafkaTemplate<String, ByteArray>
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
        producerFactory = DefaultKafkaProducerFactory(
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ),
            StringSerializer(),
            ByteArraySerializer(),
        )
        kafka = KafkaTemplate(producerFactory)
    }

    @AfterAll
    fun stop() {
        producerFactory.destroy()
        kafkaContainer.stop()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        mongo.remove(Query(), "processed_events")
        topic = "kraken.events.consumer-" + UUID.randomUUID()
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

    private fun startConsumer(maxAttempts: Int = 5) {
        val factory = DefaultKafkaConsumerFactory(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ),
            StringDeserializer(),
            ByteArrayDeserializer(),
        )
        consumer = EventConsumer(
            factory, topic, "group-" + UUID.randomUUID(), registry, HandlerInvoker(registry, codec), tx, mongo,
            maxAttempts, backOffMs = 100,
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
            admin.listConsumerGroupOffsets(consumer!!.groupIdForTest()).partitionsToOffsetAndMetadata().get().values.singleOrNull()?.offset()
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
        startConsumer()
        send("d-1", "test.Happened")
        send("d-1", "test.Happened")
        send("d-2", "test.Happened")
        awaitSeen("d-2")
        assertEquals(listOf("d-1", "d-2"), handlers.seen.toList())
        assertEquals(2, mongo.count(Query(), "processed_events"))
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
    fun `poison record is skipped after max attempts`() {
        handlers.failAlways.set("poison-1")
        startConsumer(maxAttempts = 5)
        send("poison-1", "test.Happened")
        send("good-1", "test.Happened")
        awaitSeen("good-1")
        assertEquals(5, handlers.seen.count { it == "poison-1" })
        assertEquals(0, mongo.count(Query(org.springframework.data.mongodb.core.query.Criteria.where("_id").`is`("poison-1")), "processed_events"))
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
