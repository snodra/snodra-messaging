package io.damoreira.kraken.messaging.kafka

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.test.KrakenTestKafka
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
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
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaProducerException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OutboxTest {
    private val kafkaContainer = KrakenContainers.kafka()
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var txManager: MongoTransactionManager
    private lateinit var producerFactory: DefaultKafkaProducerFactory<String, ByteArray>
    private lateinit var kafka: KafkaTemplate<String, ByteArray>
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private lateinit var bus: OutboxEventBus
    private val topic = "kraken.events.test-" + java.util.UUID.randomUUID()

    @BeforeAll
    fun start() {
        kafkaContainer.start()
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "outbox_test")
        mongo = MongoTemplate(factory)
        txManager = MongoTransactionManager(factory)
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
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafkaContainer.bootstrapServers)).use {
            try {
                it.createTopics(listOf(NewTopic(topic, 6, 1.toShort()))).all().get()
            } catch (e: java.util.concurrent.ExecutionException) {
                if (e.cause !is org.apache.kafka.common.errors.TopicExistsException) throw e
            }
        }
        bus = OutboxEventBus(mongo, codec, resolver, TraceHeaders(null, null))
    }

    @AfterAll
    fun stop() {
        producerFactory.destroy()
        kafkaContainer.stop()
        mongoContainer.stop()
    }

    @BeforeEach
    fun clean() {
        mongo.remove(Query(), "outbox")
    }

    private fun publisher() = OutboxPublisher(mongo, kafka, topic, "orders", Duration.ofMillis(100), 100)

    /** A real template that fails records whose key is in [failing] with [failure]. */
    private fun failingTemplate(failing: Set<String>, failure: () -> Throwable) =
        object : KafkaTemplate<String, ByteArray>(producerFactory) {
            override fun send(record: ProducerRecord<String, ByteArray>): CompletableFuture<SendResult<String, ByteArray>> =
                if (record.key() in failing) CompletableFuture.failedFuture(KafkaProducerException(record, "Failed to send", failure()))
                else super.send(record)
        }

    private fun doc(key: String) = mongo.findOne(Query(Criteria.where("key").`is`(key)), OutboxDocument::class.java)!!

    @Test
    fun `non retriable send failure marks document failed and continues`() {
        bus.publishBlocking(Happened("nr-1", Instant.now()), key = "nr-1")
        bus.publishBlocking(Happened("nr-poison", Instant.now()), key = "nr-poison")
        bus.publishBlocking(Happened("nr-2", Instant.now()), key = "nr-2")
        val template = failingTemplate(setOf("nr-poison")) { RecordTooLargeException("record too large") }
        val publisher = OutboxPublisher(mongo, template, topic, "orders", Duration.ofMillis(100), 100, maxAttempts = 10)

        assertEquals(2, publisher.publishPendingNow()) // the poison document did not stop the batch

        val poison = doc("nr-poison")
        assertNotNull(poison.failedAt)
        assertNull(poison.sentAt)
        assertEquals(1, poison.attempts)
        assertNotNull(doc("nr-1").sentAt)
        assertNotNull(doc("nr-2").sentAt)
        assertEquals(0, publisher.publishPendingNow()) // failed documents are no longer pending
        assertEquals(1, doc("nr-poison").attempts)
    }

    @Test
    fun `retriable send failure stops the batch until max attempts then marks failed`() {
        bus.publishBlocking(Happened("r-poison", Instant.now()), key = "r-poison")
        bus.publishBlocking(Happened("r-1", Instant.now()), key = "r-1")
        val template = failingTemplate(setOf("r-poison")) { org.apache.kafka.common.errors.TimeoutException("broker down") }
        val publisher = OutboxPublisher(mongo, template, topic, "orders", Duration.ofMillis(100), 100, maxAttempts = 2)

        assertEquals(0, publisher.publishPendingNow()) // retriable: attempts 1, batch stopped to keep order
        assertNull(doc("r-poison").failedAt)
        assertNull(doc("r-1").sentAt)
        assertEquals(1, publisher.publishPendingNow()) // attempt 2 reaches max-attempts: marked failed, r-1 sent
        assertNotNull(doc("r-poison").failedAt)
        assertEquals(2, doc("r-poison").attempts)
        assertNotNull(doc("r-1").sentAt)
    }

    @Test
    fun `publish writes outbox document with headers and camelCase payload`() {
        bus.publishBlocking(Happened("a-1", Instant.parse("2026-01-01T00:00:00Z")), key = "a-1")

        val docs = mongo.findAll(OutboxDocument::class.java)
        assertEquals(1, docs.size)
        val doc = docs.single()
        assertEquals("test.Happened", doc.type)
        assertEquals("a-1", doc.key)
        assertNull(doc.sentAt)
        assertEquals(0, doc.attempts)
        assertEquals("a-1", doc.payload.getString("id"))
        assertTrue(doc.headers.containsKey("correlationId"))
        assertEquals("", doc.headers["userId"])
    }

    @Test
    fun `publish inside failing transaction leaves no document`() {
        val ex = assertFailsWith<RuntimeException> {
            TransactionTemplate(txManager).executeWithoutResult {
                bus.publishBlocking(Happened("tx", Instant.now()))
                throw RuntimeException("boom")
            }
        }
        assertEquals("boom", ex.message)
        assertEquals(0, mongo.count(Query(), "outbox"))
    }

    @Test
    fun `publisher sends to kafka with all headers and marks sent`() {
        bus.publishBlocking(Happened("h-1", Instant.now()), key = "h-1")
        assertEquals(1, publisher().publishPendingNow())

        val (event, headers) = KrakenTestKafka(kafkaContainer.bootstrapServers, "test", topic)
            .awaitEvent<Happened> { it.id == "h-1" }
        assertEquals("h-1", event.id)
        for (h in listOf("type", "id", "correlationId", "timestamp", "source", "userId")) {
            assertTrue(headers.containsKey(h), "missing header $h in $headers")
        }
        assertEquals("orders", headers["source"])
        val doc = mongo.findOne(Query(Criteria.where("payload.id").`is`("h-1")), OutboxDocument::class.java)!!
        assertNotNull(doc.sentAt)
        assertEquals(doc.id, headers["id"])
        assertEquals(doc.type, headers["type"])
        assertEquals(doc.createdAt.toString(), headers["timestamp"])
    }

    @Test
    fun `publisher preserves per-key order`() {
        val ids = (1..20).map { "o-%02d".format(it) }
        ids.forEach { bus.publishBlocking(Happened(it, Instant.now()), key = "k") }
        assertEquals(20, publisher().publishPendingNow())

        val seen = mutableListOf<String>()
        KafkaConsumer<String, ByteArray>(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "order-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            ByteArrayDeserializer(),
        ).use { c ->
            c.subscribe(listOf(topic))
            val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos()
            while (seen.size < 20 && System.nanoTime() < deadline) {
                c.poll(Duration.ofMillis(200)).filter { it.key() == "k" }.forEach {
                    seen += codec.decodeTree(it.value()).get("id").asString()
                }
            }
        }
        assertEquals(ids, seen)
    }

    @Test
    fun `indexes exist with expected ttl`() {
        MessagingIndexes(mongo, Duration.ofDays(7), Duration.ofDays(30)).afterPropertiesSet()
        MessagingIndexes(mongo, Duration.ofDays(7), Duration.ofDays(30)).afterPropertiesSet() // idempotent

        fun indexes(collection: String): List<org.bson.Document> =
            mongo.getCollection(collection).listIndexes(org.bson.Document::class.java).into(mutableListOf())
        val outbox = indexes("outbox")
        assertTrue(outbox.any { (it["key"] as org.bson.Document).keys.toList() == listOf("sentAt", "createdAt", "seq") })
        assertTrue(outbox.any { (it["expireAfterSeconds"] as Number?)?.toLong() == Duration.ofDays(7).seconds })
        assertTrue(indexes("processed_events").any { (it["expireAfterSeconds"] as Number?)?.toLong() == Duration.ofDays(30).seconds })
    }
}
