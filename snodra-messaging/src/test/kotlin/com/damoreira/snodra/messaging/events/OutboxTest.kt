package com.damoreira.snodra.messaging.events

import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.metrics.RecordingMessagingMetrics
import com.damoreira.snodra.messaging.mongo.MongoMessageStore
import com.damoreira.snodra.messaging.test.SnodraContainers
import com.damoreira.snodra.messaging.test.SnodraTestKafka
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.kafka.KafkaEventStore
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreSettings
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
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
import org.springframework.transaction.support.TransactionTemplate
import org.bson.Document
import java.time.Duration
import java.time.Instant
import java.util.Date
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OutboxTest {
    private val kafkaContainer = SnodraContainers.kafka()
    private val mongoContainer = SnodraContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var txManager: MongoTransactionManager
    private lateinit var store: MongoMessageStore
    private lateinit var kafka: KafkaEventStore
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private lateinit var bus: OutboxEventBus
    private val topic = "snodra.events.test-" + java.util.UUID.randomUUID()

    @BeforeAll
    fun start() {
        kafkaContainer.start()
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "outbox_test")
        mongo = MongoTemplate(factory)
        txManager = MongoTransactionManager(factory)
        store = MongoMessageStore(mongo, txManager)
        kafka = KafkaEventStore(KafkaEventStoreSettings(kafkaContainer.bootstrapServers))
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafkaContainer.bootstrapServers)).use {
            try {
                it.createTopics(listOf(NewTopic(topic, 6, 1.toShort()))).all().get()
            } catch (e: java.util.concurrent.ExecutionException) {
                if (e.cause !is org.apache.kafka.common.errors.TopicExistsException) throw e
            }
        }
        bus = OutboxEventBus(store.outbox, codec, resolver, TraceHeaders(null, null))
    }

    @AfterAll
    fun stop() {
        kafka.close()
        kafkaContainer.stop()
        mongoContainer.stop()
    }

    @BeforeEach
    fun clean() {
        mongo.remove(Query(), "outbox")
    }

    private fun publisher() = OutboxPublisher(store.outbox, kafka, topic, "orders", Duration.ofMillis(100), 100)

    /** The real event store, but a publish of [onPublish] runs first (it may throw: the publish then fails with it). */
    private class Wrapped(private val inner: EventStore, private val onPublish: (OutgoingEvent) -> Unit) : EventStore by inner {
        override suspend fun publish(event: OutgoingEvent) {
            onPublish(event)
            inner.publish(event)
        }
    }

    /** The real event store, failing events whose key is in [failing] with [failure]. */
    private fun failingTemplate(failing: Set<String>, failure: () -> Throwable): EventStore =
        Wrapped(kafka) { if (it.key in failing) throw failure() }

    /** An `outbox` document as stored. */
    private class Doc(d: Document) {
        val id: String = d.getString("_id")
        val type: String = d.getString("type")
        val key: String? = d.getString("key")
        val headers: Map<String, Any?> = d.get("headers", Document::class.java)
        val payload: Document = d.get("payload", Document::class.java)
        val createdAt: Instant = d.getDate("createdAt").toInstant()
        val sentAt: Date? = d.getDate("sentAt")
        val failedAt: Date? = d.getDate("failedAt")
        val attempts: Int = (d["attempts"] as Number).toInt()
    }

    private fun doc(key: String) = Doc(mongo.findOne(Query(Criteria.where("key").`is`(key)), Document::class.java, "outbox")!!)

    @Test
    fun `two relays taking turns on one outbox send every entry once, and a stopped relay holds what it claimed until its lease passes`() {
        val ids = (1..30).map { "two-%02d".format(it) }
        ids.forEach { bus.publishBlocking(Happened(it, Instant.now()), key = it) }
        val sent = java.util.concurrent.ConcurrentLinkedQueue<String>()
        val counting = Wrapped(kafka) { sent += it.key!! }
        val a = OutboxPublisher(store.outbox, counting, topic, "orders", Duration.ofMillis(100), batchSize = 7)
        val b = OutboxPublisher(store.outbox, counting, topic, "orders", Duration.ofMillis(100), batchSize = 7)
        do {
            val sentNow = a.publishPendingNow() + b.publishPendingNow()
        } while (sentNow > 0)
        assertEquals(ids.sorted(), sent.sorted(), "each entry sent once")

        // A relay that claimed a batch and stopped before sending it holds it until its lease passes.
        bus.publishBlocking(Happened("held", Instant.now()), key = "held")
        val crashed = OutboxPublisher(store.outbox, counting, topic, "orders", Duration.ofMillis(100), 100, lease = Duration.ofSeconds(46))
        crashed.stop() // stop requested: it claims, then sends nothing
        assertEquals(0, crashed.publishPendingNow())
        assertEquals(0, a.publishPendingNow(), "held by the stopped relay")
        assertNull(doc("held").sentAt)
    }

    @Test
    fun `a rejected event is given up at once, and the batch continues`() {
        bus.publishBlocking(Happened("nr-1", Instant.now()), key = "nr-1")
        bus.publishBlocking(Happened("nr-poison", Instant.now()), key = "nr-poison")
        bus.publishBlocking(Happened("nr-2", Instant.now()), key = "nr-2")
        val template = failingTemplate(setOf("nr-poison")) { EventStoreException.Rejected("record too large") }
        val publisher = OutboxPublisher(store.outbox, template, topic, "orders", Duration.ofMillis(100), 100, maxAttempts = 10)

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
        val template = failingTemplate(setOf("r-poison")) { EventStoreException.Failed("broker down") }
        val publisher = OutboxPublisher(store.outbox, template, topic, "orders", Duration.ofMillis(100), 100, maxAttempts = 2)

        assertEquals(0, publisher.publishPendingNow()) // retriable: attempts 1, batch stopped to keep order
        assertNull(doc("r-poison").failedAt)
        assertNull(doc("r-1").sentAt)
        assertEquals(1, publisher.publishPendingNow()) // attempt 2 reaches max-attempts: marked failed, r-1 sent
        assertNotNull(doc("r-poison").failedAt)
        assertEquals(2, doc("r-poison").attempts)
        assertNotNull(doc("r-1").sentAt)
    }

    @Test
    fun `the relay reports every send attempt, and samples the pending count and the oldest pending entry store-wide`() {
        bus.publishBlocking(Happened("m-1", Instant.now()), key = "m-1")
        bus.publishBlocking(Happened("m-poison", Instant.now()), key = "m-poison")
        bus.publishBlocking(Happened("m-2", Instant.now()), key = "m-2")
        bus.publishBlocking(Happened("m-dead", Instant.now()), key = "m-dead")
        mongo.updateFirst(Query(Criteria.where("key").`is`("m-dead")), org.springframework.data.mongodb.core.query.Update().set("failedAt", Date()), "outbox")
        val metrics = RecordingMessagingMetrics()
        val template = failingTemplate(setOf("m-poison")) { EventStoreException.Failed("broker down") }
        val publisher = OutboxPublisher(store.outbox, template, topic, "orders", Duration.ofMillis(100), 100, maxAttempts = 2, metrics = metrics)

        publisher.samplePending()
        assertEquals(listOf(3L), metrics.pending.toList(), "unsent and not given up on: the failed document is not pending")
        assertEquals(listOf<Instant?>(doc("m-1").createdAt), metrics.oldest.toList(), "the oldest pending entry")
        assertEquals(1, publisher.publishPendingNow())
        assertEquals(listOf(true, false), metrics.published.toList(), "m-1 sent, m-poison failed (retriable), the batch stopped")
        assertEquals(1, metrics.oldest.size, "a poll does not report the oldest entry: only a sample does")
        publisher.samplePending()
        assertEquals(2L, metrics.pending.last())
        assertEquals(doc("m-poison").createdAt, metrics.oldest.last(), "held by this relay, still the oldest pending entry")

        // Another relay's view is the same: the gauge is store-wide, whoever holds the entries.
        val other = RecordingMessagingMetrics()
        OutboxPublisher(store.outbox, template, topic, "orders", Duration.ofMillis(100), 100, metrics = other).samplePending()
        assertEquals(doc("m-poison").createdAt, other.oldest.single())

        assertEquals(1, publisher.publishPendingNow()) // max attempts: m-poison given up on, m-2 sent
        assertEquals(listOf(true, false, false, true), metrics.published.toList())
        assertEquals(0, publisher.publishPendingNow())
        publisher.samplePending()
        assertEquals(0L, metrics.pending.last())
        assertNull(metrics.oldest.last(), "nothing pending")
    }

    @Test
    fun `the running relay counts the outbox when the metrics want it, and never without them`() {
        bus.publishBlocking(Happened("c-1", Instant.now()), key = "c-1")
        val metrics = RecordingMessagingMetrics()
        val publisher = OutboxPublisher(store.outbox, kafka, topic, "orders", Duration.ofMillis(50), 100, metrics = metrics, pendingSampleInterval = Duration.ofMillis(100))
        publisher.start()
        try {
            await atMost Duration.ofSeconds(10) until { metrics.pending.size >= 2 && metrics.pending.last() == 0L }
        } finally {
            publisher.stop()
        }
        assertTrue(metrics.published.all { it })
        val counted = java.util.concurrent.atomic.AtomicBoolean(false)
        val quiet = object : com.damoreira.snodra.messaging.metrics.MessagingMetrics {
            override fun outboxPending(count: Long) = counted.set(true)
        }
        val relay = OutboxPublisher(store.outbox, kafka, topic, "orders", Duration.ofMillis(50), 100, metrics = quiet, pendingSampleInterval = Duration.ofMillis(10))
        relay.start()
        Thread.sleep(300)
        relay.stop()
        assertEquals(false, counted.get(), "no count in the database when the metrics do not want it")
    }

    @Test
    fun `publish writes outbox document with headers and camelCase payload`() {
        bus.publishBlocking(Happened("a-1", Instant.parse("2026-01-01T00:00:00Z")), key = "a-1")

        val docs = mongo.findAll(Document::class.java, "outbox").map(::Doc)
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

        val (event, headers) = SnodraTestKafka(kafkaContainer.bootstrapServers, "test", topic)
            .awaitEvent<Happened> { it.id == "h-1" }
        assertEquals("h-1", event.id)
        for (h in listOf("type", "id", "correlationId", "timestamp", "source", "userId")) {
            assertTrue(headers.containsKey(h), "missing header $h in $headers")
        }
        assertEquals("orders", headers["source"])
        val doc = Doc(mongo.findOne(Query(Criteria.where("payload.id").`is`("h-1")), Document::class.java, "outbox")!!)
        assertNotNull(doc.sentAt)
        assertEquals(doc.id, headers["id"])
        assertEquals(doc.type, headers["type"])
        assertEquals(com.damoreira.snodra.messaging.eventstore.EventHeaders.timestamp(doc.createdAt), headers["timestamp"])
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
}
