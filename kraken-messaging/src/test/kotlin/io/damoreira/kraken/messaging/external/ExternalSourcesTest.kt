package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.mongoStore
import io.damoreira.kraken.messaging.SAGAS
import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import io.damoreira.kraken.messaging.saga.stateDoc
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.KafkaExternalSource
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.StartSaga
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
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
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@MessageType("ek.Replied")
data class EkReplied(val jobId: String, val status: String) : Event

/** The ordinary handler of the partner's event (stage 1 of the local dispatch). */
class EkHandlers {
    val seen = CopyOnWriteArrayList<String>()
    val contexts = CopyOnWriteArrayList<MessageContext>()

    @EventHandler
    fun on(e: EkReplied, ctx: MessageContext) {
        seen += e.jobId
        contexts += ctx
    }
}

@Saga(name = "ek.Job")
class EkSaga {
    var jobId: String? = null
    var replies = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "jobId")
    fun on(e: EkReplied) {
        jobId = e.jobId
        replies++
    }
}

/** `{"jobId", "status", "eventId"?}`: status `ignore` is ignored, `boom` fails, `extra` takes its headers along. */
private class EkTranslator(private val codec: JsonCodec) : ExternalRecordTranslator {
    val calls = AtomicInteger()

    override fun translate(record: ExternalRecord): TranslatedEvent? {
        calls.incrementAndGet()
        val tree = codec.decodeTree(record.payload)
        val status = tree["status"].asString()
        if (status == "ignore") return null
        if (status == "boom") throw IllegalStateException("cannot translate")
        return TranslatedEvent(EkReplied(tree["jobId"].asString(), status), tree["eventId"]?.asString())
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
/** Partner sources read through the core's [ExternalSources] from the Kafka add-on's [KafkaExternalSource] (ADR 0027). */
class ExternalSourcesTest {
    private val kafkaContainer = KrakenContainers.kafka()
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var store: MessageStore
    private lateinit var tx: TransactionTemplate
    private lateinit var kafka: KafkaProducer<String, ByteArray>
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("ek")
    private lateinit var topic: String
    private lateinit var handlers: EkHandlers
    private lateinit var translator: EkTranslator
    private lateinit var dispatcher: LocalEventDispatcher
    private var sources: ExternalSources? = null

    @BeforeAll
    fun start() {
        kafkaContainer.start()
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "external_kafka_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        store = mongoStore(mongo)
        listOf("processed_events", "saga_failures", "outbox").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
        kafka = KafkaProducer(
            mapOf<String, Any>(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers, ProducerConfig.ACKS_CONFIG to "all"),
            StringSerializer(),
            ByteArraySerializer(),
        )
    }

    @AfterAll
    fun stop() {
        kafka.close()
        kafkaContainer.stop()
        mongoContainer.stop()
    }

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events", "outbox").forEach { mongo.remove(Query(), it) }
        topic = "partner.printer-" + UUID.randomUUID()
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafkaContainer.bootstrapServers)).use {
            it.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
        }
        handlers = EkHandlers()
        translator = EkTranslator(codec)
        val registry = HandlerRegistry(resolver).also { it.register(handlers) }
        val runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(EkSaga::class)), ports = store.sagas, tx = tx, codec = codec, typeResolver = resolver,
            beans = { error("no beans") },
            retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), store, runtime)
    }

    @AfterEach
    fun tearDown() {
        sources?.stop()
        sources = null
    }

    /** The source `printer` on the test topic, in the default group `service-b.external.printer`. */
    private fun printer(properties: Map<String, String> = emptyMap()) =
        KafkaExternalSource("printer", topic, "service-b.external.printer", kafkaContainer.bootstrapServers, properties)

    private fun sources(
        source: ExternalSourceProvider = printer(),
        translators: Map<String, ExternalRecordTranslator> = mapOf("printer" to translator),
        maxAttempts: Int = 3,
    ) = ExternalSources(mapOf("printer" to source), translators, dispatcher, codec, resolver, maxAttempts = maxAttempts, backOffMs = 50)
        .also { sources = it }

    private fun startSources(maxAttempts: Int = 3) = sources(maxAttempts = maxAttempts).start()

    private fun send(json: String, headers: Map<String, String> = emptyMap()) {
        val record = ProducerRecord<String, ByteArray>(topic, "k", json.toByteArray())
        headers.forEach { (k, v) -> record.headers().add(RecordHeader(k, v.toByteArray())) }
        kafka.send(record).get()
    }

    private fun sagaOf(jobId: String) = mongo.findAll(SagaDocument::class.java, SAGAS).singleOrNull { it.stateDoc.getString("jobId") == jobId }
    private fun awaitSaga(jobId: String, replies: Int = 1) = await().atMost(Duration.ofSeconds(20)).untilAsserted {
        assertEquals(replies, sagaOf(jobId)?.stateDoc?.getInteger("replies"), "saga $jobId")
    }

    @Test
    fun `a properties entry for a key the library fixes fails the startup naming the source and the key`() {
        val fixed = listOf(
            "bootstrap.servers", "group.id", "key.deserializer", "value.deserializer", "enable.auto.commit",
        )
        for (key in fixed) {
            val e = assertFailsWith<IllegalStateException>(key) { printer(mapOf(key to "x")) }
            assertTrue("printer" in e.message!! && key in e.message!!, "$key: ${e.message}")
        }
        // the keys the library does not fix still pass through
        val passing = printer(mapOf("security.protocol" to "PLAINTEXT", "auto.offset.reset" to "latest")).consumerProperties()
        assertEquals("latest", passing["auto.offset.reset"])
        assertEquals(false, passing["allow.auto.create.topics"], "a partner's topic is never created")
    }

    @Test
    fun `a record on the partner topic becomes a local event a saga handles`() {
        startSources()
        send("""{"jobId":"j1","status":"done"}""")

        awaitSaga("j1")
        assertEquals(listOf("j1"), handlers.seen.toList(), "the ordinary handler saw it too")
        // The dedupe id is the source, the segment and the position (the partition and the offset); both stages' markers carry it.
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("processor:default:printer:0:0")), "processed_events"))
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("saga:ek.Job:printer:0:0")), "processed_events"))
    }

    @Test
    fun `null from the translator ignores the record`() {
        startSources()
        send("""{"jobId":"skipped","status":"ignore"}""")
        send("""{"jobId":"j2","status":"done"}""")

        awaitSaga("j2")
        assertEquals(2, translator.calls.get())
        assertEquals(listOf("j2"), handlers.seen.toList())
        assertEquals(null, sagaOf("skipped"))
        assertEquals(0, mongo.count(Query(Criteria.where("_id").`is`("processor:default:printer:0:0")), "processed_events"), "an ignored record leaves no marker")
    }

    @Test
    fun `a redelivered record is deduplicated by its dedupe key`() {
        startSources()
        send("""{"jobId":"j3","status":"done","eventId":"evt-1"}""")
        send("""{"jobId":"j3","status":"done","eventId":"evt-1"}""")
        send("""{"jobId":"j3","status":"again","eventId":"evt-2"}""")

        awaitSaga("j3", replies = 2)
        assertEquals(listOf("j3", "j3"), handlers.seen.toList(), "evt-1 once, evt-2 once")
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("processor:default:printer:evt-1")), "processed_events"))
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("saga:ek.Job:printer:evt-2")), "processed_events"))
    }

    @Test
    fun `the event is not published to kraken events`() {
        startSources()
        send("""{"jobId":"j4","status":"done"}""")

        awaitSaga("j4")
        assertEquals(0, mongo.count(Query(), "outbox"), "nothing reached the outbox that relays to kraken.events")
    }

    @Test
    fun `a source without a translator bean fails the startup`() {
        val e = assertFailsWith<IllegalStateException> { sources(translators = emptyMap()) }
        assertTrue("printer" in e.message!!, e.message)
        assertTrue("ExternalRecordTranslator" in e.message!!, e.message)
    }

    @Test
    fun `a translator that throws is retried and then skipped`() {
        startSources(maxAttempts = 3)
        send("""{"jobId":"bad","status":"boom"}""")
        send("""{"jobId":"j5","status":"done"}""")

        awaitSaga("j5")
        assertEquals(4, translator.calls.get(), "three attempts for the bad record, one for the next")
        assertEquals(null, sagaOf("bad"))
    }

    @Test
    fun `the handlers see the dedupe id, the source and the record's headers`() {
        sources().start()
        send("""{"jobId":"j6","status":"done"}""", headers = mapOf("x-partner" to "acme"))

        awaitSaga("j6")
        val ctx = handlers.contexts.single()
        assertEquals("printer:0:0", ctx.messageId)
        assertEquals("ek.Replied", ctx.type)
        assertEquals("acme", ctx.headers["x-partner"])
        assertEquals("printer:0:0", ctx.headers["id"])
        assertEquals("printer", ctx.headers["source"])
    }
}
