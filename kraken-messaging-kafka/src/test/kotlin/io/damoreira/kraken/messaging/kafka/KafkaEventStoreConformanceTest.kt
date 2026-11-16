package io.damoreira.kraken.messaging.kafka

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.deadLetterSource
import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.test.eventstore.EventStoreConformanceTest
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

/**
 * The event store conformance kit against a real Kafka broker: a topic of two partitions and its `.dlt`, one
 * [KafkaEventStore] per service instance. The at-least-once check keeps its markers in the MongoDB add-on's message store.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaEventStoreConformanceTest : EventStoreConformanceTest() {
    private val kafka = KrakenContainers.kafka()
    private val mongo = KrakenContainers.mongo()
    private lateinit var client: MongoClient
    private val opened = mutableListOf<KafkaEventStore>()
    private val topic = "kraken.conformance." + UUID.randomUUID().toString().replace("-", "")

    override val source: String get() = topic

    @BeforeAll
    fun start() {
        kafka.start()
        mongo.start()
        client = MongoClients.create(mongo.connectionString + "/?directConnection=true")
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 2, 1.toShort()), NewTopic(deadLetterSource(topic), 1, 1.toShort())))
                .all().get(30, TimeUnit.SECONDS)
        }
    }

    @AfterAll
    fun stop() {
        opened.forEach { it.close() }
        client.close()
        mongo.stop()
        kafka.stop()
    }

    private val instances = AtomicInteger()

    /**
     * Each instance's client id sorts before the earlier instances' (the range assignor hands partitions out in member order), so a
     * new instance takes partitions from the earlier ones and an earlier holder is left without any: the kit then sees that
     * instance's revocation closed by an empty assignment (with Kafka's default client ids the earliest members keep them).
     */
    override fun eventStore(): EventStore {
        val clientId = "kit-%04d".format(9_999 - instances.incrementAndGet())
        return KafkaEventStore(KafkaEventStoreSettings(bootstrapServers = kafka.bootstrapServers, clientProperties = mapOf("client.id" to clientId)))
            .also { synchronized(opened) { opened += it } }
    }

    override fun messageStore(): MessageStore {
        val template = MongoTemplate(SimpleMongoClientDatabaseFactory(client, "store_" + UUID.randomUUID().toString().replace("-", "")))
        return MongoMessageStore(template).also { it.ensureIndexes() }
    }
}
