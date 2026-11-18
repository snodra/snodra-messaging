package com.damoreira.snodra.messaging.events

import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.kafka.KafkaEventStore
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreSettings
import com.damoreira.snodra.messaging.mongo.MongoMessageStore
import com.damoreira.snodra.messaging.test.SnodraContainers
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Kafka and MongoDB (Testcontainers, reused) for the event-processor tests: the Kafka event store (the add-on), topics,
 * producing, committed offsets.
 */
class KafkaMongoSupport(database: String) {
    val kafkaContainer: org.testcontainers.kafka.KafkaContainer = SnodraContainers.kafka().also { it.start() }
    val mongoContainer: org.testcontainers.mongodb.MongoDBContainer = SnodraContainers.mongo().also { it.start() }
    private val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), database)
    val mongo = MongoTemplate(factory)
    /** The MongoDB message store over [mongo], its indexes created as at startup. */
    val store = MongoMessageStore(mongo, MongoTransactionManager(factory)).also { it.ensureIndexes() }
    val tx = TransactionTemplate(store.transactions)

    init {
        // Two processors' first transactions must not both create the collection implicitly (MongoDB makes the second wait
        // for the first's lock, stalling both consumers).
        for (name in listOf("processed_events", "dead_letters")) if (!mongo.collectionExists(name)) mongo.createCollection(name)
    }
    val codec = JsonCodec()
    val bootstrapServers: String = kafkaContainer.getBootstrapServers()
    val kafka = KafkaProducer(
        mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        ),
        StringSerializer(), ByteArraySerializer(),
    )

    /** The Kafka event store over the container (one per test class; its consumers start with each subscription). */
    val events = KafkaEventStore(KafkaEventStoreSettings(bootstrapServers))

    fun admin(): AdminClient = AdminClient.create(mapOf<String, Any>("bootstrap.servers" to bootstrapServers))

    fun newTopic(prefix: String, partitions: Int = 1): String {
        val topic = "$prefix-" + UUID.randomUUID()
        admin().use { it.createTopics(listOf(NewTopic(topic, partitions, 1.toShort()))).all().get() }
        return topic
    }

    fun ensureTopic(topic: String, partitions: Int = 1) {
        admin().use {
            try {
                it.createTopics(listOf(NewTopic(topic, partitions, 1.toShort()))).all().get()
            } catch (e: java.util.concurrent.ExecutionException) {
                if (e.cause !is org.apache.kafka.common.errors.TopicExistsException) throw e
            }
        }
    }

    /** Produces an event of [type] (`test.Happened` payload unless [payload] is given) and returns its offset. */
    fun send(
        topic: String, id: String?, type: String? = "test.Happened", key: String? = "k", partition: Int? = null,
        payload: ByteArray? = codec.encode(Happened(id ?: "none", Instant.now())), extra: Map<String, String> = emptyMap(),
    ): Long {
        val record = ProducerRecord<String, ByteArray>(topic, partition, key, payload)
        if (type != null) record.headers().add(RecordHeader("type", type.toByteArray()))
        if (id != null) record.headers().add(RecordHeader("id", id.toByteArray()))
        extra.forEach { (k, v) -> record.headers().add(RecordHeader(k, v.toByteArray())) }
        return kafka.send(record).get().offset()
    }

    /** The committed offsets of [group] by partition. */
    fun committed(group: String): Map<Int, Long> = admin().use { admin ->
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().entries.associate { it.key.partition() to it.value.offset() }
    }

    /** Every record of [topic] (from the start, waiting up to [timeout] for at least [atLeast]). */
    fun readAll(topic: String, atLeast: Int, timeout: Duration = Duration.ofSeconds(15)): List<ConsumerRecord<String, ByteArray>> {
        val props = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        )
        KafkaConsumer<String, ByteArray>(props).use { c ->
            val tps = c.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            c.assign(tps)
            c.seekToBeginning(tps)
            val out = mutableListOf<ConsumerRecord<String, ByteArray>>()
            val deadline = System.nanoTime() + timeout.toNanos()
            while (out.size < atLeast && System.nanoTime() < deadline) out += c.poll(Duration.ofMillis(200))
            return out
        }
    }

    fun close() {
        events.close()
        kafka.close()
        kafkaContainer.stop()
        mongoContainer.stop()
    }
}
