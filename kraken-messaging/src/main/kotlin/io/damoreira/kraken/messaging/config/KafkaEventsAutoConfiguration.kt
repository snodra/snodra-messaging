package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.kafka.EventConsumer
import io.damoreira.kraken.messaging.kafka.MessagingIndexes
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.kafka.OutboxPublisher
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.transaction.support.TransactionTemplate

/**
 * Events over Kafka with the transactional outbox: [OutboxEventBus] as the [EventBus], [OutboxPublisher] relaying the
 * `outbox` collection to the topic, [EventConsumer] running `@EventHandler`s, and the TTL/relay indexes.
 *
 * Needs the application's [MongoTemplate] (Boot's Mongo auto-configuration provides it when
 * `spring.mongodb.uri` is set; Spring Boot 4 no longer binds `spring.data.mongodb.uri`), hence ordered after it. The [MongoTransactionManager] (when the application has
 * none) also backs the application's `@Transactional` methods, so business writes and outbox inserts commit together. The Kafka factories and template are private to the
 * starter (`defaultCandidate = false`, injected by name): they neither satisfy nor suppress the application's own
 * Kafka beans.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration",
        "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
    ],
    // Before Boot's transaction auto-configuration, so the MongoTransactionManager below exists when it decides whether
    // to enable @Transactional support and its own TransactionTemplate.
    beforeName = ["org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration"],
)
@ConditionalOnClass(KafkaTemplate::class, MongoTemplate::class)
@ConditionalOnBean(MongoTemplate::class)
class KafkaEventsAutoConfiguration {

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [PRODUCER_FACTORY])
    fun krakenKafkaProducerFactory(properties: KrakenMessagingProperties): ProducerFactory<String, ByteArray> =
        DefaultKafkaProducerFactory(
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ),
        )

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [KAFKA_TEMPLATE])
    fun krakenKafkaTemplate(@Qualifier(PRODUCER_FACTORY) producerFactory: ProducerFactory<String, ByteArray>) =
        KafkaTemplate(producerFactory)

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [CONSUMER_FACTORY])
    fun krakenKafkaConsumerFactory(properties: KrakenMessagingProperties): ConsumerFactory<String, ByteArray> =
        DefaultKafkaConsumerFactory(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to properties.kafka.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to properties.serviceName,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ),
        )

    @Bean
    @ConditionalOnMissingBean
    fun mongoTransactionManager(databaseFactory: MongoDatabaseFactory) = MongoTransactionManager(databaseFactory)

    @Bean
    @ConditionalOnMissingBean
    fun transactionTemplate(transactionManager: MongoTransactionManager) = TransactionTemplate(transactionManager)

    @Bean
    @ConditionalOnMissingBean(EventBus::class)
    fun outboxEventBus(mongo: MongoTemplate, codec: JsonCodec, typeResolver: MessageTypeResolver, traceHeaders: TraceHeaders): EventBus =
        OutboxEventBus(mongo, codec, typeResolver, traceHeaders)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kraken.messaging.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun outboxPublisher(
        mongo: MongoTemplate,
        @Qualifier(KAFKA_TEMPLATE) kafka: KafkaTemplate<String, ByteArray>,
        properties: KrakenMessagingProperties,
    ) = OutboxPublisher(
        mongo, kafka,
        topic = properties.kafka.topic,
        source = properties.serviceName,
        pollInterval = properties.outbox.pollInterval,
        batchSize = properties.outbox.batchSize,
    )

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kraken.messaging.kafka.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun eventConsumer(
        @Qualifier(CONSUMER_FACTORY) consumerFactory: ConsumerFactory<String, ByteArray>,
        registry: HandlerRegistry,
        invoker: HandlerInvoker,
        tx: TransactionTemplate,
        mongo: MongoTemplate,
        codec: JsonCodec,
        properties: KrakenMessagingProperties,
    ) = EventConsumer(
        consumerFactory,
        topic = properties.kafka.topic,
        groupId = properties.serviceName,
        registry = registry,
        invoker = invoker,
        tx = tx,
        mongo = mongo,
        maxAttempts = properties.kafka.consumer.maxAttempts,
        codec = codec,
    )

    @Bean
    @ConditionalOnMissingBean
    fun messagingIndexes(mongo: MongoTemplate, properties: KrakenMessagingProperties) =
        MessagingIndexes(mongo, properties.outbox.retention, properties.idempotency.retention)

    companion object {
        const val PRODUCER_FACTORY = "krakenKafkaProducerFactory"
        const val KAFKA_TEMPLATE = "krakenKafkaTemplate"
        const val CONSUMER_FACTORY = "krakenKafkaConsumerFactory"
    }
}
