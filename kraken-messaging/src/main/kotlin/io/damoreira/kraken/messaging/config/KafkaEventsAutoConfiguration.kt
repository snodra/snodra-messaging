package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.control.ProcessorControl
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.deadletter.DeadLetterFactory
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.handler.Processors
import io.damoreira.kraken.messaging.kafka.DeadLetterSink
import io.damoreira.kraken.messaging.kafka.EventConsumer
import io.damoreira.kraken.messaging.kafka.EventProcessors
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.kafka.OutboxPublisher
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.time.Clock

/**
 * Events over Kafka with the transactional outbox: [OutboxEventBus] as the [EventBus], [OutboxPublisher] relaying the
 * outbox to the topic, [EventProcessors] running the `@EventHandler`s (one [EventConsumer] per processor, ADR 0022), the
 * dead letters ([DeadLetters]) and the processor control ([ProcessorControl], P6b).
 *
 * Needs a [MessageStore] (ADR 0028): a store add-on's (`kraken-messaging-mongo` provides one over the application's
 * MongoDB, ordered before this configuration) or the application's own bean. Without one none of this exists, and an event
 * handler, a saga or an [EventBus] injection fails the startup naming the missing store ([MessageStoreGuard],
 * [MissingMessageStoreFailureAnalyzer]). The outbox entries and the markers join the store's transactions, which the add-on
 * also gives the application's `@Transactional` methods, so business writes and outbox entries commit together. The Kafka
 * factories and template are private to the starter (`defaultCandidate = false`, injected by name): they neither satisfy
 * nor suppress the application's own Kafka beans.
 *
 * The relay, the dispatcher and the consumers report to the application's [MessagingMetrics] bean (the Micrometer one of
 * [MessagingMetricsAutoConfiguration] when a registry exists), else to [NoopMessagingMetrics].
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class],
    afterName = ["org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"],
)
@ConditionalOnClass(KafkaTemplate::class)
@ConditionalOnBean(MessageStore::class)
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
                // Bound a send: metadata/buffer wait, then delivery including retries (the publisher waits for both).
                // delivery.timeout.ms must be >= linger.ms (5 ms default) + request.timeout.ms, hence 20 s per request.
                ProducerConfig.MAX_BLOCK_MS_CONFIG to 10_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 30_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to 20_000,
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
    @ConditionalOnMissingBean(EventBus::class)
    fun outboxEventBus(store: MessageStore, codec: JsonCodec, typeResolver: MessageTypeResolver, traceHeaders: TraceHeaders): OutboxEventBus =
        OutboxEventBus(store.outbox, codec, typeResolver, traceHeaders)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kraken.messaging.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun outboxPublisher(
        store: MessageStore,
        @Qualifier(KAFKA_TEMPLATE) kafka: KafkaTemplate<String, ByteArray>,
        properties: KrakenMessagingProperties,
        metrics: ObjectProvider<MessagingMetrics>,
    ) = OutboxPublisher(
        store.outbox, kafka,
        topic = properties.kafka.topic,
        source = properties.serviceName,
        pollInterval = properties.outbox.pollInterval,
        batchSize = properties.outbox.batchSize,
        maxAttempts = properties.outbox.maxAttempts,
        metrics = metrics.getIfAvailable { NoopMessagingMetrics },
        lease = properties.outbox.lease,
    )

    /** Runs an event's ordinary handlers, then the saga stage when the saga runtime provides one (none: [SagaStage.NONE]). */
    @Bean
    @ConditionalOnMissingBean
    fun localEventDispatcher(
        registry: HandlerRegistry,
        invoker: HandlerInvoker,
        store: MessageStore,
        sagaStage: ObjectProvider<SagaStage>,
        metrics: ObjectProvider<MessagingMetrics>,
    ) = LocalEventDispatcher(registry, invoker, store, sagaStage.getIfAvailable { SagaStage.NONE }, metrics.getIfAvailable { NoopMessagingMetrics })

    /** The dead letters' admin API over the store's dead letters; in modes `topic` and `log` every call fails naming the mode. */
    @Bean
    @ConditionalOnMissingBean
    fun deadLetters(properties: KrakenMessagingProperties, store: MessageStore, dispatcher: LocalEventDispatcher): DeadLetters {
        val mode = properties.kafka.deadLetter.mode
        return DeadLetters(mode, store.deadLetters.takeIf { mode == DeadLetterMode.COLLECTION }, dispatcher)
    }

    /**
     * The control of the event processors (P6b, ADR 0023): what their `processor_control` documents say, applied at start and
     * every 5 s, and the hub's control requests (pause, resume, reset, dead letters). Its Kafka `Admin` client (a reset's
     * offsets) shares the consumer factory's connection settings.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kraken.messaging.kafka.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun processorControl(
        @Qualifier(CONSUMER_FACTORY) consumerFactory: ConsumerFactory<String, ByteArray>,
        store: MessageStore,
        registry: HandlerRegistry,
        deadLetters: DeadLetters,
    ) = ProcessorControl(store.processorControl, registry, deadLetters, consumerFactory.configurationProperties)

    /**
     * The event processors (ADR 0022): one consumer per processor that runs here, `default` in `kafka.group-id` (else the
     * service name), a named one in `<that group>.<name>`. The `Admin` client of their status shares the consumer factory's
     * connection settings.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "kraken.messaging.kafka.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun eventProcessors(
        @Qualifier(CONSUMER_FACTORY) consumerFactory: ConsumerFactory<String, ByteArray>,
        @Qualifier(KAFKA_TEMPLATE) kafka: KafkaTemplate<String, ByteArray>,
        codec: JsonCodec,
        dispatcher: LocalEventDispatcher,
        properties: KrakenMessagingProperties,
        store: MessageStore,
        clock: ObjectProvider<Clock>,
        control: ObjectProvider<ProcessorControl>,
        metrics: ObjectProvider<MessagingMetrics>,
    ): EventProcessors {
        val sink = when (properties.kafka.deadLetter.mode) {
            DeadLetterMode.COLLECTION ->
                DeadLetterSink.Collection(store.deadLetters, DeadLetterFactory(properties.kafka.deadLetter.retention, clock.getIfAvailable { Clock.systemUTC() }))
            DeadLetterMode.TOPIC -> DeadLetterSink.Topic(kafka)
            DeadLetterMode.LOG -> DeadLetterSink.Log
        }
        val serviceGroup = properties.kafka.serviceGroupId(properties.serviceName)
        val messagingMetrics = metrics.getIfAvailable { NoopMessagingMetrics }
        return EventProcessors(
            dispatcher,
            consumerFor = { processor ->
                EventConsumer(
                    consumerFactory,
                    topic = properties.kafka.topic,
                    groupId = Processors.groupId(serviceGroup, processor),
                    dispatcher = dispatcher,
                    maxAttempts = properties.kafka.consumer.maxAttempts,
                    codec = codec,
                    processor = processor,
                    deadLetters = sink,
                    metrics = messagingMetrics,
                )
            },
            adminConfig = consumerFactory.configurationProperties,
            control = control.getIfAvailable(),
        )
    }

    companion object {
        const val PRODUCER_FACTORY = "krakenKafkaProducerFactory"
        const val KAFKA_TEMPLATE = "krakenKafkaTemplate"
        const val CONSUMER_FACTORY = "krakenKafkaConsumerFactory"
    }
}
