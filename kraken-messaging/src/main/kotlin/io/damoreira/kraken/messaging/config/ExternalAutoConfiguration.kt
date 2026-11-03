package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.external.ExternalCallIndexes
import io.damoreira.kraken.messaging.external.ExternalCallSettings
import io.damoreira.kraken.messaging.external.ExternalEventIngress
import io.damoreira.kraken.messaging.external.ExternalKafkaSources
import io.damoreira.kraken.messaging.external.ExternalRecordTranslator
import io.damoreira.kraken.messaging.external.IdempotencyInterceptor
import io.damoreira.kraken.messaging.external.WebhookSignatureVerifier
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock

/**
 * The integration toolkit's edges (S1 plan, Task 13), on top of [KafkaEventsAutoConfiguration]'s local dispatcher:
 *
 * - the [WebhookSignatureVerifier] and the [ExternalEventIngress], always (they are passive: a service uses them from its own endpoint);
 * - [ExternalKafkaSources], only when `kraken.messaging.external.kafka.sources.<name>` is configured; each source needs an
 *   [ExternalRecordTranslator] bean named `<name>`, else the startup fails naming the source;
 * - the `external_calls` indexes and the [IdempotencyInterceptor] (which `HandlerInvoker` then uses for `@IdempotentHandler`
 *   methods), only when the application has an `@IdempotentHandler` method (scanned from its packages) or
 *   `kraken.messaging.external.calls.enabled=true`.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class, KafkaEventsAutoConfiguration::class],
    afterName = ["org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"],
)
@ConditionalOnClass(MongoTemplate::class)
@ConditionalOnBean(LocalEventDispatcher::class)
class ExternalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun webhookSignatureVerifier() = WebhookSignatureVerifier()

    @Bean
    @ConditionalOnMissingBean
    fun externalEventIngress(
        dispatcher: LocalEventDispatcher,
        mongo: MongoTemplate,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        clock: ObjectProvider<Clock>,
    ) = ExternalEventIngress(dispatcher, mongo, codec, typeResolver, clock.getIfAvailable { Clock.systemUTC() })

    @Bean
    @ConditionalOnMissingBean
    @Conditional(OnExternalKafkaSources::class)
    fun externalKafkaSources(
        properties: KrakenMessagingProperties,
        beans: ListableBeanFactory,
        dispatcher: LocalEventDispatcher,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
    ) = ExternalKafkaSources(
        sources = properties.external.kafka.sources,
        translators = beans.getBeansOfType(ExternalRecordTranslator::class.java), // bean name -> translator: a source `printer` needs the bean named `printer`
        dispatcher = dispatcher,
        codec = codec,
        typeResolver = typeResolver,
        serviceName = properties.serviceName,
        defaultBootstrapServers = properties.kafka.bootstrapServers,
        maxAttempts = properties.kafka.consumer.maxAttempts,
    )

    @Configuration(proxyBeanMethods = false)
    @Conditional(OnIdempotentHandlers::class)
    @Import(KrakenClockConfiguration::class)
    class IdempotentHandlersConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun externalCallIndexes(mongo: MongoTemplate, properties: KrakenMessagingProperties) =
            ExternalCallIndexes(mongo, properties.external.calls.retention)

        /** Its bookkeeping writes use the Mongo transaction manager (REQUIRES_NEW), so a rolled-back caller never forgets a call that happened. */
        @Bean
        @ConditionalOnMissingBean
        fun idempotencyInterceptor(
            mongo: MongoTemplate,
            codec: JsonCodec,
            txManager: MongoTransactionManager,
            clock: Clock,
            properties: KrakenMessagingProperties,
            metrics: ObjectProvider<SagaMetrics>,
        ) = IdempotencyInterceptor(
            mongo, codec, txManager, clock, ExternalCallSettings(properties.external.calls.lease), metrics.getIfAvailable { NoopSagaMetrics },
        )
    }
}
