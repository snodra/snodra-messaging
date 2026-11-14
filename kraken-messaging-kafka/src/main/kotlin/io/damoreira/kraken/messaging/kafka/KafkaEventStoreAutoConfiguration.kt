package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.config.EventsAutoConfiguration
import io.damoreira.kraken.messaging.config.ExternalAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.external.ExternalSourceProviders
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.core.type.AnnotatedTypeMetadata

/**
 * The Kafka [EventStore] (ADR 0027) from `kraken.messaging.kafka.*` ([KafkaProperties]), unless the application has an event
 * store of its own, or `kraken.messaging.event-store` names another one (when two event-store add-ons are on the classpath). It
 * connects lazily: the producer on the first publish, a consumer per subscription.
 *
 * With `kraken.messaging.external.kafka.sources.<name>` ([KafkaExternalProperties]) it also provides those partner topics as
 * the core's external sources ([KafkaExternalSource], read by the core's `ExternalSources`), whatever the service's own
 * event store is.
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class], before = [EventsAutoConfiguration::class, ExternalAutoConfiguration::class])
@ConditionalOnClass(KafkaEventStore::class)
@EnableConfigurationProperties(KafkaProperties::class, KafkaExternalProperties::class)
class KafkaEventStoreAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(EventStore::class)
    @ConditionalOnProperty(name = ["kraken.messaging.event-store"], havingValue = KafkaEventStore.NAME, matchIfMissing = true)
    fun kafkaEventStore(properties: KafkaProperties): KafkaEventStore = KafkaEventStore(properties.toSettings())

    /** The partner topics, by source name; only when one is configured. */
    @Bean
    @Conditional(OnKafkaExternalSources::class)
    fun krakenKafkaExternalSources(
        external: KafkaExternalProperties,
        kafka: KafkaProperties,
        messaging: KrakenMessagingProperties,
    ): ExternalSourceProviders {
        val sources = external.sources.mapValues { (name, source) ->
            KafkaExternalSource(
                name = name,
                topic = source.topic,
                groupId = source.groupId ?: "${messaging.serviceName}.external.$name",
                bootstrapServers = source.bootstrapServers ?: kafka.bootstrapServers,
                properties = source.properties,
                settings = kafka.toSettings().copy(bootstrapServers = source.bootstrapServers ?: kafka.bootstrapServers, clientProperties = emptyMap()),
            )
        }
        return ExternalSourceProviders { sources }
    }
}

/** True when at least one `kraken.messaging.external.kafka.sources.<name>` is configured. */
internal class OnKafkaExternalSources : Condition {
    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean =
        Binder.get(context.environment)
            .bind("kraken.messaging.external.kafka.sources", Bindable.mapOf(String::class.java, Any::class.java))
            .map { it.isNotEmpty() }.orElse(false) == true
}
