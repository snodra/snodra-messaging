package com.damoreira.snodra.messaging.kafka

import com.damoreira.snodra.messaging.config.ConditionalOnNoApplicationEventStore
import com.damoreira.snodra.messaging.config.EventsAutoConfiguration
import com.damoreira.snodra.messaging.config.ExternalAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingProperties
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.external.ExternalSourceProviders
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.core.type.AnnotatedTypeMetadata

/**
 * The Kafka [EventStore] from `snodra.messaging.kafka.*` ([KafkaProperties]), unless the application has an event
 * store of its own, or `snodra.messaging.event-store` names another one (when two event-store add-ons are on the classpath). It
 * connects lazily: the producer on the first publish, a consumer per subscription.
 *
 * With `snodra.messaging.external.kafka.sources.<name>` ([KafkaExternalProperties]) it also provides those partner topics as
 * the core's external sources ([KafkaExternalSource], read by the core's `ExternalSources`), whatever the service's own
 * event store is.
 */
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class], before = [EventsAutoConfiguration::class, ExternalAutoConfiguration::class])
@ConditionalOnClass(KafkaEventStore::class)
@EnableConfigurationProperties(KafkaProperties::class, KafkaExternalProperties::class)
class KafkaEventStoreAutoConfiguration {
    /** Steps back for an event store the application defines, not for another add-on's: two add-ons need the setting (EventStoreAddOnsCheck). */
    @Bean
    @ConditionalOnNoApplicationEventStore
    @ConditionalOnProperty(name = ["snodra.messaging.event-store"], havingValue = KafkaEventStore.NAME, matchIfMissing = true)
    fun kafkaEventStore(properties: KafkaProperties): KafkaEventStore = KafkaEventStore(properties.toSettings())

    /** The partner topics, by source name; only when one is configured. */
    @Bean
    @Conditional(OnKafkaExternalSources::class)
    fun snodraKafkaExternalSources(
        external: KafkaExternalProperties,
        kafka: KafkaProperties,
        messaging: SnodraMessagingProperties,
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

    /**
     * Masks the Kafka client properties (a `sasl.jaas.config` holds a secret) and the partner sources' properties in the actuator's
     * `configprops` and `env` output, whatever `show-values` is.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.springframework.boot.actuate.endpoint.SanitizingFunction"])
    class KafkaSecretsSanitizingConfiguration {
        @Bean
        fun snodraKafkaSecretsSanitizingFunction(): SanitizingFunction = SanitizingFunction { data ->
            if (KafkaSecretKeys.matches(data.key)) data.withSanitizedValue() else data
        }
    }
}

/** The settings whose values may hold Kafka secrets, in every form (property, relaxed environment variable, configprops key). */
internal object KafkaSecretKeys {
    private const val CLIENT_PROPERTIES = "snodramessagingkafkaclientproperties"
    private val partnerProperties = Regex("snodramessagingexternalkafkasources[a-z0-9]*properties.*")

    fun matches(key: String?): Boolean {
        val normalized = key?.lowercase()?.filter { it.isLetterOrDigit() } ?: return false
        return normalized.startsWith(CLIENT_PROPERTIES) || partnerProperties.matches(normalized)
    }
}

/** True when at least one `snodra.messaging.external.kafka.sources.<name>` is configured. */
internal class OnKafkaExternalSources : Condition {
    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean =
        Binder.get(context.environment)
            .bind("snodra.messaging.external.kafka.sources", Bindable.mapOf(String::class.java, Any::class.java))
            .map { it.isNotEmpty() }.orElse(false) == true
}
