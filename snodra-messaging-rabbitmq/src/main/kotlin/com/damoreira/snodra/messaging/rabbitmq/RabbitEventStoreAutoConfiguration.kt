package com.damoreira.snodra.messaging.rabbitmq

import com.damoreira.snodra.messaging.config.ConditionalOnNoApplicationEventStore
import com.damoreira.snodra.messaging.config.EventsAutoConfiguration
import com.damoreira.snodra.messaging.config.ExternalAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * The RabbitMQ Streams [com.damoreira.snodra.messaging.eventstore.EventStore] (ADR 0027) from `snodra.messaging.rabbitmq.*`
 * ([RabbitProperties]), unless the application has an event store of its own, or `snodra.messaging.event-store` names another
 * one (needed when two event-store add-ons are on the classpath). It connects lazily (the publisher on the first publish, the
 * consumers per subscription); with the management API configured it asks it once at startup ([RabbitEventStore.open]).
 */
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class], before = [EventsAutoConfiguration::class, ExternalAutoConfiguration::class])
@ConditionalOnClass(RabbitEventStore::class)
@EnableConfigurationProperties(RabbitProperties::class)
class RabbitEventStoreAutoConfiguration {
    /** Steps back for an event store the application defines, not for another add-on's: two add-ons need the setting (EventStoreAddOnsCheck). */
    @Bean
    @ConditionalOnNoApplicationEventStore
    @ConditionalOnProperty(name = ["snodra.messaging.event-store"], havingValue = RabbitEventStore.NAME, matchIfMissing = true)
    fun rabbitEventStore(properties: RabbitProperties): RabbitEventStore = RabbitEventStore.open(properties.toSettings())

    /** Masks the stream user's and the management API's passwords in the actuator's `configprops` and `env` output, whatever `show-values` is. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.springframework.boot.actuate.endpoint.SanitizingFunction"])
    class RabbitSecretsSanitizingConfiguration {
        @Bean
        fun snodraRabbitSecretsSanitizingFunction(): SanitizingFunction = SanitizingFunction { data ->
            if (RabbitSecretKeys.matches(data.key)) data.withSanitizedValue() else data
        }
    }
}

/** The passwords' names in every form (property, relaxed environment variable, configprops key); their files are paths and stay visible. */
internal object RabbitSecretKeys {
    private val secrets = setOf("snodramessagingrabbitmqpassword", "snodramessagingrabbitmqmanagementpassword")

    fun matches(key: String?): Boolean = key?.lowercase()?.filter { it.isLetterOrDigit() } in secrets
}
