package io.damoreira.kraken.messaging.rabbitmq

import io.damoreira.kraken.messaging.config.ConditionalOnNoApplicationEventStore
import io.damoreira.kraken.messaging.config.EventsAutoConfiguration
import io.damoreira.kraken.messaging.config.ExternalAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * The RabbitMQ Streams [io.damoreira.kraken.messaging.eventstore.EventStore] (ADR 0027) from `kraken.messaging.rabbitmq.*`
 * ([RabbitProperties]), unless the application has an event store of its own, or `kraken.messaging.event-store` names another
 * one (needed when two event-store add-ons are on the classpath). It connects lazily (the publisher on the first publish, the
 * consumers per subscription); with the management API configured it asks it once at startup ([RabbitEventStore.open]).
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class], before = [EventsAutoConfiguration::class, ExternalAutoConfiguration::class])
@ConditionalOnClass(RabbitEventStore::class)
@EnableConfigurationProperties(RabbitProperties::class)
class RabbitEventStoreAutoConfiguration {
    /** Steps back for an event store the application defines, not for another add-on's: two add-ons need the setting (EventStoreAddOnsCheck). */
    @Bean
    @ConditionalOnNoApplicationEventStore
    @ConditionalOnProperty(name = ["kraken.messaging.event-store"], havingValue = RabbitEventStore.NAME, matchIfMissing = true)
    fun rabbitEventStore(properties: RabbitProperties): RabbitEventStore = RabbitEventStore.open(properties.toSettings())
}
