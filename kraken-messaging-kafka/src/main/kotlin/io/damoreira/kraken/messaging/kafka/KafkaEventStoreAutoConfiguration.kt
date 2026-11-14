package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.eventstore.EventStore
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean

/**
 * The Kafka [EventStore] (ADR 0027) from `kraken.messaging.kafka.bootstrap-servers`, unless the application has an event store
 * of its own. It connects lazily: the producer on the first publish, a consumer per subscription.
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class])
@ConditionalOnClass(KafkaEventStore::class)
class KafkaEventStoreAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(EventStore::class)
    fun kafkaEventStore(properties: KrakenMessagingProperties): KafkaEventStore =
        KafkaEventStore(KafkaEventStoreSettings(bootstrapServers = properties.kafka.bootstrapServers))
}
