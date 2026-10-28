package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.Query
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

/**
 * Skeleton auto-configuration. Every bean is a stub that fails fast; the Kafka and hub
 * transports are implemented in the next phase (see ../kraken/docs/02-messaging.md).
 */
@AutoConfiguration
@EnableConfigurationProperties(KrakenMessagingProperties::class)
class KrakenMessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun eventBus(): EventBus = object : EventBus {
        override suspend fun publish(event: Event, key: String?) = throw NotImplementedError("skeleton")
    }

    @Bean
    @ConditionalOnMissingBean
    fun commandBus(): CommandBus = object : CommandBus {
        override suspend fun <R> send(command: Command<R>, routingKey: String?): R = throw NotImplementedError("skeleton")
    }

    @Bean
    @ConditionalOnMissingBean
    fun queryBus(): QueryBus = object : QueryBus {
        override suspend fun <R> send(query: Query<R>): R = throw NotImplementedError("skeleton")
    }
}
