package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStoreSettings
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import io.damoreira.kraken.messaging.store.memory.MemoryTransactionManager
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.transaction.TransactionManager
import java.time.Clock

/**
 * The in-memory stores of the core (ADR 0027, ADR 0028), for tests, local runs and single-process demos: **not durable**, one
 * JVM. Each runs only when its setting names it, never because nothing else is there:
 *
 * - `kraken.messaging.event-store=memory`: the [MemoryEventStore] (`kraken.messaging.memory.segments` segments per source).
 * - `kraken.messaging.store=memory`: the [MemoryMessageStore], and its [MemoryTransactionManager] as the application's
 *   transaction manager when it has none (so its `@Transactional` methods and the outbox commit together).
 *
 * Each logs at WARN when it starts that it is not durable, naming the active Spring profiles (`default` when none):
 * `in-memory event store: not durable (profile local)`, `in-memory message store: not durable (profile local)`. An event-store or
 * message-store add-on on the classpath steps back when its setting names `memory`.
 *
 * Ordered after the auto-configurations that create the application's own transaction manager (JDBC, JPA), so the in-memory one
 * never takes their place, and before Boot's transaction auto-configuration and the core's configurations that need a store.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class],
    before = [EventsAutoConfiguration::class, SagaAutoConfiguration::class, ExternalAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration",
    ],
    beforeName = ["org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration"],
)
class MemoryStoresAutoConfiguration {
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = [EventStoreGuard.EVENT_STORE], havingValue = MemoryEventStore.NAME)
    class MemoryEventStoreConfiguration {
        @Bean
        @ConditionalOnMissingBean(EventStore::class)
        fun memoryEventStore(properties: KrakenMessagingProperties, clock: ObjectProvider<Clock>, environment: Environment): MemoryEventStore {
            log.warn("in-memory event store: not durable (profile {})", profiles(environment))
            return MemoryEventStore(MemoryEventStoreSettings(properties.memory.segments), clock.getIfAvailable { Clock.systemUTC() })
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = [MessageStoreGuard.STORE], havingValue = MemoryMessageStore.NAME)
    class MemoryMessageStoreConfiguration {
        @Bean
        @ConditionalOnMissingBean(MessageStore::class)
        fun memoryMessageStore(clock: ObjectProvider<Clock>, environment: Environment): MemoryMessageStore {
            log.warn("in-memory message store: not durable (profile {})", profiles(environment))
            return MemoryMessageStore(clock.getIfAvailable { Clock.systemUTC() })
        }

        /** The store's transactions back the application's `@Transactional` methods too, unless it has a transaction manager of its own. */
        @Bean
        @ConditionalOnBean(MemoryMessageStore::class)
        @ConditionalOnMissingBean(TransactionManager::class)
        fun memoryTransactionManager(store: MemoryMessageStore): MemoryTransactionManager = store.transactions as MemoryTransactionManager
    }

    companion object {
        private val log = LoggerFactory.getLogger(MemoryStoresAutoConfiguration::class.java)

        /** The active Spring profiles, comma-separated; `default` when none is active. */
        internal fun profiles(environment: Environment): String =
            environment.activeProfiles.ifEmpty { arrayOf("default") }.joinToString(",")
    }
}
