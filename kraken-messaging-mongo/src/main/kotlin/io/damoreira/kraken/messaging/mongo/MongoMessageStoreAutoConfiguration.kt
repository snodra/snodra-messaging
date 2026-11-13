package io.damoreira.kraken.messaging.mongo

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.store.MessageStore
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock

/**
 * The MongoDB [MessageStore] (ADR 0028), when the application has a [MongoTemplate] and no store of its own. The retentions
 * come from `kraken.messaging.*.retention`; its indexes are created at startup unless `kraken.messaging.store.init-schema`
 * is false.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class],
    afterName = ["org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"],
)
@ConditionalOnBean(MongoTemplate::class)
class MongoMessageStoreAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(MessageStore::class)
    fun mongoMessageStore(
        mongo: MongoTemplate,
        transactionManager: ObjectProvider<MongoTransactionManager>,
        properties: ObjectProvider<KrakenMessagingProperties>,
        clock: ObjectProvider<Clock>,
        environment: Environment,
    ): MongoMessageStore {
        val store = MongoMessageStore(
            mongo,
            transactionManager.getIfAvailable { MongoTransactionManager(mongo.mongoDatabaseFactory) },
            properties.getIfAvailable()?.let(::settingsOf) ?: MongoStoreSettings(),
            clock.getIfAvailable { Clock.systemUTC() },
        )
        if (environment.getProperty("kraken.messaging.store.init-schema", Boolean::class.java, true)) store.ensureIndexes()
        return store
    }

    private fun settingsOf(p: KrakenMessagingProperties) = MongoStoreSettings(
        outboxRetention = p.outbox.retention,
        idempotencyRetention = p.idempotency.retention,
        externalCallRetention = p.external.calls.retention,
        sagaEndedRetention = p.sagas.endedRetention,
        sagaDispatchRetention = p.sagas.dispatchRetention,
        deadlineRetention = p.deadlines.retention,
        failedRetention = p.sagas.failedRetention,
    )
}
