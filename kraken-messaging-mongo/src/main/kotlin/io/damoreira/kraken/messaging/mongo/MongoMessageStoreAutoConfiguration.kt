package io.damoreira.kraken.messaging.mongo

import io.damoreira.kraken.messaging.config.EventsAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.store.MessageStore
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock

/**
 * The MongoDB [MessageStore] (ADR 0028), when the application has a [MongoTemplate] and no store of its own, unless
 * `kraken.messaging.store` names another one (`memory`, for example: then neither the store nor its transaction manager is
 * created here). The retentions
 * come from `kraken.messaging.*.retention`; the core creates its indexes at startup ([MongoMessageStore.initSchema]) unless
 * `kraken.messaging.store.init-schema` is false.
 *
 * The [MongoStoreTransactionManager] (when the application has no [MongoTransactionManager] of its own) is the store's transactions and also backs the application's
 * `@Transactional` methods, so business writes, outbox entries and markers commit together. Ordered before Boot's transaction
 * auto-configuration (which then enables `@Transactional` over it) and before the core's configurations that need a store.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class],
    before = [EventsAutoConfiguration::class],
    afterName = ["org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"],
    beforeName = ["org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration"],
)
@ConditionalOnBean(MongoTemplate::class)
@ConditionalOnProperty(name = ["kraken.messaging.store"], havingValue = MongoMessageStore.NAME, matchIfMissing = true)
class MongoMessageStoreAutoConfiguration {
    /** The store's transaction manager, which reports a transient commit failure as such ([MongoStoreTransactionManager]). */
    @Bean
    @ConditionalOnMissingBean
    fun mongoTransactionManager(databaseFactory: MongoDatabaseFactory): MongoTransactionManager = MongoStoreTransactionManager(databaseFactory)

    @Bean
    @ConditionalOnMissingBean(MessageStore::class)
    fun mongoMessageStore(
        mongo: MongoTemplate,
        transactionManager: ObjectProvider<MongoTransactionManager>,
        properties: ObjectProvider<KrakenMessagingProperties>,
        clock: ObjectProvider<Clock>,
    ): MongoMessageStore = MongoMessageStore(
        mongo,
        transactionManager.getIfAvailable { MongoStoreTransactionManager(mongo.mongoDatabaseFactory) },
        properties.getIfAvailable()?.let(::settingsOf) ?: MongoStoreSettings(),
        clock.getIfAvailable { Clock.systemUTC() },
    )

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
