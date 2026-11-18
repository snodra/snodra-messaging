package com.damoreira.snodra.messaging.mongo

import com.damoreira.snodra.messaging.config.EventsAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingProperties
import com.damoreira.snodra.messaging.config.ConditionalOnNoApplicationMessageStore
import com.damoreira.snodra.messaging.store.MessageStore
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
 * `snodra.messaging.store.type` names another one (`memory`, for example: then neither the store nor its transaction manager is
 * created here). The retentions
 * come from `snodra.messaging.*.retention`; the core creates its indexes at startup ([MongoMessageStore.initSchema]) unless
 * `snodra.messaging.store.init-schema` is false.
 *
 * The [MongoStoreTransactionManager] (when the application has no [MongoTransactionManager] of its own) is the store's transactions and also backs the application's
 * `@Transactional` methods, so business writes, outbox entries and markers commit together. Ordered before Boot's transaction
 * auto-configuration (which then enables `@Transactional` over it) and before the core's configurations that need a store.
 */
@AutoConfiguration(
    after = [SnodraMessagingAutoConfiguration::class],
    before = [EventsAutoConfiguration::class],
    afterName = ["org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"],
    beforeName = ["org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration"],
)
@ConditionalOnBean(MongoTemplate::class)
@ConditionalOnProperty(name = ["snodra.messaging.store.type"], havingValue = MongoMessageStore.NAME, matchIfMissing = true)
class MongoMessageStoreAutoConfiguration {
    /** The store's transaction manager, which reports a transient commit failure as such ([MongoStoreTransactionManager]). */
    @Bean
    @ConditionalOnMissingBean
    fun mongoTransactionManager(databaseFactory: MongoDatabaseFactory): MongoTransactionManager = MongoStoreTransactionManager(databaseFactory)

    /** Steps back for any store the application defines, not for another add-on's: two add-ons need the setting (MessageStoreAddOnsCheck). */
    @Bean
    @ConditionalOnNoApplicationMessageStore
    fun mongoMessageStore(
        mongo: MongoTemplate,
        transactionManager: ObjectProvider<MongoTransactionManager>,
        properties: ObjectProvider<SnodraMessagingProperties>,
        clock: ObjectProvider<Clock>,
    ): MongoMessageStore = MongoMessageStore(
        mongo,
        transactionManager.getIfAvailable { MongoStoreTransactionManager(mongo.mongoDatabaseFactory) },
        properties.getIfAvailable()?.let(::settingsOf) ?: MongoStoreSettings(),
        clock.getIfAvailable { Clock.systemUTC() },
    )

    private fun settingsOf(p: SnodraMessagingProperties) = MongoStoreSettings(
        outboxRetention = p.outbox.retention,
        idempotencyRetention = p.idempotency.retention,
        externalCallRetention = p.external.calls.retention,
        sagaEndedRetention = p.sagas.endedRetention,
        sagaDispatchRetention = p.sagas.dispatchRetention,
        deadlineRetention = p.deadlines.retention,
        failedRetention = p.sagas.failedRetention,
    )
}
