package com.damoreira.snodra.messaging.mongo

import com.mongodb.MongoException
import com.mongodb.client.MongoCollection
import com.damoreira.snodra.messaging.store.DeadLetterStore
import com.damoreira.snodra.messaging.store.ExternalCallStore
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.OutboxStore
import com.damoreira.snodra.messaging.store.ProcessedEventStore
import com.damoreira.snodra.messaging.store.ProcessorControlStore
import com.damoreira.snodra.messaging.store.SagaStores
import org.bson.Document
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoExceptionTranslator
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.time.Clock
import java.time.Duration

/** How long MongoDB keeps what the library expires through TTL indexes (the defaults of `snodra.messaging.*.retention`). */
data class MongoStoreSettings(
    /** Sent `outbox` documents. */
    val outboxRetention: Duration = Duration.ofDays(7),
    /** `processed_events` documents. */
    val idempotencyRetention: Duration = Duration.ofDays(30),
    /** `external_calls` documents. */
    val externalCallRetention: Duration = Duration.ofDays(30),
    /** Ended `sagas`. */
    val sagaEndedRetention: Duration = Duration.ofDays(30),
    /** Dispatched `saga_dispatch` entries. */
    val sagaDispatchRetention: Duration = Duration.ofDays(7),
    /** Fired or cancelled `deadlines`. */
    val deadlineRetention: Duration = Duration.ofDays(7),
    /** What failed for good: `saga_dispatch` entries given up on and parked `saga_failures`. */
    val failedRetention: Duration = Duration.ofDays(30),
)

/**
 * The [MessageStore] on MongoDB: the collections `outbox`, `processed_events`, `dead_letters`,
 * `processor_control`, `external_calls`, `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines` and `saga_failures`,
 * with the documents the library has always written. A replica set is needed for transactions.
 *
 * Writes that join the caller's transaction go through the [MongoTemplate]'s session-aware database; the writes that must
 * commit on their own (dead letters, processor control) go through the factory's plain database, which a Spring
 * transaction never binds. Times are stamped with [clock].
 */
class MongoMessageStore(
    private val mongo: MongoTemplate,
    override val transactions: PlatformTransactionManager = MongoStoreTransactionManager(mongo.mongoDatabaseFactory),
    internal val settings: MongoStoreSettings = MongoStoreSettings(),
    clock: Clock = Clock.systemUTC(),
) : MessageStore {
    override val name: String = NAME

    override val outbox: OutboxStore = MongoOutboxStore(mongo, clock)

    override val processedEvents: ProcessedEventStore = MongoProcessedEventStore(mongo)

    override val deadLetters: DeadLetterStore = MongoDeadLetterStore(mongo, clock)

    override val processorControl: ProcessorControlStore = MongoProcessorControlStore(mongo, clock)

    override val externalCalls: ExternalCallStore = MongoExternalCallStore(mongo)

    override val sagas: SagaStores = mongoSagaStores(mongo)

    /** Creates the indexes of every collection above (idempotent): the relay's, the TTL ones, the saga routing ones. */
    fun ensureIndexes() = MongoIndexes.ensure(mongo, settings)

    /** The store's schema is its indexes ([ensureIndexes]). */
    override fun initSchema() = ensureIndexes()

    companion object {
        /** The message store's name (`snodra.messaging.store.type=mongo`). */
        const val NAME = "mongo"
    }
}

/** The collection a write joining the caller's transaction goes to: the session-aware database of the template. */
internal fun MongoTemplate.joined(name: String): MongoCollection<Document> = getCollection(name)

/** The collection a write that commits on its own goes to: the factory's plain database, never bound to a transaction. */
internal fun MongoTemplate.independent(name: String): MongoCollection<Document> = mongoDatabaseFactory.mongoDatabase.getCollection(name)

private val translator = MongoExceptionTranslator()

/**
 * Runs [block], translating a driver exception as the template does (a duplicate key: Spring's `DuplicateKeyException`),
 * except that a write conflict of two transactions (`WriteConflict`, or any error labelled `TransientTransactionError`) is a
 * `TransientMongoDbException`, whatever kind of write it was: the transient failure a caller retries (the same rule as
 * [MongoStoreTransactionManager] at commit).
 */
internal fun <T> translated(block: () -> T): T = try {
    block()
} catch (e: MongoException) {
    throw transient(e) ?: translator.translateExceptionIfPossible(e) ?: e
} catch (e: RuntimeException) {
    throw translator.translateExceptionIfPossible(e) ?: e
}
