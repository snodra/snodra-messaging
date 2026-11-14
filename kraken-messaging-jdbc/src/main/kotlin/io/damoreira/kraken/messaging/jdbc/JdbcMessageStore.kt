package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.store.AddOnMessageStore
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ExternalCallStore
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxStore
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import io.damoreira.kraken.messaging.store.SagaStores
import com.zaxxer.hikari.HikariDataSource
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import java.sql.Connection
import java.time.Clock
import java.time.Duration
import javax.sql.DataSource

/** How long the store keeps what it purges (the defaults of `kraken.messaging.*.retention`, as the MongoDB add-on's TTLs). */
data class JdbcStoreSettings(
    /** Sent outbox entries. */
    val outboxRetention: Duration = Duration.ofDays(7),
    /** Processed-event markers. */
    val idempotencyRetention: Duration = Duration.ofDays(30),
    /** External-call records, from their last update. */
    val externalCallRetention: Duration = Duration.ofDays(30),
    /** Ended sagas. */
    val sagaEndedRetention: Duration = Duration.ofDays(30),
    /** Dispatched saga-dispatch entries. */
    val sagaDispatchRetention: Duration = Duration.ofDays(7),
    /** Fired or cancelled deadlines. */
    val deadlineRetention: Duration = Duration.ofDays(7),
    /** What failed for good: saga-dispatch entries given up on and parked saga failures. */
    val failedRetention: Duration = Duration.ofDays(30),
)

/**
 * The [MessageStore] on a SQL database through plain JDBC (ADR 0028): PostgreSQL 13+ or MySQL 8.0.17+ ([dialect]), the tables
 * `kraken_outbox`, `kraken_processed_events`, `kraken_dead_letters`, `kraken_processor_control`, `kraken_external_calls`,
 * `kraken_sagas`, `kraken_saga_associations`, `kraken_saga_dispatch`, `kraken_saga_dispatch_leases`, `kraken_deadlines` and
 * `kraken_saga_failures` (schema `db/kraken/<dialect>/V1__kraken.sql`, applied by [initSchema]).
 *
 * **Transactions:** the writes that join the caller's transaction run on the connection Spring binds to the thread for
 * [dataSource]: [transactions] (the application's `DataSourceTransactionManager` or `JpaTransactionManager` over the same
 * DataSource) binds it, so business rows, outbox entries and markers commit together. A transaction of another manager active
 * on the thread (one that does not hold this DataSource's connection) is refused with an [IllegalStateException] rather than
 * written beside. What commits on its own (dead letters, processor control, the relays' claims) uses a connection of its own
 * from the same DataSource. Conflicts surface as Spring's `DuplicateKeyException` and `TransientDataAccessException`s.
 *
 * Times are stamped with [clock], at millisecond precision. Nothing expires by itself: [purgeExpired] deletes what the
 * retentions in [settings] let go (the auto-configuration runs it every `kraken.messaging.store.jdbc.purge-interval`).
 *
 * On PostgreSQL every append also notifies `kraken_outbox` in its transaction, and [OutboxStore.onAppend] listens on it
 * through [listenConnection] (a connection of its own, kept open; by default one borrowed from [dataSource] for at most 30 s at
 * a time), so a relay claims as soon as an append commits. MySQL has no such notification: the relay polls.
 */
class JdbcMessageStore(
    private val dataSource: DataSource,
    val dialect: SqlDialect,
    override val transactions: PlatformTransactionManager = DataSourceTransactionManager(dataSource),
    internal val settings: JdbcStoreSettings = JdbcStoreSettings(),
    clock: Clock = Clock.systemUTC(),
    /** A connection only the relay wake-up uses (PostgreSQL): opened outside the pool, kept as long as a relay runs. */
    listenConnection: (() -> Connection)? = null,
    /** Whether appends wake the relays up (PostgreSQL `NOTIFY`; `kraken.messaging.store.jdbc.notify`): off, the relays poll. */
    notify: Boolean = true,
) : AddOnMessageStore {
    override val name: String = NAME

    init {
        // A write that commits on its own inside the service's transaction needs a second connection: a pool of one would wait
        // for itself until its connection timeout.
        (dataSource as? HikariDataSource)?.maximumPoolSize?.let { size ->
            require(size >= MIN_POOL_SIZE) {
                "The kraken message store needs a connection pool of at least $MIN_POOL_SIZE (the pool '${dataSource.poolName}' has $size): " +
                    "a write that commits on its own inside the service's transaction takes a second connection"
            }
        }
    }

    internal val db = Db(dataSource, dialect, clock)

    private val notifies = notify && dialect.notifies

    private val notifications: PostgresNotifications? =
        if (notifies) PostgresNotifications(listenConnection ?: dataSource::getConnection, dedicated = listenConnection != null) else null

    override val outbox: OutboxStore = JdbcOutboxStore(db, notifications, notifies)

    override val processedEvents: ProcessedEventStore = JdbcProcessedEventStore(db)

    override val deadLetters: DeadLetterStore = JdbcDeadLetterStore(db)

    override val processorControl: ProcessorControlStore = JdbcProcessorControlStore(db)

    override val externalCalls: ExternalCallStore = JdbcExternalCallStore(db)

    override val sagas: SagaStores = jdbcSagaStores(db)

    /**
     * Checks the server (the dialect's database, at least its minimum version) and applies schema version 1 unless the
     * database has it (`kraken_schema` holds version 1 or later), under a database lock so instances starting together apply
     * it once. A database already at version 1 is only read.
     */
    override fun initSchema() = JdbcSchema.apply(dataSource, dialect)

    /** For a service that applies the schema itself: fails unless the database has the schema version this library needs, or later. */
    fun requireSchema() = JdbcSchema.require(dataSource, dialect)

    /** Deletes what the retentions let go and the expired dead letters and leases; returns how many rows went. */
    fun purgeExpired(): Long = JdbcPurge.run(db, settings)

    companion object {
        /** The message store's name (`kraken.messaging.store.type=jdbc`). */
        const val NAME = "jdbc"

        /** The smallest connection pool the store runs on. */
        const val MIN_POOL_SIZE = 2
    }
}
