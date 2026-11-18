package com.damoreira.snodra.messaging.jdbc

import com.damoreira.snodra.messaging.config.EventsAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.config.SnodraMessagingProperties
import com.damoreira.snodra.messaging.config.MessageStoreGuard
import com.zaxxer.hikari.HikariDataSource
import com.damoreira.snodra.messaging.config.ConditionalOnNoApplicationMessageStore
import com.damoreira.snodra.messaging.store.MessageStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionManager
import java.sql.Connection
import java.sql.DriverManager
import java.time.Clock
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/** `snodra.messaging.store.jdbc.*`: the SQL message store's own settings (its retentions are the core's `*.retention`). */
@ConfigurationProperties("snodra.messaging.store.jdbc")
data class JdbcStoreProperties(
    /**
     * `postgresql` or `mysql`; unset: detected from the DataSource's database product name. Set, it is used whatever the product
     * (a compatible database); the server version is checked when the product is the dialect's own.
     */
    val dialect: String? = null,
    /**
     * Whether appends wake the outbox relays up at once (PostgreSQL `LISTEN`/`NOTIFY`); off, the relays poll. Turn it off behind
     * PgBouncer in transaction mode (LISTEN needs a session).
     */
    val notify: Boolean = true,
    /** How often the expired rows are purged ([JdbcMessageStore.purgeExpired]); zero turns the purge off. */
    val purgeInterval: Duration = Duration.ofMinutes(1),
)

/**
 * The SQL [MessageStore] (ADR 0028) on the application's [DataSource], unless `snodra.messaging.store.type` names another
 * store (then nothing here is created), or the application declares a [MessageStore] of its own. Another add-on does not make it
 * step back: two of them without the setting fail the startup. The dialect is the database's (`snodra.messaging.store.jdbc.dialect`
 * overrides it); a database other than PostgreSQL 13+ or MySQL 8.0.17+ fails the startup naming the supported ones. The core
 * applies the schema once every bean exists, or with `snodra.messaging.store.init-schema=false` checks it is there.
 *
 * **Transactions:** the store's transaction manager is the application's (Boot's `DataSourceTransactionManager`, or the
 * `JpaTransactionManager` of a JPA application: both bind the DataSource's connection, which the store joins). Only an
 * application without any transaction manager gets one here, a [DataSourceTransactionManager], which then backs its
 * `@Transactional` methods too. Ordered after Boot's DataSource, JDBC transaction and JPA auto-configurations, and before its
 * transaction auto-configuration and the core's configurations that need a store.
 *
 * On PostgreSQL the relay wake-up listens on a connection of its own built like the pool's (outside it) when it can be
 * (`snodra.messaging.store.jdbc.notify=false` turns the wake-up off), else on one borrowed from the pool for a while at a time. [JdbcStorePurger] runs
 * the retention every `snodra.messaging.store.jdbc.purge-interval`.
 */
@AutoConfiguration(
    after = [SnodraMessagingAutoConfiguration::class],
    before = [EventsAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration",
    ],
    beforeName = ["org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration"],
)
@ConditionalOnBean(DataSource::class)
@ConditionalOnProperty(name = [MessageStoreGuard.STORE], havingValue = JdbcMessageStore.NAME, matchIfMissing = true)
@EnableConfigurationProperties(JdbcStoreProperties::class)
class JdbcMessageStoreAutoConfiguration {
    /** For an application without a transaction manager: the store's, and its `@Transactional` methods'. */
    @Bean
    @ConditionalOnMissingBean(TransactionManager::class)
    fun jdbcStoreTransactionManager(dataSource: DataSource): DataSourceTransactionManager = DataSourceTransactionManager(dataSource)

    /** Steps back for any store the application defines, not for another add-on's: two add-ons need the setting (MessageStoreAddOnsCheck). */
    @Bean
    @ConditionalOnNoApplicationMessageStore
    fun jdbcMessageStore(
        dataSource: DataSource,
        transactionManagers: ObjectProvider<PlatformTransactionManager>,
        jdbc: JdbcStoreProperties,
        properties: ObjectProvider<SnodraMessagingProperties>,
        connectionDetails: ObjectProvider<JdbcConnectionDetails>,
        clock: ObjectProvider<Clock>,
    ): JdbcMessageStore {
        val dialect = JdbcDialects.detect(dataSource, jdbc.dialect)
        val listen = if (dialect.notifies && jdbc.notify) listenConnection(dataSource, connectionDetails.ifAvailable) else null
        log.info("SQL message store: {}", dialect.name)
        return JdbcMessageStore(
            dataSource,
            dialect,
            transactionManagers.getIfUnique { DataSourceTransactionManager(dataSource) },
            properties.getIfAvailable()?.let(::settingsOf) ?: JdbcStoreSettings(),
            clock.getIfAvailable { Clock.systemUTC() },
            listen,
            jdbc.notify,
        )
    }

    /**
     * The relay wake-up's own connection, outside the pool: built like the pool's when the DataSource is HikariCP's (its JDBC
     * URL, user, password and `data-source-properties`), else from Boot's [JdbcConnectionDetails] (URL, user, password; what the
     * URL carries, such as `ssl` parameters, applies). Null without either: the wake-up then borrows pooled connections.
     */
    private fun listenConnection(dataSource: DataSource, details: JdbcConnectionDetails?): (() -> Connection)? {
        val hikari = dataSource as? HikariDataSource
        if (hikari?.jdbcUrl != null) {
            return {
                val props = Properties()
                props.putAll(hikari.dataSourceProperties)
                hikari.username?.let { props["user"] = it }
                hikari.password?.let { props["password"] = it }
                DriverManager.getConnection(hikari.jdbcUrl, props)
            }
        }
        return details?.let { d -> { DriverManager.getConnection(d.jdbcUrl, d.username, d.password) } }
    }

    @Bean
    @ConditionalOnBean(JdbcMessageStore::class)
    fun jdbcStorePurger(store: JdbcMessageStore, jdbc: JdbcStoreProperties): JdbcStorePurger = JdbcStorePurger(store, jdbc.purgeInterval)

    private fun settingsOf(p: SnodraMessagingProperties) = JdbcStoreSettings(
        outboxRetention = p.outbox.retention,
        idempotencyRetention = p.idempotency.retention,
        externalCallRetention = p.external.calls.retention,
        sagaEndedRetention = p.sagas.endedRetention,
        sagaDispatchRetention = p.sagas.dispatchRetention,
        deadlineRetention = p.deadlines.retention,
        failedRetention = p.sagas.failedRetention,
    )

    private companion object {
        val log = LoggerFactory.getLogger(JdbcMessageStoreAutoConfiguration::class.java)
    }
}

/** Which [SqlDialect] a DataSource needs. */
object JdbcDialects {
    /**
     * The dialect named by [override] (`postgresql`, `mysql`: used whatever the database's product name), else the one of
     * [dataSource]'s database product; fails with an [IllegalStateException] naming the supported databases for any other, when
     * the server is older than the dialect's minimum, and on MySQL without a STRICT `sql_mode` ([JdbcSchema.verify]).
     */
    fun detect(dataSource: DataSource, override: String?): SqlDialect = dataSource.connection.use { c ->
        val overridden = !override.isNullOrBlank()
        val product = c.metaData.databaseProductName
        val dialect = if (!override.isNullOrBlank()) {
            SqlDialect.named(override) ?: throw IllegalStateException(
                "snodra.messaging.store.jdbc.dialect is '$override': the SQL message store supports ${SqlDialect.all.joinToString(", ") { it.name }}",
            )
        } else {
            SqlDialect.ofProduct(product) ?: throw IllegalStateException(
                "The SQL message store (snodra-messaging-jdbc) supports PostgreSQL 13 or later and MySQL 8.0.17 or later; the DataSource's " +
                    "database is $product. Use another message store (snodra.messaging.store.type) or remove the add-on.",
            )
        }
        JdbcSchema.verify(c, dialect, overridden)
        dialect
    }
}

/**
 * Runs [JdbcMessageStore.purgeExpired] every [interval] (none when it is zero or negative) on a thread of its own, from the
 * context's start to its stop. A failed purge is logged and tried again at the next interval.
 */
class JdbcStorePurger(private val store: JdbcMessageStore, private val interval: Duration) : SmartLifecycle {
    private var executor: ScheduledExecutorService? = null

    @Synchronized
    override fun start() {
        if (executor != null || interval.isZero || interval.isNegative) return
        executor = Executors.newSingleThreadScheduledExecutor { Thread(it, "snodra-store-purge").apply { isDaemon = true } }.also {
            it.scheduleWithFixedDelay(::purge, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    @Synchronized
    override fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    @Synchronized
    override fun isRunning(): Boolean = executor != null

    private fun purge() {
        try {
            val deleted = store.purgeExpired()
            if (deleted > 0) log.debug("Purged {} expired rows of the message store", deleted)
        } catch (e: Exception) {
            log.warn("Purging the message store failed; retrying in {}: {}", interval, e.toString())
        }
    }

    private companion object {
        val log = LoggerFactory.getLogger(JdbcStorePurger::class.java)
    }
}
