package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.MemoryStoresAutoConfiguration
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.time.Duration
import java.time.Instant
import javax.sql.DataSource

/** When the SQL store is the application's, with which dialect and transaction manager (a mocked DataSource: no database). */
class JdbcMessageStoreAutoConfigurationTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                JdbcMessageStoreAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues("kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=test", "kraken.messaging.store.init-schema=false")

    private fun database(product: String, version: String): DataSource {
        val meta = mockk<DatabaseMetaData>(relaxed = true) {
            every { databaseProductName } returns product
            every { databaseProductVersion } returns version
            every { databaseMajorVersion } returns version.substringBefore('.').toInt()
            every { databaseMinorVersion } returns version.substringAfter('.').substringBefore('.').toInt()
        }
        val connection = mockk<Connection>(relaxed = true) { every { metaData } returns meta }
        return mockk(relaxed = true) { every { this@mockk.connection } returns connection }
    }

    private fun with(dataSource: DataSource) = runner.withBean(DataSource::class.java, { dataSource })

    @Test
    fun noDataSourceNoStore() {
        runner.run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBeansOfType(MessageStore::class.java).isEmpty())
        }
    }

    @Test
    fun aPostgresDataSourceGetsTheStoreAndATransactionManagerWhenItHasNone() {
        with(database("PostgreSQL", "17.7")).run { context ->
            assertNull(context.startupFailure)
            val store = context.getBean(MessageStore::class.java) as JdbcMessageStore
            assertEquals("jdbc", store.name)
            assertEquals(PostgresDialect, store.dialect)
            assertSame(context.getBean(PlatformTransactionManager::class.java), store.transactions, "the @Transactional manager is the store's")
            assertTrue(store.transactions is DataSourceTransactionManager)
            assertEquals(JdbcStoreSettings(), store.settings)
            assertTrue(context.getBean(JdbcStorePurger::class.java).isRunning)
        }
    }

    @Test
    fun theApplicationsTransactionManagerIsTheStores() {
        val ds = database("MySQL", "8.4.7")
        val mine = DataSourceTransactionManager(ds)
        with(ds).withBean(PlatformTransactionManager::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            val store = context.getBean(MessageStore::class.java) as JdbcMessageStore
            assertEquals(MySqlDialect, store.dialect)
            assertSame(mine, store.transactions)
            assertEquals(1, context.getBeansOfType(PlatformTransactionManager::class.java).size, "no second manager")
        }
    }

    @Test
    fun theDialectSettingOverridesDetection() {
        with(database("MySQL", "8.0.36")).withPropertyValues("kraken.messaging.store.jdbc.dialect=mysql").run { context ->
            assertNull(context.startupFailure)
            assertEquals(MySqlDialect, (context.getBean(MessageStore::class.java) as JdbcMessageStore).dialect)
        }
        with(database("MySQL", "8.0.36")).withPropertyValues("kraken.messaging.store.jdbc.dialect=sqlserver").run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("supports postgresql, mysql"))
        }
    }

    @Test
    fun anUnsupportedDatabaseFailsNamingTheSupportedOnes() {
        with(database("H2", "2.3.232")).run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("supports PostgreSQL 13 or later and MySQL 8.0 or later; the DataSource's database is H2"), failure)
        }
    }

    @Test
    fun anOlderServerFailsTheStartup() {
        with(database("PostgreSQL", "12.22")).run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("needs PostgreSQL 13.0 or later; this server is PostgreSQL 12.22"), failure)
        }
        with(database("MySQL", "5.7.44")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("needs MySQL 8.0 or later"))
        }
        with(database("MySQL", "11.4.2-MariaDB")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("does not support MariaDB yet"))
        }
    }

    @Test
    fun theStoreStepsBackWhenTheSettingNamesAnother() {
        with(database("PostgreSQL", "17.7")).withPropertyValues("kraken.messaging.store.type=mongo").run { context ->
            // The guard then reports the missing MongoDB store: the SQL one is not there in its place.
            val failure = context.startupFailure!!.message!!
            assertTrue(failure.contains("kraken.messaging.store.type is 'mongo', but no message store is configured"), failure)
        }
        with(database("PostgreSQL", "17.7")).withPropertyValues("kraken.messaging.store.type=jdbc").run { context ->
            assertNull(context.startupFailure)
            assertNotNull(context.getBean(JdbcMessageStore::class.java), "named by the setting")
        }
    }

    @Test
    fun anApplicationStoreWins() {
        val mine: MessageStore = mockk(relaxed = true)
        with(database("PostgreSQL", "17.7")).withBean(MessageStore::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            assertSame(mine, context.getBean(MessageStore::class.java))
            assertTrue(context.getBeansOfType(JdbcStorePurger::class.java).isEmpty())
        }
    }

    @Test
    fun retentionsComeFromThePropertiesAndThePurgeCanBeTurnedOff() {
        with(database("PostgreSQL", "17.7")).withPropertyValues(
            "kraken.messaging.outbox.retention=1d",
            "kraken.messaging.idempotency.retention=2d",
            "kraken.messaging.external.calls.retention=3d",
            "kraken.messaging.sagas.ended-retention=4d",
            "kraken.messaging.sagas.dispatch-retention=5d",
            "kraken.messaging.deadlines.retention=6d",
            "kraken.messaging.sagas.failed-retention=8d",
            "kraken.messaging.store.jdbc.purge-interval=0s",
        ).run { context ->
            assertNull(context.startupFailure)
            val days = { n: Long -> Duration.ofDays(n) }
            assertEquals(JdbcStoreSettings(days(1), days(2), days(3), days(4), days(5), days(6), days(8)), context.getBean(JdbcMessageStore::class.java).settings)
            assertFalse(context.getBean(JdbcStorePurger::class.java).isRunning)
        }
    }

    /**
     * `store.type=memory` beside a DataSource: the in-memory store's transaction manager is ordered after Boot's JDBC one
     * (`MemoryStoresAutoConfiguration` names `DataSourceTransactionManagerAutoConfiguration`), so the application keeps its
     * DataSourceTransactionManager, and an in-memory outbox entry appended in its transaction lands after its commit, never after
     * its rollback.
     */
    @Test
    fun theInMemoryStoreBesideADataSourceKeepsTheJdbcTransactionManager() {
        with(database("PostgreSQL", "17.7"))
            .withConfiguration(AutoConfigurations.of(MemoryStoresAutoConfiguration::class.java, DataSourceTransactionManagerAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.store.type=memory")
            .run { context ->
                assertNull(context.startupFailure)
                val store = context.getBean(MessageStore::class.java)
                assertTrue(store is MemoryMessageStore, "$store")
                val manager = context.getBean(PlatformTransactionManager::class.java)
                assertTrue(manager is DataSourceTransactionManager, "the application's transaction manager is the JDBC one: $manager")

                TransactionTemplate(manager).executeWithoutResult {
                    runBlocking {
                        store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 1))
                        assertEquals(0L, store.outbox.pending(), "not before the commit")
                    }
                }
                assertEquals(1L, runBlocking { store.outbox.pending() }, "appended after the commit")
                TransactionTemplate(manager).executeWithoutResult { status ->
                    runBlocking { store.outbox.append(OutboxEntry("e-2", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 2)) }
                    status.setRollbackOnly()
                }
                assertEquals(1L, runBlocking { store.outbox.pending() }, "never after a rollback")
            }
    }
}
