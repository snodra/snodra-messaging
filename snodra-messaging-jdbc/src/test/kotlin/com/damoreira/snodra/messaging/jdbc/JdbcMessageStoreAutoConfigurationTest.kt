package com.damoreira.snodra.messaging.jdbc

import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.config.MemoryStoresAutoConfiguration
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.OutboxEntry
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
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
import java.sql.ResultSet
import java.sql.Statement
import java.time.Duration
import java.time.Instant
import javax.sql.DataSource

/** When the SQL store is the application's, with which dialect and transaction manager (a mocked DataSource: no database). */
class JdbcMessageStoreAutoConfigurationTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                SnodraMessagingAutoConfiguration::class.java,
                JdbcMessageStoreAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test", "snodra.messaging.store.init-schema=false")

    private fun database(product: String, version: String, schemaVersion: Int = 1): DataSource {
        val meta = mockk<DatabaseMetaData>(relaxed = true) {
            every { databaseProductName } returns product
            every { databaseProductVersion } returns version
            every { databaseMajorVersion } returns version.substringBefore('.').toInt()
            every { databaseMinorVersion } returns version.substringAfter('.').substringBefore('.').toInt()
        }
        val mode = mockk<ResultSet>(relaxed = true) {
            every { next() } returns true
            every { getString(1) } returns "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
        }
        val schema = mockk<ResultSet>(relaxed = true) {
            every { next() } returns true
            every { getInt(1) } returns schemaVersion
        }
        val statement = mockk<Statement>(relaxed = true) {
            every { executeQuery("SELECT @@SESSION.sql_mode") } returns mode
            every { executeQuery("SELECT MAX(version) FROM snodra_schema") } returns schema
        }
        val connection = mockk<Connection>(relaxed = true) {
            every { metaData } returns meta
            every { createStatement() } returns statement
        }
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
        with(database("MySQL", "8.0.36")).withPropertyValues("snodra.messaging.store.jdbc.dialect=mysql").run { context ->
            assertNull(context.startupFailure)
            assertEquals(MySqlDialect, (context.getBean(MessageStore::class.java) as JdbcMessageStore).dialect)
        }
        with(database("MySQL", "8.0.36")).withPropertyValues("snodra.messaging.store.jdbc.dialect=sqlserver").run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("supports postgresql, mysql"))
        }
    }

    @Test
    fun anUnsupportedDatabaseFailsNamingTheSupportedOnes() {
        with(database("H2", "2.3.232")).run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("supports PostgreSQL 13 or later and MySQL 8.0.17 or later; the DataSource's database is H2"), failure)
        }
    }

    @Test
    fun anOlderServerFailsTheStartup() {
        with(database("PostgreSQL", "12.22")).run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("needs PostgreSQL 13.0 or later; this server is PostgreSQL 12.22"), failure)
        }
        with(database("MySQL", "5.7.44")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("needs MySQL 8.0.17 or later"))
        }
        with(database("MySQL", "8.0.16")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("needs MySQL 8.0.17 or later; this server is MySQL 8.0.16"))
        }
        with(database("MySQL", "11.4.2-MariaDB")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("does not support MariaDB yet"))
        }
    }

    @Test
    fun theStoreStepsBackWhenTheSettingNamesAnother() {
        with(database("PostgreSQL", "17.7")).withPropertyValues("snodra.messaging.store.type=mongo").run { context ->
            // The guard then reports the missing MongoDB store: the SQL one is not there in its place.
            val failure = context.startupFailure!!.message!!
            assertTrue(failure.contains("snodra.messaging.store.type is 'mongo', but no message store is configured"), failure)
        }
        with(database("PostgreSQL", "17.7")).withPropertyValues("snodra.messaging.store.type=jdbc").run { context ->
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

    /** The application's own store of an add-on's class (a bean method or a registered bean): the add-ons step back for it. */
    @Test
    fun anApplicationsOwnAddOnStoreWins() {
        val ds = database("PostgreSQL", "17.7")
        with(ds).withUserConfiguration(OwnJdbcStore::class.java).run { context ->
            assertNull(context.startupFailure) // the SQL add-on alone: it steps back for the application's JdbcMessageStore
            assertEquals(1, context.getBeansOfType(MessageStore::class.java).size)
        }
        val both = withBothAddOns(ds)
        both.withUserConfiguration(OwnJdbcStore::class.java).run { context ->
            assertNull(context.startupFailure)
            assertEquals(1, context.getBeansOfType(MessageStore::class.java).size)
            assertSame(context.getBean("myStore"), context.getBean(MessageStore::class.java))
        }
        val mongo = com.damoreira.snodra.messaging.mongo.MongoMessageStore(mockk(relaxed = true), mockk(relaxed = true))
        both.withBean("myMongoStore", com.damoreira.snodra.messaging.mongo.MongoMessageStore::class.java, { mongo }).run { context ->
            assertNull(context.startupFailure)
            assertSame(mongo, context.getBean(MessageStore::class.java))
        }
        val mine: MessageStore = mockk(relaxed = true)
        both.withBean(MessageStore::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            assertSame(mine, context.getBean(MessageStore::class.java))
        }
        both.withUserConfiguration(OwnJdbcStore::class.java).withPropertyValues("snodra.messaging.store.type=jdbc").run { context ->
            assertNull(context.startupFailure)
            assertSame(context.getBean("myStore"), context.getBean(MessageStore::class.java), "the setting names its type: still the application's")
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    class OwnJdbcStore {
        @org.springframework.context.annotation.Bean
        fun myStore(dataSource: DataSource) = JdbcMessageStore(dataSource, PostgresDialect)
    }

    private fun withBothAddOns(ds: DataSource) = with(ds)
        .withConfiguration(AutoConfigurations.of(com.damoreira.snodra.messaging.mongo.MongoMessageStoreAutoConfiguration::class.java))
        .withBean(org.springframework.data.mongodb.MongoDatabaseFactory::class.java, { mockk(relaxed = true) })
        .withBean(org.springframework.data.mongodb.core.MongoTemplate::class.java, { mockk(relaxed = true) })

    @Test
    fun retentionsComeFromThePropertiesAndThePurgeCanBeTurnedOff() {
        with(database("PostgreSQL", "17.7")).withPropertyValues(
            "snodra.messaging.outbox.retention=1d",
            "snodra.messaging.idempotency.retention=2d",
            "snodra.messaging.external.calls.retention=3d",
            "snodra.messaging.sagas.ended-retention=4d",
            "snodra.messaging.sagas.dispatch-retention=5d",
            "snodra.messaging.deadlines.retention=6d",
            "snodra.messaging.sagas.failed-retention=8d",
            "snodra.messaging.store.jdbc.purge-interval=0s",
        ).run { context ->
            assertNull(context.startupFailure)
            val days = { n: Long -> Duration.ofDays(n) }
            assertEquals(JdbcStoreSettings(days(1), days(2), days(3), days(4), days(5), days(6), days(8)), context.getBean(JdbcMessageStore::class.java).settings)
            assertFalse(context.getBean(JdbcStorePurger::class.java).isRunning)
        }
    }

    /** Two store add-ons on one classpath: without the setting the startup fails naming both; each value runs that one. */
    @Test
    fun twoStoreAddOnsNeedTheSetting() {
        val both = withBothAddOns(database("PostgreSQL", "17.7"))
        both.run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("2 message store add-ons are configured"), failure)
            assertTrue(failure.contains("JdbcMessageStore: jdbc") && failure.contains("MongoMessageStore: mongo"), failure)
            assertTrue(failure.contains("set snodra.messaging.store.type to the one this service uses"), failure)
        }
        both.withPropertyValues("snodra.messaging.store.type=jdbc").run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBean(MessageStore::class.java) is JdbcMessageStore)
        }
        both.withPropertyValues("snodra.messaging.store.type=mongo").run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBean(MessageStore::class.java) is com.damoreira.snodra.messaging.mongo.MongoMessageStore)
        }
    }

    /** The dialect setting overrides detection: a compatible database of another product name runs. */
    @Test
    fun theDialectSettingAcceptsACompatibleDatabase() {
        with(database("CockroachDB", "23.1.0")).run { context ->
            assertTrue(context.startupFailure!!.stackTraceToString().contains("the DataSource's database is CockroachDB"))
        }
        with(database("CockroachDB", "23.1.0")).withPropertyValues("snodra.messaging.store.jdbc.dialect=postgresql").run { context ->
            assertNull(context.startupFailure)
            assertEquals(PostgresDialect, (context.getBean(MessageStore::class.java) as JdbcMessageStore).dialect)
        }
    }

    @Test
    fun theWakeUpCanBeTurnedOff() {
        with(database("PostgreSQL", "17.7")).run { context ->
            assertNotNull(((context.getBean(MessageStore::class.java) as JdbcMessageStore).outbox as JdbcOutboxStore).notifications)
        }
        with(database("PostgreSQL", "17.7")).withPropertyValues("snodra.messaging.store.jdbc.notify=false").run { context ->
            assertNull(context.startupFailure)
            assertNull(((context.getBean(MessageStore::class.java) as JdbcMessageStore).outbox as JdbcOutboxStore).notifications, "the relays poll")
        }
    }

    @Test
    fun withInitSchemaOffTheSchemaMustBeThere() {
        with(database("PostgreSQL", "17.7", schemaVersion = 0)).run { context ->
            val failure = context.startupFailure!!.stackTraceToString()
            assertTrue(failure.contains("schema is at version 0, it needs 1 or later: apply db/snodra/postgresql/V1__snodra.sql"), failure)
        }
    }

    /** A pool of one would wait for itself (the store's own-commit writes take a second connection): refused at once. */
    @Test
    fun aPoolOfOneIsRefused() {
        val pool = mockk<com.zaxxer.hikari.HikariDataSource>(relaxed = true) {
            every { maximumPoolSize } returns 1
            every { poolName } returns "tiny"
        }
        val e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java) { JdbcMessageStore(pool, PostgresDialect) }
        assertTrue(e.message!!.contains("needs a connection pool of at least 2 (the pool 'tiny' has 1)"), e.message)
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
            .withPropertyValues("snodra.messaging.store.type=memory")
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

    @Test fun `the configuration metadata describes every SQL store setting`() {
        val metadata = javaClass.classLoader.getResources("META-INF/spring-configuration-metadata.json").toList()
            .map { url -> url.openStream().use { String(it.readAllBytes()) } }
            .single { "\"snodra.messaging.store.jdbc\"" in it }
        val described = com.damoreira.snodra.messaging.codec.JsonCodec().decodeTree(metadata.toByteArray())["properties"]
            .associate { it["name"].asString() to it["description"]?.asString() }
        for (key in JdbcStoreProperties::class.java.declaredFields.map { it.name }.filterNot { it.startsWith("$") }) {
            val name = "snodra.messaging.store.jdbc." + key.replace(Regex("([A-Z])")) { "-" + it.value.lowercase() }
            assertTrue(!described[name].isNullOrBlank(), "$name is described in the metadata")
        }
    }
}
