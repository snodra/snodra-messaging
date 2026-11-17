package com.damoreira.snodra.messaging.jdbc

import com.zaxxer.hikari.HikariDataSource
import com.damoreira.snodra.messaging.store.OutboxEntry
import com.damoreira.snodra.messaging.store.ProcessedMarker
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import kotlinx.coroutines.runBlocking
import org.hibernate.cfg.AvailableSettings
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.orm.jpa.SharedEntityManagerCreator
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/** A business row of the JPA test. */
@Entity
@Table(name = "orders_jpa")
class OrderRow(@Id var id: String = "", var item: String = "")

/**
 * The store joins the service's transaction, whichever Spring manager runs it over the DataSource: a business write and the
 * outbox entry (and the processed marker) commit together or roll back together.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JdbcTransactionJoinTest(private val database: TestDatabase) {
    private lateinit var dataSource: HikariDataSource
    private lateinit var jdbc: JdbcClient

    @BeforeAll
    fun start() {
        dataSource = database.dataSource()
        JdbcMessageStore(dataSource, database.dialect).initSchema()
        jdbc = JdbcClient.create(dataSource)
        jdbc.sql("CREATE TABLE orders (id VARCHAR(64) PRIMARY KEY, item VARCHAR(64) NOT NULL)").update()
    }

    @AfterAll
    fun stop() = dataSource.close()

    @BeforeEach
    fun clear() {
        listOf("orders", "snodra_outbox", "snodra_processed_events").forEach { jdbc.sql("DELETE FROM $it").update() }
    }

    @Test
    fun joins_datasource_transaction() {
        val manager = DataSourceTransactionManager(dataSource)
        val store = JdbcMessageStore(dataSource, database.dialect, manager)
        TransactionTemplate(manager).executeWithoutResult {
            jdbc.sql("INSERT INTO orders (id, item) VALUES ('o-1', 'book')").update()
            runBlocking {
                store.outbox.append(entry("e-1"))
                assertTrue(store.processedEvents.record(ProcessedMarker("processor:default:evt-1", "t.Happened", now())))
            }
        }
        assertThrows(IllegalStateException::class.java) {
            TransactionTemplate(manager).executeWithoutResult {
                jdbc.sql("INSERT INTO orders (id, item) VALUES ('o-2', 'pen')").update()
                runBlocking {
                    store.outbox.append(entry("e-2"))
                    store.processedEvents.record(ProcessedMarker("processor:default:evt-2", "t.Happened", now()))
                }
                throw IllegalStateException("the handler failed after its writes")
            }
        }
        assertEquals(listOf("o-1"), orders())
        assertEquals(listOf("e-1"), runBlocking { store.outbox.claim("relay", Duration.ofSeconds(30), 10) }.map { it.id })
        assertTrue(runBlocking { store.processedEvents.exists("processor:default:evt-1") })
        assertFalse(runBlocking { store.processedEvents.exists("processor:default:evt-2") }, "rolled back with the business row")
    }

    @Test
    fun joins_jpa_transaction() {
        jpa { manager, em ->
            val store = JdbcMessageStore(dataSource, database.dialect, manager)
            TransactionTemplate(manager).executeWithoutResult {
                em.persist(OrderRow("o-1", "book"))
                em.flush()
                runBlocking { store.outbox.append(entry("e-1")) }
            }
            assertThrows(IllegalStateException::class.java) {
                TransactionTemplate(manager).executeWithoutResult {
                    em.persist(OrderRow("o-2", "pen"))
                    em.flush()
                    runBlocking { store.outbox.append(entry("e-2")) }
                    throw IllegalStateException("the handler failed after its writes")
                }
            }
            assertEquals(listOf("o-1"), jdbc.sql("SELECT id FROM orders_jpa ORDER BY id").query(String::class.java).list())
            assertEquals(listOf("e-1"), runBlocking { store.outbox.claim("relay", Duration.ofSeconds(30), 10) }.map { it.id })
        }
    }

    /** Runs [block] with a JpaTransactionManager and a shared EntityManager over the test DataSource (Hibernate, schema created). */
    private fun jpa(block: (JpaTransactionManager, EntityManager) -> Unit) {
        val ds = dataSource // inside apply, `dataSource` would be the factory bean's own (null) property
        val factory = LocalContainerEntityManagerFactoryBean().apply {
            setDataSource(ds)
            jpaVendorAdapter = HibernateJpaVendorAdapter()
            setManagedTypes(org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes.of(OrderRow::class.java.name))
            val hibernateDialect = if (database == TestDatabase.POSTGRES) "org.hibernate.dialect.PostgreSQLDialect" else "org.hibernate.dialect.MySQLDialect"
            setJpaPropertyMap(mapOf(AvailableSettings.HBM2DDL_AUTO to "create", AvailableSettings.DIALECT to hibernateDialect))
            afterPropertiesSet()
        }
        try {
            block(JpaTransactionManager(factory.`object`!!), SharedEntityManagerCreator.createSharedEntityManager(factory.`object`!!))
        } finally {
            factory.destroy()
        }
    }

    /** An append from the service's afterCommit callback (its transaction committed already) commits on its own, under either manager. */
    @Test
    fun an_append_from_after_commit_commits_on_its_own() {
        jpa { manager, _ ->
            for (m in listOf(DataSourceTransactionManager(dataSource), manager)) {
                val store = JdbcMessageStore(dataSource, database.dialect, m)
                val id = "after-commit-${m::class.java.simpleName}"
                TransactionTemplate(m).executeWithoutResult {
                    TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                        override fun afterCommit() = runBlocking { store.outbox.append(entry(id)) }
                    })
                }
                assertEquals(1L, jdbc.sql("SELECT COUNT(*) FROM snodra_outbox WHERE id = :id").param("id", id).query(Long::class.javaObjectType).single(), m.toString())
            }
        }
    }

    @Test
    fun a_transaction_of_another_manager_is_refused() {
        database.dataSource(poolSize = 2).use { other ->
            val foreign: PlatformTransactionManager = DataSourceTransactionManager(other)
            val store = JdbcMessageStore(dataSource, database.dialect)
            val e = assertThrows(IllegalStateException::class.java) {
                TransactionTemplate(foreign).executeWithoutResult { runBlocking { store.outbox.append(entry("e-1")) } }
            }
            assertTrue(e.message!!.contains("not on the message store's DataSource"), e.message)
            assertEquals(0L, runBlocking { store.outbox.pending() }, "nothing was written beside the transaction")
        }
    }

    private fun orders() = jdbc.sql("SELECT id FROM orders ORDER BY id").query(String::class.java).list()

    private fun now() = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    private fun entry(id: String) = OutboxEntry(id, "t.Happened", "k", mapOf("correlationId" to "c-$id"), """{"id":"$id"}""".toByteArray(), now(), 1)
}

class PostgresTransactionJoinTest : JdbcTransactionJoinTest(TestDatabase.POSTGRES)

class MySqlTransactionJoinTest : JdbcTransactionJoinTest(TestDatabase.MYSQL)
