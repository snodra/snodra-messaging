package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.ProcessedMarker
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.event.TransactionPhase
import org.springframework.transaction.event.TransactionalEventListener
import org.springframework.transaction.support.TransactionTemplate
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * A pool whose connections are not in auto-commit (`spring.datasource.hikari.auto-commit=false`, common with JPA): every
 * write made outside a transaction still commits (the store runs it in a short transaction of its own), so the relay marks
 * what it sent and never sends it again.
 */
abstract class JdbcAutoCommitOffTest(private val database: TestDatabase) {
    /** An event the service publishes in its transaction; its listener appends to the outbox after the commit. */
    data class OrderPlaced(val id: String)

    class AfterCommitListener(private val store: JdbcMessageStore) {
        @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
        fun on(event: OrderPlaced) = runBlocking {
            store.outbox.append(OutboxEntry(event.id, "t.OrderPlaced", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 1))
        }
    }

    /**
     * A `@TransactionalEventListener(AFTER_COMMIT)` runs once the transaction ended (its after-completion phase): the store's write
     * there runs in a transaction of its own, so it commits on an auto-commit=false pool too.
     */
    @Test
    fun anAfterCommitListenersWriteCommits() {
        database.dataSource(autoCommit = false).use { ds ->
            val store = JdbcMessageStore(ds, database.dialect).also { it.initSchema() }
            ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java))
                .withBean(PlatformTransactionManager::class.java, { DataSourceTransactionManager(ds) })
                .withBean(AfterCommitListener::class.java, { AfterCommitListener(store) })
                .run { context ->
                    assertNull(context.startupFailure)
                    TransactionTemplate(context.getBean(PlatformTransactionManager::class.java)).executeWithoutResult {
                        context.publishEvent(OrderPlaced("after-commit"))
                    }
                    assertEquals(1L, runBlocking { store.outbox.pending() }, "the listener's append committed")
                }
        }
    }

    @Test
    fun writesOutsideATransactionCommit() {
        database.dataSource(autoCommit = false).use { ds ->
            val store = JdbcMessageStore(ds, database.dialect).also { it.initSchema() }
            val t = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            runBlocking {
                store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), """{"n":1}""".toByteArray(), t, 1))
                assertTrue(store.processedEvents.record(ProcessedMarker("processor:default:evt-1", "t.Happened", t)))
            }
            val events = MemoryEventStore()
            val relay = OutboxPublisher(store.outbox, events, "kraken.events", "orders", pollInterval = Duration.ofMinutes(10), batchSize = 10)
            assertEquals(1, relay.publishPendingNow())
            assertEquals(0, relay.publishPendingNow(), "marked sent: never sent again")
            assertEquals(1, events.events("kraken.events").size)

            val dispatch = store.sagas.dispatch
            assertTrue(dispatch.claimLease("s1", "relay-a", until = t.plusSeconds(60), now = t))
            assertTrue(dispatch.renewLease("s1", "relay-a", until = t.plusSeconds(120)))
            dispatch.releaseLease("s1", "relay-a")

            // Read back through another store: the pool rolled back whatever a returned connection left uncommitted.
            val again = JdbcMessageStore(ds, database.dialect)
            runBlocking {
                assertEquals(0L, again.outbox.pending())
                assertTrue(again.processedEvents.exists("processor:default:evt-1"))
            }
            assertTrue(again.sagas.dispatch.claimLease("s1", "relay-b", until = t.plusSeconds(60), now = t), "the release was committed")
        }
    }
}

class PostgresAutoCommitOffTest : JdbcAutoCommitOffTest(TestDatabase.POSTGRES)

class MySqlAutoCommitOffTest : JdbcAutoCommitOffTest(TestDatabase.MYSQL)
