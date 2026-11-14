package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.ProcessedMarker
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
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
