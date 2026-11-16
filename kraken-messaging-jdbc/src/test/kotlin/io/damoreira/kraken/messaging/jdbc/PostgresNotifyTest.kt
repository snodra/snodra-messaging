package io.damoreira.kraken.messaging.jdbc

import com.zaxxer.hikari.HikariDataSource
import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/** The relay wake-up on PostgreSQL: `LISTEN kraken_outbox`, notified by an append's commit. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresNotifyTest {
    private lateinit var connection: TestDatabase.Connection
    private lateinit var dataSource: HikariDataSource

    @BeforeAll
    fun start() {
        connection = TestDatabase.POSTGRES.create()
        dataSource = connection.dataSource()
        JdbcMessageStore(dataSource, PostgresDialect).initSchema()
    }

    @AfterAll
    fun stop() {
        dataSource.close()
        connection.drop()
    }

    @Test
    fun notify_wakes_relay_before_poll() {
        val store = JdbcMessageStore(dataSource, PostgresDialect, listenConnection = { DriverManager.getConnection(connection.url, connection.user, connection.password) })
        val events = MemoryEventStore()
        val relay = OutboxPublisher(store.outbox, events, "kraken.events", "orders", pollInterval = Duration.ofMinutes(10), batchSize = 100)
        relay.start()
        try {
            // LISTEN is in place (and its start-up wake-up spent) before anything is appended: only a NOTIFY wakes the relay later.
            val notifications = (store.outbox as JdbcOutboxStore).notifications!!
            await().atMost(Duration.ofSeconds(10)).until { notifications.listens > 0 }
            append(store, "e-1")
            // Relayed by the first poll or by the wake-up; either way the relay now waits ten minutes for its next poll.
            await().atMost(Duration.ofSeconds(10)).until { events.events("kraken.events").map { it.id } == listOf("e-1") }
            append(store, "e-2")
            await().atMost(Duration.ofSeconds(5)).until { events.events("kraken.events").map { it.id } == listOf("e-1", "e-2") }
        } finally {
            relay.stop()
        }
    }

    @Test
    fun only_a_commit_notifies_and_listening_ends_with_the_last_relay() {
        val store = JdbcMessageStore(dataSource, PostgresDialect) // the wake-up borrows its connection from the pool
        val wakes = AtomicInteger()
        val handle = store.outbox.onAppend { wakes.incrementAndGet() }
        val notifications = (store.outbox as JdbcOutboxStore).notifications!!
        await().atMost(Duration.ofSeconds(10)).until { notifications.listens > 0 && wakes.get() >= 1 } // listening, its start-up wake-up spent
        val listening = wakes.get()

        TransactionTemplate(store.transactions).executeWithoutResult { status ->
            runBlocking { store.outbox.append(entry("rolled-back")) }
            status.setRollbackOnly()
        }
        Thread.sleep(1500) // three waits of the listening thread
        assertEquals(listening, wakes.get(), "a rolled-back append notifies nobody")

        append(store, "committed")
        await().atMost(Duration.ofSeconds(5)).until { wakes.get() > listening }

        assertTrue(notifications.listening)
        handle.close()
        assertFalse(notifications.listening, "no listening thread outlives the last relay")
    }

    private fun append(store: JdbcMessageStore, id: String) =
        TransactionTemplate(DataSourceTransactionManager(dataSource)).executeWithoutResult { runBlocking { store.outbox.append(entry(id)) } }

    private fun entry(id: String) = OutboxEntry(id, "t.Happened", "k", emptyMap(), """{"id":"$id"}""".toByteArray(), Instant.now(), 1)
}
