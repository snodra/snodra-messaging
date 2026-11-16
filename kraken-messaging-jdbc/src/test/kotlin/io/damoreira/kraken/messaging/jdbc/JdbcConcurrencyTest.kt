package io.damoreira.kraken.messaging.jdbc

import com.zaxxer.hikari.HikariDataSource
import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The lock behaviours a database adds to the store kit's checks: on MySQL, REPEATABLE READ locks every row a locking read
 * scanned and an UPDATE or a locking read that finds no row takes a gap lock; neither may break the claims' order or turn two
 * claims into a deadlock.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JdbcConcurrencyTest(private val database: TestDatabase) {
    private lateinit var dataSource: HikariDataSource
    private lateinit var store: JdbcMessageStore

    @BeforeAll
    fun start() {
        dataSource = database.dataSource(poolSize = 40)
        JdbcMessageStore(dataSource, database.dialect).initSchema()
    }

    @AfterAll
    fun stop() = dataSource.close()

    @BeforeEach
    fun clear() {
        dataSource.connection.use { c -> c.createStatement().use { s -> JdbcTables.ALL.reversed().forEach { s.execute("DELETE FROM $it") } } }
        store = JdbcMessageStore(dataSource, database.dialect)
    }

    /**
     * Relay B holds e1 (its send failed); while relay A's claim (of e2) is still open, B's next claim returns e1 first, then e3:
     * A's claim must not keep e1, which it scanned but did not take, locked (MySQL at REPEATABLE READ would).
     */
    @Test
    fun anOpenClaimDoesNotHideAnotherRelaysEntries() = runBlocking {
        val t = now()
        listOf("e1", "e2", "e3").forEachIndexed { i, id -> store.outbox.append(OutboxEntry(id, "t.Happened", "k", emptyMap(), "{}".toByteArray(), t, i.toLong())) }
        assertEquals(listOf("e1"), store.outbox.claim("B", Duration.ofMinutes(5), 1).map { it.id })
        store.outbox.markAttemptFailed("e1", "broker unavailable")

        val claimed = CountDownLatch(1)
        val release = CountDownLatch(1)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val a = pool.submit(
                Callable {
                    store.db.own { jdbc ->
                        val rows = jdbc.sql(JdbcOutboxStore.CLAIM).param("owner", "A").param("now", store.db.ts(store.db.now())).param("limit", 1)
                            .query { rs, _ -> rs.getString("id") }.list()
                        claimed.countDown()
                        release.await(10, TimeUnit.SECONDS)
                        rows
                    }
                },
            )
            assertTrue(claimed.await(10, TimeUnit.SECONDS))
            assertEquals(listOf("e1", "e3"), store.outbox.claim("B", Duration.ofMinutes(5), 10).map { it.id }, "B's failed entry first, then the free one")
            release.countDown()
            assertEquals(listOf("e2"), a.get(10, TimeUnit.SECONDS))
        } finally {
            release.countDown()
            pool.shutdownNow()
        }
    }

    /** Two resets locking a processor that has no row yet: one takes the lock, the other is told who holds it, neither fails. */
    @Test
    fun concurrentLocksOfANewProcessorGiveOneHolder() {
        repeat(10) { round ->
            val processor = "p-$round"
            val results = race(2) { i -> runBlocking { store.processorControl.lock(processor, "op-$i", Duration.ofSeconds(30)) } }
            assertEquals(1, results.count { it == ProcessorControlStore.Lock.Taken }, "round $round: $results")
            assertEquals(1, results.count { it is ProcessorControlStore.Lock.Held }, "round $round: $results")
        }
    }

    /** Two relays taking over one expired dispatch lease: exactly one gets it, the other is told no, neither fails. */
    @Test
    fun concurrentTakeoversOfAnExpiredLeaseGiveOneOwner() {
        val dispatch = store.sagas.dispatch
        val t = now()
        repeat(20) { round ->
            val saga = "s-$round"
            assertTrue(dispatch.claimLease(saga, "crashed", until = t.minusSeconds(1), now = t.minusSeconds(31)))
            val won = race(2) { i -> dispatch.claimLease(saga, "relay-$i", until = t.plusSeconds(30), now = t) }
            assertEquals(1, won.count { it }, "round $round: $won")
        }
        repeat(20) { round ->
            val won = race(2) { i -> dispatch.claimLease("new-$round", "relay-$i", until = t.plusSeconds(30), now = t) }
            assertEquals(1, won.count { it }, "a new lease, round $round: $won")
        }
    }

    /** Sixteen transactions starting sagas of different keys at once, all open together: none fails (no gap-lock deadlock). */
    @Test
    fun concurrentStartsOfDifferentKeysNeverConflict() {
        val n = 16
        val t = now()
        val allInserted = CountDownLatch(n)
        val failures = race(n) { i ->
            runCatching {
                TransactionTemplate(store.transactions).executeWithoutResult {
                    store.sagas.sagas.insert(
                        SagaDocument("s-$i", "t.Order", "{}", listOf(Association("orderId", "o-$i")), "t.Order|orderId=o-$i", createdAt = t, updatedAt = t),
                    )
                    allInserted.countDown()
                    allInserted.await(10, TimeUnit.SECONDS) // every transaction holds its insert until all inserted
                }
            }.exceptionOrNull()
        }
        assertEquals(emptyList<Throwable>(), failures.filterNotNull())
        assertEquals(n.toLong(), store.sagas.sagas.countLiveByType()["t.Order"])
    }

    /** Two first-time saves of one saga failure at once (each in a transaction): both succeed, one row is left. */
    @Test
    fun concurrentFirstSavesOfOneFailureBothSucceed() {
        val t = now()
        repeat(10) { round ->
            val id = "t.Order:e-$round"
            val failures = race(2) { i ->
                runCatching {
                    TransactionTemplate(store.transactions).executeWithoutResult {
                        store.sagas.failures.save(SagaFailureDocument(id, "t.Order", "e-$round", "t.Paid", "{}", emptyMap(), "boom $i", 3, t, t))
                    }
                }.exceptionOrNull()
            }
            assertEquals(listOf<Throwable?>(null, null), failures, "round $round")
            assertEquals(1, store.sagas.failures.list("t.Order").count { it.id == id })
        }
    }

    /** Two re-saves of an existing saga failure at once (each in a transaction): both succeed, one row is left. */
    @Test
    fun concurrentReSavesOfOneFailureBothSucceed() {
        val t = now()
        repeat(10) { round ->
            val id = "t.Order:r-$round"
            store.sagas.failures.save(SagaFailureDocument(id, "t.Order", "r-$round", "t.Paid", "{}", emptyMap(), "first", 1, t, t))
            val failures = race(2) { i ->
                runCatching {
                    TransactionTemplate(store.transactions).executeWithoutResult {
                        store.sagas.failures.save(SagaFailureDocument(id, "t.Order", "r-$round", "t.Paid", "{}", emptyMap(), "again $i", 2, t, t))
                    }
                }.exceptionOrNull()
            }
            assertEquals(listOf<Throwable?>(null, null), failures, "round $round")
            assertEquals(2, store.sagas.failures.find(id)!!.attempts)
        }
    }

    private fun <T> race(n: Int, work: (Int) -> T): List<T> {
        val start = CyclicBarrier(n)
        val pool = Executors.newFixedThreadPool(n)
        try {
            return (0 until n).map { i -> pool.submit(Callable { start.await(10, TimeUnit.SECONDS); work(i) }) }.map { it.get(60, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)
}

class PostgresConcurrencyTest : JdbcConcurrencyTest(TestDatabase.POSTGRES)

class MySqlConcurrencyTest : JdbcConcurrencyTest(TestDatabase.MYSQL)
