package io.damoreira.kraken.messaging.store.memory

import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.ProcessedMarker
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.dao.ConcurrencyFailureException
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.UnexpectedRollbackException
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** What the conformance kit does not pin about the in-memory message store: its transactions and the relay's wake-up. */
class MemoryMessageStoreTest {
    private val store = MemoryMessageStore()
    private val tx = TransactionTemplate(store.transactions)

    private fun marker(id: String) = ProcessedMarker(id, "t.Happened", Instant.now())

    private fun entry(id: String) = OutboxEntry(id, "t.Happened", "k", emptyMap(), """{"id":"$id"}""".toByteArray(), Instant.now(), 1)

    @Test
    fun aTransactionReadsItsOwnWritesAndNobodyElseDoesBeforeTheCommit() {
        val inside = CountDownLatch(1)
        val checked = CountDownLatch(1)
        val writer = CompletableFuture.runAsync {
            tx.executeWithoutResult {
                runBlocking {
                    assertTrue(store.processedEvents.record(marker("m-1")))
                    assertTrue(store.processedEvents.exists("m-1"), "its own write")
                    store.outbox.append(entry("e-1"))
                }
                inside.countDown()
                checked.await(5, TimeUnit.SECONDS)
            }
        }
        assertTrue(inside.await(5, TimeUnit.SECONDS))
        runBlocking {
            assertFalse(store.processedEvents.exists("m-1"), "not before the commit")
            assertEquals(0L, store.outbox.pending(), "not before the commit")
        }
        checked.countDown()
        writer.get(5, TimeUnit.SECONDS)
        runBlocking {
            assertTrue(store.processedEvents.exists("m-1"))
            assertEquals(listOf("e-1"), store.outbox.claim("relay", Duration.ofSeconds(30), 10).map { it.id })
        }
    }

    @Test
    fun twoLiveTransactionsWritingOneRowConflictAtTheSecondWrite() {
        val first = CountDownLatch(1)
        val done = CountDownLatch(1)
        val holder = CompletableFuture.runAsync {
            tx.executeWithoutResult {
                runBlocking { assertTrue(store.processedEvents.record(marker("m-1"))) }
                first.countDown()
                done.await(5, TimeUnit.SECONDS)
            }
        }
        assertTrue(first.await(5, TimeUnit.SECONDS))
        assertFailsWith<ConcurrencyFailureException> {
            tx.executeWithoutResult { runBlocking { store.processedEvents.record(marker("m-1")) } }
        }
        done.countDown()
        holder.get(5, TimeUnit.SECONDS)
        runBlocking { assertTrue(store.processedEvents.exists("m-1")) }
        tx.executeWithoutResult { runBlocking { assertFalse(store.processedEvents.record(marker("m-1")), "committed: a duplicate now") } }
    }

    @Test
    fun aRowChangedUnderATransactionFailsItsCommitAndNothingOfItApplies() {
        runBlocking { store.outbox.append(entry("e-1")) }
        val saga = SagaDocument("s1", "t.Order", "{}", createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH)
        assertFailsWith<ConcurrencyFailureException> {
            tx.executeWithoutResult {
                store.sagas.sagas.insert(saga)
                runBlocking { store.deadLetters.deleteInTransaction("none") }
                // Another writer commits the same saga id meanwhile (outside this transaction).
                CompletableFuture.runAsync { store.sagas.sagas.insert(saga.copy(state = """{"other":true}""")) }.get(5, TimeUnit.SECONDS)
            }
        }
        assertEquals("""{"other":true}""", store.sagas.sagas.findById("s1")?.state)
    }

    @Test
    fun twoTransactionsStartingOneSagaKeyCannotBothCommit() {
        fun saga(id: String) = SagaDocument(id, "t.Order", "{}", startKey = "t.Order|orderId=o-1", createdAt = Instant.EPOCH, updatedAt = Instant.EPOCH)
        val inside = CountDownLatch(1)
        val release = CountDownLatch(1)
        val first = CompletableFuture.runAsync {
            tx.executeWithoutResult {
                store.sagas.sagas.insert(saga("s1"))
                inside.countDown()
                release.await(5, TimeUnit.SECONDS)
            }
        }
        assertTrue(inside.await(5, TimeUnit.SECONDS))
        tx.executeWithoutResult { store.sagas.sagas.insert(saga("s2")) } // the first is not committed yet
        release.countDown()
        val failure = runCatching { first.get(5, TimeUnit.SECONDS) }.exceptionOrNull()?.cause
        assertTrue(failure is DuplicateKeyException, "the second commit of a live start key fails: $failure")
        assertEquals("s2", store.sagas.sagas.findById("s2")?.id)
        assertEquals(null, store.sagas.sagas.findById("s1"), "nothing of the failed commit applied")
    }

    @Test
    fun requiresNewCommitsOnItsOwnAndTheOuterRollbackKeepsIt() {
        val own = TransactionTemplate(store.transactions).apply { propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW }
        tx.executeWithoutResult { status ->
            runBlocking { store.processedEvents.record(marker("outer")) }
            own.executeWithoutResult { runBlocking { store.processedEvents.record(marker("inner")) } }
            runBlocking { assertTrue(store.processedEvents.exists("outer"), "the outer transaction is back after the inner one") }
            status.setRollbackOnly()
        }
        runBlocking {
            assertTrue(store.processedEvents.exists("inner"), "the inner transaction committed")
            assertFalse(store.processedEvents.exists("outer"), "the outer one rolled back")
        }
    }

    @Test
    fun aParticipantMarkedRollbackOnlyRollsTheWholeTransactionBack() {
        assertFailsWith<UnexpectedRollbackException> {
            tx.executeWithoutResult {
                runBlocking { store.processedEvents.record(marker("m-1")) }
                tx.executeWithoutResult { inner -> inner.setRollbackOnly() }
            }
        }
        runBlocking { assertFalse(store.processedEvents.exists("m-1")) }
    }

    @Test
    fun aWriteFromAnAfterCompletionCallbackCommitsOnItsOwn() {
        tx.executeWithoutResult { status ->
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCompletion(status: Int) {
                    runBlocking { store.processedEvents.record(marker("after")) } // the transaction ended: none to join
                }
            })
            status.setRollbackOnly()
        }
        runBlocking { assertTrue(store.processedEvents.exists("after")) }
    }

    @Test
    fun aTransactionBegunFromAnAfterCompletionCallbackIsANewOne() {
        tx.executeWithoutResult {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCompletion(status: Int) {
                    tx.executeWithoutResult { runBlocking { store.processedEvents.record(marker("next")) } }
                }
            })
            runBlocking { store.processedEvents.record(marker("first")) }
        }
        runBlocking {
            assertTrue(store.processedEvents.exists("first"))
            assertTrue(store.processedEvents.exists("next"), "the second transaction began, joined nothing and committed")
        }
        tx.executeWithoutResult { runBlocking { assertTrue(store.processedEvents.record(marker("later")), "nothing left bound") } }
    }

    @Test
    fun theRelayIsWokenWhenAnAppendCommitsNeverForARollback() {
        val woken = AtomicInteger()
        store.outbox.onAppend { woken.incrementAndGet() }
        tx.executeWithoutResult { status ->
            runBlocking { store.outbox.append(entry("rolled-back")) }
            status.setRollbackOnly()
        }
        assertEquals(0, woken.get(), "nothing committed")
        tx.executeWithoutResult { runBlocking { store.outbox.append(entry("e-1")) } }
        assertEquals(1, woken.get(), "woken at the commit")
        runBlocking { store.outbox.append(entry("e-2")) }
        assertEquals(2, woken.get(), "an append outside a transaction commits at once")
    }
}
