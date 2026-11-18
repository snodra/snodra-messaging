package com.damoreira.snodra.messaging.jdbc

import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetterError
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import com.damoreira.snodra.messaging.external.ExternalCallStatus
import com.damoreira.snodra.messaging.saga.Association
import com.damoreira.snodra.messaging.saga.DispatchKind
import com.damoreira.snodra.messaging.saga.SagaCommandFailed
import com.damoreira.snodra.messaging.saga.SagaDispatchDocument
import com.damoreira.snodra.messaging.saga.SagaFailureDocument
import com.damoreira.snodra.messaging.store.ExternalCallRecord
import com.damoreira.snodra.messaging.saga.DeadlineDocument
import com.damoreira.snodra.messaging.saga.SagaDocument
import com.damoreira.snodra.messaging.store.OutboxEntry
import com.damoreira.snodra.messaging.store.ProcessedMarker
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.simple.JdbcClient
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

/** The retention: what the MongoDB add-on's TTL indexes expire, the purge deletes; nothing pending or live goes. */
abstract class JdbcPurgeTest(private val database: TestDatabase) {
    @Test
    fun purgeDeletesWhatTheRetentionsLetGo() = runBlocking {
        database.dataSource().use { ds ->
            val t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            val store = JdbcMessageStore(ds, database.dialect, clock = Clock.fixed(t0, ZoneOffset.UTC)).also { it.initSchema() }
            listOf("sent", "unsent").forEachIndexed { i, id ->
                store.outbox.append(OutboxEntry(id, "t.Happened", "k", emptyMap(), "{}".toByteArray(), t0, i.toLong()))
            }
            store.outbox.markSent(listOf("sent"))
            store.processedEvents.record(ProcessedMarker("processor:default:old", "t.Happened", t0))
            store.processedEvents.record(ProcessedMarker("processor:default:new", "t.Happened", t0.plus(Duration.ofDays(29))))
            store.sagas.sagas.insert(SagaDocument("ended", "t.Order", "{}", listOf(Association("k", "v")), createdAt = t0, updatedAt = t0, ended = true, endedAt = t0))
            store.sagas.sagas.insert(SagaDocument("live", "t.Order", "{}", listOf(Association("k", "v")), createdAt = t0, updatedAt = t0))
            store.sagas.deadlines.insert(DeadlineDocument("fired", "t.Order", "live", "remind", null, null, t0, "c"))
            store.sagas.deadlines.markFired("fired", t0)
            store.sagas.deadlines.insert(DeadlineDocument("scheduled", "t.Order", "live", "remind", null, null, t0, "c"))
            store.deadLetters.park(letter("expiring", t0, expireAt = t0.plus(Duration.ofDays(1))))
            store.deadLetters.park(letter("kept", t0, expireAt = t0.plus(Duration.ofDays(60))))
            store.sagas.dispatch.claimLease("s1", "relay", until = t0.plusSeconds(30), now = t0)
            store.externalCalls.insert(ExternalCallRecord("print:old", ExternalCallStatus.SUCCEEDED, updatedAt = t0))
            store.externalCalls.insert(ExternalCallRecord("print:new", ExternalCallStatus.SUCCEEDED, updatedAt = t0.plus(Duration.ofDays(29))))
            fun entry(id: String) = SagaDispatchDocument(id, "live", id.length.toLong(), DispatchKind.COMMAND, "t.Charge", "{}", createdAt = t0)
            store.sagas.dispatch.record(listOf(entry("dispatched"), entry("given-up"), entry("pending")))
            store.sagas.dispatch.markDispatched("dispatched", t0)
            store.sagas.dispatch.markFailed("given-up", 3, SagaCommandFailed.Reason.EXHAUSTED, "gave up", t0, t0)
            store.sagas.failures.save(SagaFailureDocument("t.Order:e1", "t.Order", "e1", "t.Paid", "{}", emptyMap(), "boom", 3, t0, t0))

            val later = JdbcMessageStore(ds, database.dialect, clock = Clock.fixed(t0.plus(Duration.ofDays(31)), ZoneOffset.UTC))
            // The sent entry, the old marker, the old external call, the ended saga (its association goes with it), the dispatched
            // and the given-up dispatch entries, the passed lease, the fired deadline, the saga failure, the expired letter.
            assertEquals(10L, later.purgeExpired())
            assertEquals(listOf("unsent"), JdbcClient.create(ds).sql("SELECT id FROM snodra_outbox").query(String::class.java).list())
            assertFalse(later.processedEvents.exists("processor:default:old"))
            assertTrue(later.processedEvents.exists("processor:default:new"))
            assertNull(later.sagas.sagas.findById("ended"))
            assertEquals(listOf("live"), later.sagas.sagas.findLive("t.Order", "k", "v").map { it.id })
            assertEquals(1L, JdbcClient.create(ds).sql("SELECT COUNT(*) FROM snodra_saga_associations").query(Long::class.javaObjectType).single())
            assertNull(later.sagas.deadlines.findScheduled("fired"))
            assertEquals("scheduled", later.sagas.deadlines.findScheduled("scheduled")?.id)
            assertFalse(later.deadLetters.exists("billing:t:0:1"))
            assertTrue(later.deadLetters.exists("billing:t:0:2"))
            assertTrue(later.sagas.dispatch.claimLease("s1", "other", until = t0.plus(Duration.ofDays(32)), now = t0.plus(Duration.ofDays(31))))
            assertNull(later.externalCalls.find("print:old"))
            assertEquals("print:new", later.externalCalls.find("print:new")?.id)
            assertEquals(listOf("pending"), JdbcClient.create(ds).sql("SELECT id FROM snodra_saga_dispatch").query(String::class.java).list())
            assertEquals(emptyList<Any>(), later.sagas.failures.list(null))
            assertEquals(0L, later.purgeExpired(), "nothing left to purge")
        }
    }

    private fun letter(id: String, at: Instant, expireAt: Instant): DeadLetter {
        val position = if (id == "expiring") "1" else "2"
        return DeadLetter(
            "billing:t:0:$position", "billing", "t", "0", position, "k", "e-$id", "t.Happened", emptyMap(), "{}", null,
            DeadLetterReason.HANDLER_FAILED, DeadLetterError("Boom", "boom"), null, 1, at, at, expireAt,
        )
    }
}

class PostgresPurgeTest : JdbcPurgeTest(TestDatabase.POSTGRES)

class MySqlPurgeTest : JdbcPurgeTest(TestDatabase.MYSQL)
