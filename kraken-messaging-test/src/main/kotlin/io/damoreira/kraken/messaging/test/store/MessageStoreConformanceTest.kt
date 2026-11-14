package io.damoreira.kraken.messaging.test.store

import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.external.ExternalCallStatus
import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.ProcessedMarker
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The conformance kit of the message store (ADR 0028): the behaviour every store add-on must have, whatever its database.
 * An add-on runs it by subclassing this class in its own tests and implementing [store]:
 *
 * ```kotlin
 * class MongoMessageStoreConformanceTest : MessageStoreConformanceTest() {
 *     override fun store(): MessageStore = MongoMessageStore(templateOverAFreshDatabase(), ...)
 * }
 * ```
 *
 * [store] is called once before every test and must return a store over **empty** state (a fresh database or schema, or
 * one cleared first): the checks count what the outbox holds. Its schema (indexes, tables) must be ready, as the add-on's
 * auto-configuration leaves it. The checks use the wall clock with leases of about three seconds, never a fake clock.
 *
 * Transactions are Spring's: a check opens one with a [TransactionTemplate] over [MessageStore.transactions] and calls the
 * store inside it on the same thread, as the library does; a store joins it there.
 *
 * Instants are compared at millisecond precision (what every first-wave database keeps), so the checks build them truncated.
 */
abstract class MessageStoreConformanceTest {
    /** A store over empty state, called before each test. */
    abstract fun store(): MessageStore

    private lateinit var store: MessageStore

    @BeforeEach
    fun createStore() {
        store = store()
    }

    // ---------------------------------------------------------------------------------------------------------------- outbox

    @Test
    fun appendedEntryIsClaimedInCreatedSeqOrder() = runBlocking {
        val t0 = now()
        val t1 = t0.plusMillis(5)
        // Appended out of order: same millisecond broken by seq, and an earlier createdAt with a larger seq.
        val c = entry("c", createdAt = t1, seq = 2, key = "k1")
        val a = entry("a", createdAt = t0, seq = 9, key = null)
        val d = entry("d", createdAt = t1, seq = 3, key = "k2")
        val b = entry("b", createdAt = t1, seq = 1, key = "k1", headers = mapOf("correlationId" to "corr-b", "userId" to ""))
        listOf(c, a, d, b).forEach { store.outbox.append(it) }
        assertEquals(4L, store.outbox.pending())

        val claimed = store.outbox.claim("relay-1", Duration.ofSeconds(30), limit = 3)
        assertEquals(listOf("a", "b", "c"), claimed.map { it.id }, "claimed in (createdAt, seq) order, up to the limit")
        assertEntry(b, claimed[1])
        assertEntry(a, claimed[0])

        // The owner's next claim returns what it still holds, in order, plus what is free; another owner gets nothing it holds.
        assertEquals(listOf("d"), store.outbox.claim("relay-2", Duration.ofSeconds(30), limit = 10).map { it.id })
        assertEquals(listOf("a", "b", "c"), store.outbox.claim("relay-1", Duration.ofSeconds(30), limit = 10).map { it.id })

        store.outbox.markSent(listOf("a", "b"))
        assertEquals(listOf("c"), store.outbox.claim("relay-1", Duration.ofSeconds(30), limit = 10).map { it.id })
        assertEquals(2L, store.outbox.pending())
    }

    @Test
    fun rolledBackTransactionLeavesNoOutboxEntry() = runBlocking {
        val kept = entry("committed", createdAt = now(), seq = 1)
        val lost = entry("rolled-back", createdAt = now(), seq = 2)
        inTransaction { store.outbox.append(kept) }
        inTransaction(rollback = true) { store.outbox.append(lost) }
        assertThrows(HandlerFailed::class.java) {
            inTransaction {
                store.outbox.append(entry("thrown", createdAt = now(), seq = 3))
                throw HandlerFailed()
            }
        }

        assertEquals(listOf("committed"), store.outbox.claim("relay", Duration.ofSeconds(30), limit = 10).map { it.id })
        assertEquals(1L, store.outbox.pending())
    }

    @Test
    fun concurrentRelaysPublishEachEntryOnce() = runBlocking {
        val base = now()
        val all = (0 until 200).map { i -> entry("e-%03d".format(i), createdAt = base.plusMillis(i / 10L), seq = i.toLong(), key = "k${i % 7}") }
        all.forEach { store.outbox.append(it) }

        val published = ConcurrentHashMap<String, MutableList<String>>()
        val start = CyclicBarrier(2)
        val firstClaimed = CyclicBarrier(2)
        suspend fun relay(owner: String) {
            val mine = published.getOrPut(owner) { mutableListOf() }
            start.await(10, TimeUnit.SECONDS) // both relays start claiming together
            var first = true
            while (true) {
                val batch = store.outbox.claim(owner, Duration.ofSeconds(60), limit = 7)
                if (first) {
                    // Both relays hold their first batch before either marks anything sent: their claims overlapped in time.
                    first = false
                    firstClaimed.await(10, TimeUnit.SECONDS)
                }
                if (batch.isEmpty()) return
                mine += batch.map { it.id }
                store.outbox.markSent(batch.map { it.id })
            }
        }
        withContext(Dispatchers.IO) {
            listOf(async { relay("relay-a") }, async { relay("relay-b") }).awaitAll()
        }

        val a = published.getValue("relay-a")
        val b = published.getValue("relay-b")
        assertEquals(a.size, a.toSet().size, "relay-a published an entry twice")
        assertEquals(b.size, b.toSet().size, "relay-b published an entry twice")
        assertTrue(a.isNotEmpty() && b.isNotEmpty(), "both relays claimed entries while the other held its own: ${a.size} and ${b.size}")
        assertEquals(emptySet<String>(), a.toSet() intersect b.toSet(), "an entry was published by both relays")
        assertEquals(all.map { it.id }.toSet(), a.toSet() + b.toSet(), "every entry was published")
        assertEquals(0L, store.outbox.pending())
    }

    @Test
    fun outboxPayloadMustBeAJsonObject() = runBlocking {
        val tooBig = "9223372036854775808" // Long.MAX_VALUE + 1
        for (payload in listOf("[1,2]", "42", "\"text\"", "null", "{not json", """{"n":$tooBig}""", """{"a":{"b":[-9223372036854775809]}}""")) {
            val entry = OutboxEntry("e-$payload", "t.Happened", "k", emptyMap(), payload.toByteArray(), now(), seq = 1)
            assertThrows(IllegalArgumentException::class.java, { runBlocking { store.outbox.append(entry) } }, payload)
        }
        assertEquals(0L, store.outbox.pending(), "nothing was appended")
        assertEquals(emptyList<OutboxEntry>(), store.outbox.claim("relay", Duration.ofSeconds(30), limit = 10))
        // The bounds themselves are fine.
        store.outbox.append(OutboxEntry("bounds", "t.Happened", "k", emptyMap(), """{"max":${Long.MAX_VALUE},"min":${Long.MIN_VALUE}}""".toByteArray(), now(), seq = 2))
        assertJson("""{"max":${Long.MAX_VALUE},"min":${Long.MIN_VALUE}}""".toByteArray(), store.outbox.claim("relay", Duration.ofSeconds(30), limit = 10).single().payload)
    }

    @Test
    fun oldestPendingIsTheOldestEntryNeitherSentNorFailed() = runBlocking {
        assertNull(store.outbox.oldestPending(), "an empty outbox")
        val t0 = now()
        listOf(entry("a", t0, 1), entry("b", t0.plusMillis(1), 2), entry("c", t0.plusMillis(2), 3), entry("d", t0.plusMillis(3), 4))
            .forEach { store.outbox.append(it) }
        assertEquals(t0, store.outbox.oldestPending())
        store.outbox.claim("relay-1", Duration.ofSeconds(30), limit = 3) // held entries count: the gauge is store-wide
        assertEquals(t0, store.outbox.oldestPending())
        store.outbox.markSent(listOf("a"))
        store.outbox.markFailed("b", "not retriable")
        assertEquals(t0.plusMillis(2), store.outbox.oldestPending(), "sent and failed entries are not pending")
        store.outbox.markSent(listOf("c", "d"))
        assertNull(store.outbox.oldestPending())
    }

    @Test
    fun expiredLeaseIsReclaimed() = runBlocking {
        val base = now()
        listOf("e1", "e2", "e3").forEachIndexed { i, id -> store.outbox.append(entry(id, createdAt = base, seq = i.toLong())) }
        val lease = Duration.ofSeconds(3)

        assertEquals(listOf("e1", "e2", "e3"), store.outbox.claim("crashed", lease, limit = 10).map { it.id })
        store.outbox.markAttemptFailed("e1", "broker unavailable") // one attempt, the entry stays the claimer's
        store.outbox.markFailed("e3", "record too large") // given up on: never claimed again
        assertEquals(emptyList<String>(), store.outbox.claim("survivor", lease, limit = 10).map { it.id }, "claimed while the lease holds")
        assertEquals(2L, store.outbox.pending(), "a failed entry is not pending")

        Thread.sleep(lease.toMillis() + 500)
        val reclaimed = store.outbox.claim("survivor", lease.multipliedBy(30), limit = 10)
        assertEquals(listOf("e1", "e2"), reclaimed.map { it.id }, "reclaimed after the lease, in order, without the failed entry")
        assertEquals(1, reclaimed[0].attempts, "the failed attempt is counted")
        assertEquals(0, reclaimed[1].attempts)
        assertEquals(emptyList<String>(), store.outbox.claim("crashed", lease, limit = 10).map { it.id }, "the survivor holds them now")
    }

    @Test
    fun purgeRemovesOnlySentBefore() = runBlocking {
        val base = now()
        listOf("old", "new", "unsent", "failed").forEachIndexed { i, id -> store.outbox.append(entry(id, createdAt = base, seq = i.toLong())) }
        assertEquals(4, store.outbox.claim("relay", Duration.ofSeconds(30), limit = 10).size)
        store.outbox.markSent(listOf("old"))
        store.outbox.markFailed("failed", "not retriable")
        Thread.sleep(50)
        val cut = Instant.now()
        Thread.sleep(50)
        store.outbox.markSent(listOf("new"))

        assertEquals(0L, store.outbox.purge(base.minusSeconds(60)), "nothing was sent that early")
        assertEquals(1L, store.outbox.purge(cut), "only the entry sent before the cut")
        assertEquals(listOf("unsent"), store.outbox.claim("relay", Duration.ofSeconds(30), limit = 10).map { it.id }, "unsent entries are never purged")
        assertEquals(1L, store.outbox.purge(Instant.now().plusSeconds(60)), "the entry sent after the cut stayed until now")
        assertEquals(1L, store.outbox.pending())
    }

    // ------------------------------------------------------------------------------------------------------ processed events

    @Test
    fun rolledBackHandlerLeavesNoProcessedMarker() = runBlocking {
        val marker = marker("processor:default:evt-1")
        assertThrows(HandlerFailed::class.java) {
            inTransaction {
                assertTrue(store.processedEvents.record(marker))
                throw HandlerFailed() // the handler failed after the marker was written
            }
        }
        assertFalse(store.processedEvents.exists(marker.id), "the marker rolled back with the handler")

        inTransaction(rollback = true) { assertTrue(store.processedEvents.record(marker)) }
        assertFalse(store.processedEvents.exists(marker.id), "the marker rolled back with the transaction")

        inTransaction { assertTrue(store.processedEvents.record(marker)) }
        assertTrue(store.processedEvents.exists(marker.id), "the marker committed with the handler")
    }

    @Test
    fun processedMarkerRecordsOnce() = runBlocking {
        val at = now()
        val stage1 = marker("processor:default:evt-1", at.minus(Duration.ofHours(2)), processor = "default", topic = "kraken.events", partition = 3, offset = 42)
        val saga = marker("saga:Order:evt-1", at)
        assertFalse(store.processedEvents.exists(stage1.id))
        assertTrue(store.processedEvents.record(stage1))
        assertFalse(store.processedEvents.record(stage1), "a second record of the same id")
        assertFalse(store.processedEvents.record(stage1.copy(type = "other.Type", processedAt = at)), "the id alone decides")
        assertTrue(store.processedEvents.record(saga), "another id")
        assertTrue(store.processedEvents.exists(stage1.id))

        // Recorded until purged: only markers processed before the cut go.
        assertEquals(0L, store.processedEvents.purge(at.minus(Duration.ofHours(3))))
        assertEquals(1L, store.processedEvents.purge(at.minus(Duration.ofHours(1))))
        assertFalse(store.processedEvents.exists(stage1.id))
        assertTrue(store.processedEvents.exists(saga.id))
        assertTrue(store.processedEvents.record(stage1), "a purged marker can be recorded again")
    }

    // ---------------------------------------------------------------------------------------------------------- dead letters

    @Test
    fun deadLettersSequencePerKey() = runBlocking {
        val letters = store.deadLetters
        val t = now()
        val head = letter(PROCESSOR, "k1", partition = 0, offset = 3, parkedAt = t.plusMillis(1), headers = mapOf("type" to "t.Ordered", "id" to "evt-3"))
        val waiting5 = letter(PROCESSOR, "k1", partition = 0, offset = 5, parkedAt = t.plusMillis(2), reason = DeadLetterReason.SEQUENCED, blockedBy = head.id)
        val waiting7 = letter(PROCESSOR, "k1", partition = 0, offset = 7, parkedAt = t.plusMillis(2), reason = DeadLetterReason.SEQUENCED, blockedBy = head.id, replay = true)
        val invalid = letter(PROCESSOR, "k1", partition = 0, offset = 1, parkedAt = t, reason = DeadLetterReason.INVALID, payload = "not json")
        val otherKey = letter(PROCESSOR, "k2", partition = 1, offset = 4, parkedAt = t.plusMillis(3))
        val noKey = letter(PROCESSOR, null, partition = 0, offset = 9, parkedAt = t.plusMillis(4), payload = null, payloadBinary = byteArrayOf(0xC3.toByte(), 0x28))
        val invalidOnly = letter(PROCESSOR, "k-invalid", partition = 1, offset = 2, parkedAt = t.plusMillis(5), reason = DeadLetterReason.INVALID)
        val otherProcessor = letter("audit", "k1", partition = 0, offset = 3, parkedAt = t, payload = null)
        // Position 10: before 3 by its text, after it by value (decimal positions are ordered by value).
        val waiting10 = letter(PROCESSOR, "k1", partition = 0, offset = 10, parkedAt = t.plusMillis(6), reason = DeadLetterReason.SEQUENCED, blockedBy = head.id)
        for (l in listOf(head, waiting5, waiting7, invalid, otherKey, noKey, invalidOnly, otherProcessor, waiting10)) assertTrue(letters.park(l), "parked ${l.id}")
        assertFalse(letters.park(head.copy(attempts = 9)), "a letter is parked once")

        assertEquals(head.id, letters.head(PROCESSOR, "k1"), "the earliest position that is not INVALID, decimal positions by value")
        assertEquals(otherKey.id, letters.head(PROCESSOR, "k2"))
        assertNull(letters.head(PROCESSOR, "k3"))
        assertEquals(otherProcessor.id, letters.head("audit", "k1"), "sequencing is per processor")
        assertEquals(head.id, letters.earlier(PROCESSOR, "k1", "7"))
        assertEquals(head.id, letters.earlier(PROCESSOR, "k1", "10"), "decimal positions by value")
        assertNull(letters.earlier(PROCESSOR, "k1", "3"), "the INVALID letter below never blocks")
        assertTrue(letters.exists(waiting5.id))
        assertFalse(letters.exists("$PROCESSOR:$TOPIC:0:99"))

        assertEquals(mapOf("0" to setOf("k1"), "1" to setOf("k2")), letters.keys(PROCESSOR, TOPIC, listOf("0", "1")), "keys of blocking letters by segment, no INVALID-only key")
        assertEquals(mapOf("1" to setOf("k2")), letters.keys(PROCESSOR, TOPIC, listOf("1")))
        assertEquals(emptyMap<String, Set<String>>(), letters.keys(PROCESSOR, TOPIC, emptyList()))
        assertEquals(emptyMap<String, Set<String>>(), letters.keys(PROCESSOR, "other.topic", listOf("0", "1")))

        assertEquals(8L, letters.count(PROCESSOR))
        assertEquals(9L, letters.count(null))
        assertEquals(
            listOf(invalid, head, waiting5, waiting7, otherKey, noKey, invalidOnly, waiting10).map { it.id },
            letters.list(PROCESSOR, 100).map { it.id },
            "by parkedAt, then id",
        )
        assertEquals(listOf(invalid.id, head.id), letters.list(PROCESSOR, 2).map { it.id })
        assertEquals(head, letters.get(head.id))
        assertEquals(waiting7, letters.get(waiting7.id))
        assertEquals(noKey, letters.get(noKey.id), "a binary payload")
        assertEquals(otherProcessor, letters.get(otherProcessor.id), "an empty payload")
        assertNull(letters.get("$PROCESSOR:$TOPIC:0:99"))
        assertEquals(
            listOf(invalid, invalidOnly, head, otherKey, waiting5, waiting7, noKey, waiting10).map { DeadLetterStore.Summary(it.id, it.key, it.position, it.reason, it.parkedAt) },
            letters.summaries(PROCESSOR),
            "by position (decimal ones by value), then id",
        )

        val before = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        assertTrue(letters.recordFailure(waiting5.id, DeadLetterError("Boom", "failed again")))
        val failed = letters.get(waiting5.id)!!
        assertEquals(DeadLetterReason.HANDLER_FAILED, failed.reason)
        assertEquals(DeadLetterError("Boom", "failed again"), failed.error)
        assertEquals(waiting5.attempts + 1, failed.attempts)
        assertNull(failed.blockedBy)
        assertFalse(failed.lastAttemptAt.isBefore(before), "lastAttemptAt moves to now")
        assertEquals(waiting5.parkedAt, failed.parkedAt)
        assertFalse(letters.recordFailure("$PROCESSOR:$TOPIC:0:99", DeadLetterError("Boom", "gone")))

        inTransaction(rollback = true) { assertTrue(letters.deleteInTransaction(head.id)) }
        assertTrue(letters.exists(head.id), "a rolled-back deletion keeps the letter")
        assertTrue(letters.delete(head.id))
        assertFalse(letters.delete(head.id))
        assertEquals(waiting5.id, letters.head(PROCESSOR, "k1"), "the next letter of the key is the head now")
        inTransaction { assertTrue(letters.deleteInTransaction(waiting5.id)) }
        assertEquals(waiting7.id, letters.head(PROCESSOR, "k1"))
        assertEquals(6L, letters.count(PROCESSOR))
    }

    // ----------------------------------------------------------------------------------------------------- processor control

    @Test
    fun controlLockIsExclusiveUntilLeaseExpires() = runBlocking {
        val control = store.processorControl
        val lease = Duration.ofSeconds(3)
        assertNull(control.get(PROCESSOR))

        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-a", lease))
        val held = control.lock(PROCESSOR, "op-b", lease)
        assertInstanceOf(ProcessorControlStore.Lock.Held::class.java, held)
        assertEquals("op-a", (held as ProcessorControlStore.Lock.Held).by?.id)
        assertEquals(ProcessorControlStore.KIND_RESET, held.by?.kind)
        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-a", lease), "the holder takes it again")
        assertFalse(control.renew(PROCESSOR, "op-b", lease), "only the holder renews")
        assertFalse(control.release(PROCESSOR, "op-b"), "only the holder releases")
        assertFalse(control.release(PROCESSOR, "op-a", expiredOnly = true), "the lease still holds")
        val doc = control.get(PROCESSOR)!!
        assertEquals("op-a", doc.operation?.id)
        assertFalse(doc.paused)
        assertEquals(emptyList<ReplayRange>(), doc.replay)
        assertEquals(setOf(PROCESSOR), control.read(listOf(PROCESSOR, "never-touched")).keys)

        Thread.sleep(lease.toMillis() + 500)
        assertTrue(doc.operation!!.expired(Instant.now()))
        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-b", lease.multipliedBy(30)), "an expired lease is free")
        assertFalse(control.renew(PROCESSOR, "op-a", lease), "the old holder lost it")
        assertFalse(control.release(PROCESSOR, "op-a"))
        assertInstanceOf(ProcessorControlStore.Lock.Held::class.java, control.lock(PROCESSOR, "op-a", lease))
        assertTrue(control.renew(PROCESSOR, "op-b", lease.multipliedBy(30)))
        assertTrue(control.release(PROCESSOR, "op-b"))
        assertNull(control.get(PROCESSOR)!!.operation)
        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-a", lease), "a released lock is free")
    }

    @Test
    fun pauseStateRoundTrips() = runBlocking {
        val control = store.processorControl
        val before = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        control.setPaused(PROCESSOR, true, "control-1")
        control.setPaused(PROCESSOR, true, "control-1") // idempotent
        val paused = control.get(PROCESSOR)!!
        assertTrue(paused.paused)
        assertEquals("control-1", paused.pausedBy)
        assertFalse(paused.pausedAt!!.isBefore(before), "pausedAt is now")
        assertFalse(paused.updatedAt!!.isBefore(before))
        assertNull(paused.operation)
        assertEquals(emptyList<ReplayRange>(), paused.replay)
        assertEquals(paused, control.read(listOf(PROCESSOR, "never-touched"))[PROCESSOR])

        // Pausing leaves a reset lock alone, and a lock leaves the pause alone.
        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-a", Duration.ofSeconds(30)))
        assertTrue(control.get(PROCESSOR)!!.paused)
        control.setPaused(PROCESSOR, false, "control-2")
        val resumed = control.get(PROCESSOR)!!
        assertFalse(resumed.paused)
        assertNull(resumed.pausedAt)
        assertNull(resumed.pausedBy)
        assertEquals("op-a", resumed.operation?.id)

        control.setPaused("audit", false, "local") // a resume creates the document, not paused
        assertEquals(ProcessorControlStore.Control("audit", paused = false, updatedAt = control.get("audit")!!.updatedAt), control.get("audit"))
    }

    @Test
    fun replayRangesRoundTrip() = runBlocking {
        val control = store.processorControl
        val r1 = ReplayRange(TOPIC, "0", "10", "20")
        val r2 = ReplayRange(TOPIC, "1", "0", "5")
        val r3 = ReplayRange("other.topic", "0", "10", "20")
        assertEquals(ProcessorControlStore.Lock.Taken, control.lock(PROCESSOR, "op-a", Duration.ofSeconds(30)))
        control.setReplay(PROCESSOR, listOf(r1, r2, r3))
        assertEquals(listOf(r1, r2, r3), control.get(PROCESSOR)!!.replay, "the ranges, in order")
        assertEquals("op-a", control.get(PROCESSOR)!!.operation?.id, "the lock is untouched")

        assertTrue(control.removeReplay(PROCESSOR, r2))
        assertFalse(control.removeReplay(PROCESSOR, r2), "removed once")
        assertFalse(control.removeReplay(PROCESSOR, ReplayRange(TOPIC, "0", "10", "21")), "exactly that range")
        assertFalse(control.removeReplay("audit", r1), "another processor's document")
        assertEquals(listOf(r1, r3), control.get(PROCESSOR)!!.replay)

        control.setReplay(PROCESSOR, listOf(r2))
        assertEquals(listOf(r2), control.get(PROCESSOR)!!.replay, "setReplay replaces")
        control.setReplay(PROCESSOR, emptyList())
        assertEquals(emptyList<ReplayRange>(), control.get(PROCESSOR)!!.replay)
    }

    @Test
    fun deleteMarkersRemovesOnlyTheRange() = runBlocking {
        val markers = store.processedEvents
        fun stage1(processor: String, topic: String, partition: Int, offset: Long) =
            marker("processor:$processor:$topic:$partition:$offset", processor = processor, topic = topic, partition = partition, offset = offset)
        val inRange = (3L until 6L).map { stage1(PROCESSOR, TOPIC, 0, it) }
        val inSecondRange = listOf(stage1(PROCESSOR, TOPIC, 2, 7))
        // The range is [3, 6); 40 lies in it by its text but not by value (decimal positions are ordered by value).
        val kept = (0L until 3L).map { stage1(PROCESSOR, TOPIC, 0, it) } + stage1(PROCESSOR, TOPIC, 0, 6) + stage1(PROCESSOR, TOPIC, 0, 40) +
            stage1(PROCESSOR, TOPIC, 1, 4) + stage1(PROCESSOR, "other.topic", 0, 4) + stage1("audit", TOPIC, 0, 4) +
            marker("saga:t.Order:evt-4") + marker("ingress:src:key-4")
        (inRange + inSecondRange + kept).forEach { assertTrue(markers.record(it), "recorded ${it.id}") }

        val control = store.processorControl
        assertEquals(0L, control.deleteMarkers(PROCESSOR, emptyList()))
        assertEquals(0L, control.deleteMarkers(PROCESSOR, listOf(ReplayRange(TOPIC, "0", "100", "200"))))
        assertEquals(4L, control.deleteMarkers(PROCESSOR, listOf(ReplayRange(TOPIC, "0", "3", "6"), ReplayRange(TOPIC, "2", "7", "8"))))

        (inRange + inSecondRange).forEach { assertFalse(markers.exists(it.id), "${it.id} is deleted") }
        kept.forEach { assertTrue(markers.exists(it.id), "${it.id} is kept") }
        assertEquals(0L, control.deleteMarkers(PROCESSOR, listOf(ReplayRange(TOPIC, "0", "3", "6"))), "deleted once")
    }

    @Test
    fun deadLetterParkedInsideRolledBackTransactionIsKept() = runBlocking {
        val letters = store.deadLetters
        val parked = letter(PROCESSOR, "k1", partition = 0, offset = 1, parkedAt = now())
        inTransaction(rollback = true) { assertTrue(letters.park(parked)) }
        assertEquals(parked, letters.get(parked.id), "a dead letter commits on its own")
        assertEquals(parked.id, letters.head(PROCESSOR, "k1"))

        val thrown = letter(PROCESSOR, "k2", partition = 0, offset = 2, parkedAt = now())
        assertThrows(HandlerFailed::class.java) {
            inTransaction {
                assertTrue(letters.park(thrown))
                assertTrue(letters.recordFailure(parked.id, DeadLetterError("Boom", "failed again")))
                throw HandlerFailed()
            }
        }
        assertTrue(letters.exists(thrown.id), "parked before the handler failed")
        assertEquals(DeadLetterError("Boom", "failed again"), letters.get(parked.id)!!.error, "a retry's failure is recorded on its own too")
    }

    @Test
    fun processorControlWriteInsideRolledBackTransactionIsKept() = runBlocking {
        val control = store.processorControl
        inTransaction(rollback = true) {
            control.setPaused(PROCESSOR, true, "control-1")
            assertEquals(ProcessorControlStore.Lock.Taken, control.lock("audit", "op-a", Duration.ofSeconds(30)))
            control.setReplay("audit", listOf(ReplayRange(TOPIC, "0", "1", "2")))
        }
        assertTrue(control.get(PROCESSOR)!!.paused, "a pause commits on its own")
        val audit = control.get("audit")!!
        assertEquals("op-a", audit.operation?.id, "a lock commits on its own")
        assertEquals(listOf(ReplayRange(TOPIC, "0", "1", "2")), audit.replay, "a replay range commits on its own")
    }

    // ------------------------------------------------------------------------------------------------------- external calls

    @Test
    fun externalCallIdempotencyReturnsFirstResult() = runBlocking {
        val calls = store.externalCalls
        val now = now()
        val lease = now.plusSeconds(300)
        val first = ExternalCallRecord("print:order-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = lease, owner = "owner-1")
        assertTrue(calls.insert(first))
        assertFalse(calls.insert(first.copy(owner = "owner-2")), "the key is taken")
        assertEquals(first, calls.find(first.id))

        val reply = """{"jobId":"j-1","pages":[1,2]}""".toByteArray()
        assertFalse(calls.succeed(first.id, "owner-2", """{"jobId":"j-2"}""".toByteArray(), now), "only the owner settles it")
        assertTrue(calls.succeed(first.id, "owner-1", reply, now.plusMillis(10)))
        assertFalse(calls.succeed(first.id, "owner-1", """{"jobId":"j-3"}""".toByteArray(), now.plusMillis(20)), "settled once")
        val settled = calls.find(first.id)!!
        assertEquals(ExternalCallStatus.SUCCEEDED, settled.status)
        assertJson(reply, settled.reply!!)
        assertNull(settled.owner)
        assertNull(settled.leaseUntil)
        assertNull(settled.error)
        assertEquals(now.plusMillis(10), settled.updatedAt)
        assertFalse(calls.insert(first.copy(owner = "owner-3")), "a repeat finds the first result")
        assertJson(reply, calls.find(first.id)!!.reply!!)

        // A call whose lease passed: taken over once (by the owner and lease read), never by a stale reader.
        val stale = ExternalCallRecord("print:order-2", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.minusSeconds(1), owner = "owner-1")
        assertTrue(calls.insert(stale))
        val taken = calls.takeOver(stale.id, "owner-1", stale.leaseUntil, "owner-2", lease, now)
        assertNotNull(taken)
        assertEquals(2, taken!!.attempts)
        assertEquals("owner-2", taken.owner)
        assertEquals(lease, taken.leaseUntil)
        assertNull(calls.takeOver(stale.id, "owner-1", stale.leaseUntil, "owner-3", lease, now), "the record changed since it was read")
        assertFalse(calls.giveUp(stale.id, "owner-1", stale.leaseUntil, now), "the record changed since it was read")
        assertFalse(calls.markUnknown(stale.id, "owner-1", now), "owner-1 no longer owns it")
        val error = """{"code":"HANDLER_ERROR","message":"IllegalStateException: out of paper"}""".toByteArray()
        assertTrue(calls.fail(stale.id, "owner-2", error, now))
        val failed = calls.find(stale.id)!!
        assertEquals(ExternalCallStatus.FAILED, failed.status)
        assertJson(error, failed.error!!)
        assertNull(failed.owner)
        assertNull(failed.leaseUntil)

        // Given up on as UNKNOWN, and a retryable failure that resets the key.
        val abandoned = stale.copy(id = "print:order-3")
        assertTrue(calls.insert(abandoned))
        assertTrue(calls.giveUp(abandoned.id, abandoned.owner, abandoned.leaseUntil, now))
        assertEquals(ExternalCallStatus.UNKNOWN, calls.find(abandoned.id)!!.status)
        assertNull(calls.find(abandoned.id)!!.owner)
        val unknown = first.copy(id = "print:order-4")
        assertTrue(calls.insert(unknown))
        assertTrue(calls.markUnknown(unknown.id, "owner-1", now))
        assertEquals(ExternalCallStatus.UNKNOWN, calls.find(unknown.id)!!.status)
        val retryable = first.copy(id = "print:order-5")
        assertTrue(calls.insert(retryable))
        assertFalse(calls.reset(retryable.id, "owner-2"))
        assertTrue(calls.reset(retryable.id, "owner-1"))
        assertNull(calls.find(retryable.id))
        assertTrue(calls.insert(retryable.copy(owner = "owner-2")), "a reset key starts afresh")
    }

    // ---------------------------------------------------------------------------------------------------------------- sagas

    @Test
    fun sagaPortsRoundTrip() {
        val ports = store.sagas
        val t = now()

        // sagas: live instances by association, the live start key unique, optimistic versions
        val saga = SagaDocument("s1", "t.Order", """{"orderId":"o-1"}""", listOf(Association("orderId", "o-1")), "t.Order|orderId=o-1", createdAt = t, updatedAt = t)
        ports.sagas.insert(saga)
        assertEquals(saga, ports.sagas.findById("s1"))
        assertNull(ports.sagas.findById("s0"))
        assertEquals(listOf(saga), ports.sagas.findLive("t.Order", "orderId", "o-1"))
        assertEquals(emptyList<SagaDocument>(), ports.sagas.findLive("t.Order", "orderId", "o-2"))
        assertEquals(emptyList<SagaDocument>(), ports.sagas.findLive("t.Other", "orderId", "o-1"))
        assertThrows(DuplicateKeyException::class.java) { ports.sagas.insert(saga.copy(id = "s2")) }
        val v1 = saga.copy(state = """{"orderId":"o-1","paid":true}""", version = 1, updatedAt = t.plusMillis(1))
        assertTrue(ports.sagas.update(v1, expectedVersion = 0))
        assertFalse(ports.sagas.update(v1.copy(version = 2), expectedVersion = 0), "a stale version")
        assertEquals(v1, ports.sagas.findById("s1"))
        val ended = v1.copy(ended = true, endedAt = t.plusMillis(2), version = 2)
        assertTrue(ports.sagas.update(ended, expectedVersion = 1))
        assertEquals(emptyList<SagaDocument>(), ports.sagas.findLive("t.Order", "orderId", "o-1"), "an ended saga is not live")
        ports.sagas.insert(saga.copy(id = "s2")) // the start key is free once its saga ended
        assertEquals(listOf("s2"), ports.sagas.findLive("t.Order", "orderId", "o-1").map { it.id })
        ports.sagas.insert(SagaDocument("s3", "t.Ship", "{}", listOf(Association("orderId", "o-1")), createdAt = t, updatedAt = t)) // no start key
        ports.sagas.insert(SagaDocument("s4", "t.Ship", "{}", createdAt = t, updatedAt = t)) // forceNew: no start key either
        assertEquals(mapOf("t.Order" to 1L, "t.Ship" to 2L), ports.sagas.countLiveByType(), "live instances per type, the ended one not")

        // dispatch: recorded, and read back in seq order (what the relay reads)
        val charge = SagaDispatchDocument("s1:1000", "s1", 1000, DispatchKind.COMMAND, "t.Charge", """{"orderId":"o-1"}""", mapOf("correlation-id" to "c-1"), "o-1", t)
        val charged = SagaDispatchDocument("s1:1001", "s1", 1001, DispatchKind.EVENT, "t.Charged", """{"orderId":"o-1"}""", createdAt = t)
        ports.dispatch.record(emptyList())
        ports.dispatch.record(listOf(charged, charge))
        assertEquals(charge, ports.dispatch.nextPending("s1"))
        assertNull(ports.dispatch.nextPending("s2"))
        assertEquals(listOf("s1"), ports.dispatch.dueSagas(t, emptyList(), limit = 10))
        assertEquals(2L, ports.dispatch.pending())
        ports.dispatch.markDispatched(charge.id, t)
        assertEquals(charged, ports.dispatch.nextPending("s1"))
        assertEquals(1L, ports.dispatch.pending())
        assertThrows(DuplicateKeyException::class.java) { ports.dispatch.record(listOf(charge)) }

        // deadlines
        val remind = DeadlineDocument("d1", "t.Order", "s2", "remind", """{"n":1}""", "t.Remind", t.plusSeconds(60), "c-1")
        val remindAgain = remind.copy(id = "d2", payload = null, payloadType = null)
        val expire = remind.copy(id = "d3", name = "expire")
        val otherSaga = remind.copy(id = "d4", sagaId = "s9")
        listOf(remind, remindAgain, expire, otherSaga).forEach(ports.deadlines::insert)
        assertEquals(remind, ports.deadlines.findScheduled("d1"))
        assertEquals(remindAgain, ports.deadlines.findScheduled("d2"))
        ports.deadlines.cancel("s9", "d1", t) // not that saga's deadline: no effect
        assertNotNull(ports.deadlines.findScheduled("d1"))
        ports.deadlines.cancel("s2", "d1", t)
        assertNull(ports.deadlines.findScheduled("d1"))
        ports.deadlines.cancelByName("s2", "remind", t)
        assertNull(ports.deadlines.findScheduled("d2"))
        assertNotNull(ports.deadlines.findScheduled("d3"), "another name is kept")
        assertTrue(ports.deadlines.markFired("d3", t))
        assertFalse(ports.deadlines.markFired("d3", t), "fired once")
        assertFalse(ports.deadlines.markFired("d1", t), "a cancelled deadline never fires")
        assertNull(ports.deadlines.findScheduled("d3"))
        assertNotNull(ports.deadlines.findScheduled("d4"))
        ports.deadlines.cancelAll("s9", t)
        assertNull(ports.deadlines.findScheduled("d4"))

        // failures
        val older = SagaFailureDocument("t.Order:e1", "t.Order", "e1", "t.Paid", """{"orderId":"o-1"}""", mapOf("id" to "e1"), "boom", 3, t, t)
        val newer = SagaFailureDocument("t.Ship:e2", "t.Ship", "e2", "t.Paid", """{"orderId":"o-2"}""", emptyMap(), "bang", 3, t, t.plusMillis(5))
        ports.failures.save(newer)
        ports.failures.save(older)
        assertEquals(older, ports.failures.find(older.id))
        assertNull(ports.failures.find("t.Order:e0"))
        assertEquals(listOf(older), ports.failures.list("t.Order"))
        assertEquals(listOf(older, newer), ports.failures.list(null), "the longest waiting first")
        assertTrue(ports.failures.failedAgain(older.id, "boom again", 2, t.plusMillis(10)))
        assertEquals(older.copy(error = "boom again", attempts = 5, lastFailedAt = t.plusMillis(10)), ports.failures.find(older.id))
        assertFalse(ports.failures.failedAgain("t.Order:e0", "boom", 1, t), "never inserts")
        assertNull(ports.failures.find("t.Order:e0"))
        ports.failures.save(older) // replaces
        assertEquals(older, ports.failures.find(older.id))
        assertTrue(ports.failures.delete(older.id))
        assertFalse(ports.failures.delete(older.id))

        // markers
        assertFalse(ports.markers.exists("saga:t.Order:e1"))
        ports.markers.insert("saga:t.Order:e1", "t.Paid", t)
        assertTrue(ports.markers.exists("saga:t.Order:e1"))
        assertThrows(DuplicateKeyException::class.java) { ports.markers.insert("saga:t.Order:e1", "t.Paid", t) }
    }

    @Test
    fun sagaDispatchEntriesAreRelayedInOrderPerSaga() {
        val dispatch = store.sagas.dispatch
        val t = now()
        fun command(saga: String, seq: Long, createdAt: Instant, next: Instant? = null) = SagaDispatchDocument(
            "$saga:$seq", saga, seq, DispatchKind.COMMAND, "t.Charge", """{"n":$seq}""", mapOf("correlation-id" to "c-$saga"), "r-$saga",
            createdAt, nextAttemptAt = next,
        )
        val a0 = command("a", 1000, t.plusMillis(2))
        val a1 = command("a", 1001, t.plusMillis(3))
        val b0 = command("b", 1000, t.plusMillis(1))
        val c0 = command("c", 1000, t, next = t.plusSeconds(60))
        val c1 = command("c", 1001, t)
        dispatch.record(listOf(a1, c1, a0, b0, c0))

        // Due: a saga whose first pending entry is due; oldest work first (by that entry's createdAt).
        assertEquals(listOf("b", "a"), dispatch.dueSagas(t, emptyList(), limit = 10), "c's first entry waits, holding back its second")
        assertEquals(listOf("b"), dispatch.dueSagas(t, emptyList(), limit = 1))
        assertEquals(listOf("a"), dispatch.dueSagas(t, listOf("b"), limit = 10), "the excluded sagas are left out")
        assertEquals(listOf("c", "b", "a"), dispatch.dueSagas(t.plusSeconds(60), emptyList(), limit = 10), "due once its time came")
        assertEquals(c0, dispatch.nextPending("c"), "the first pending entry, due or not")
        assertEquals(5L, dispatch.pending())

        dispatch.markDispatched(b0.id, t)
        assertNull(dispatch.nextPending("b"))
        dispatch.scheduleRetry(a0.id, attempts = 2, nextAttemptAt = t.plusSeconds(30))
        assertEquals(a0.copy(attempts = 2, nextAttemptAt = t.plusSeconds(30)), dispatch.nextPending("a"))
        assertEquals(emptyList<String>(), dispatch.dueSagas(t, emptyList(), limit = 10))
        assertEquals(listOf("a"), dispatch.dueSagas(t.plusSeconds(30), emptyList(), limit = 10))

        // Given up on: never pending again, with a failure notice reserved for the instance that failed it.
        dispatch.markFailed(a0.id, attempts = 3, reason = SagaCommandFailed.Reason.EXHAUSTED, message = "gave up", now = t, noticeNextAt = t.plusSeconds(5))
        assertEquals(a1, dispatch.nextPending("a"), "a failed entry is not pending: the saga's later entries go on")
        assertEquals(3L, dispatch.pending())
        assertEquals(emptyList<String>(), dispatch.claimDueNotices(t, t.plusSeconds(30), limit = 10).map { it.id }, "still reserved")
        val failed = a0.copy(
            attempts = 3, nextAttemptAt = t.plusSeconds(30), failedAt = t, failureReason = SagaCommandFailed.Reason.EXHAUSTED,
            failureMessage = "gave up", noticePending = true, noticeAttempts = 0,
        )
        assertEquals(listOf(failed.copy(noticeNextAt = t.plusSeconds(35))), dispatch.claimDueNotices(t.plusSeconds(5), t.plusSeconds(35), limit = 10))
        assertEquals(emptyList<String>(), dispatch.claimDueNotices(t.plusSeconds(10), t.plusSeconds(40), limit = 10).map { it.id }, "claimed: reserved")
        dispatch.scheduleNoticeRetry(a0.id, noticeAttempts = 1, noticeNextAt = t.plusSeconds(40))

        // A second notice, due earlier: claims go earliest first, up to the limit.
        dispatch.markFailed(c0.id, attempts = 0, reason = SagaCommandFailed.Reason.NO_HANDLER, message = "nobody", now = t, noticeNextAt = t.plusSeconds(20))
        assertEquals(listOf(c0.id), dispatch.claimDueNotices(t.plusSeconds(40), t.plusSeconds(100), limit = 1).map { it.id })
        val retried = dispatch.claimDueNotices(t.plusSeconds(40), t.plusSeconds(100), limit = 10)
        assertEquals(listOf(a0.id to 1), retried.map { it.id to it.noticeAttempts })
        dispatch.clearNotice(a0.id)
        dispatch.clearNotice(c0.id)
        assertEquals(emptyList<String>(), dispatch.claimDueNotices(t.plusSeconds(3600), t.plusSeconds(3700), limit = 10).map { it.id }, "settled")
        assertEquals(a1, dispatch.nextPending("a"), "settling a notice never makes its entry pending again")
    }

    @Test
    fun sagaEventTheOutboxWouldRefuseIsNotRecorded() {
        val dispatch = store.sagas.dispatch
        val t = now()
        val command = SagaDispatchDocument("s1:1000", "s1", 1000, DispatchKind.COMMAND, "t.Charge", """{"n":1}""", createdAt = t)
        for (payload in listOf("[1,2]", "42", "{not json", """{"n":9223372036854775808}""")) {
            val event = SagaDispatchDocument("s1:1001", "s1", 1001, DispatchKind.EVENT, "t.Charged", payload, createdAt = t)
            assertThrows(IllegalArgumentException::class.java, { dispatch.record(listOf(command, event)) }, payload)
            assertNull(dispatch.nextPending("s1"), "nothing of the batch was recorded: $payload")
        }
        assertEquals(0L, dispatch.pending())
    }

    @Test
    fun concurrentLeaseClaimsGrantOneOwner() {
        val dispatch = store.sagas.dispatch
        val t = now()
        val sagas = (0 until 20).map { "s$it" }
        val won = race { owner -> sagas.filter { dispatch.claimLease(it, owner, until = t.plusSeconds(30), now = t) } }
        assertEquals(sagas.toSet(), won.values.flatten().toSet(), "every lease was taken")
        assertEquals(emptySet<String>(), won.getValue("a").toSet() intersect won.getValue("b").toSet(), "a lease taken twice")
    }

    @Test
    fun concurrentDeadlineClaimsReturnEachDeadlineOnce() {
        val deadlines = store.sagas.deadlines
        val t = now()
        val ids = (0 until 40).map { "d%02d".format(it) }
        ids.forEachIndexed { i, id -> deadlines.insert(DeadlineDocument(id, "t.Order", "s-$id", "remind", null, null, t.minusSeconds(60L - i), "c-$id")) }
        val claimed = race { owner -> generateSequence { deadlines.claimDue(owner, t, t.plusSeconds(30)) }.map { it.id }.toList() }
        assertEquals(ids.toSet(), claimed.values.flatten().toSet(), "every due deadline was claimed")
        assertEquals(ids.size, claimed.values.sumOf { it.size }, "a deadline was claimed twice: $claimed")
    }

    @Test
    fun concurrentNoticeClaimsReturnEachNoticeOnce() {
        val dispatch = store.sagas.dispatch
        val t = now()
        val ids = (0 until 40).map { "s$it:1000" }
        dispatch.record(ids.map { SagaDispatchDocument(it, it.substringBefore(':'), 1000, DispatchKind.COMMAND, "t.Charge", "{}", createdAt = t) })
        ids.forEach { dispatch.markFailed(it, 1, SagaCommandFailed.Reason.NO_HANDLER, "nobody", t, noticeNextAt = t) }
        val claimed = race { _ ->
            generateSequence { dispatch.claimDueNotices(t, t.plusSeconds(30), limit = 3).takeIf { it.isNotEmpty() } }.flatten().map { it.id }.toList()
        }
        assertEquals(ids.toSet(), claimed.values.flatten().toSet(), "every due notice was claimed")
        assertEquals(ids.size, claimed.values.sumOf { it.size }, "a notice was claimed twice: $claimed")
    }

    @Test
    fun sagaDispatchLeaseIsExclusiveUntilItExpires() {
        val dispatch = store.sagas.dispatch
        val t = now()
        assertTrue(dispatch.claimLease("s1", "relay-a", until = t.plusSeconds(30), now = t))
        assertFalse(dispatch.claimLease("s1", "relay-b", until = t.plusSeconds(30), now = t.plusSeconds(10)), "held by relay-a")
        assertFalse(dispatch.claimLease("s1", "relay-a", until = t.plusSeconds(30), now = t.plusSeconds(10)), "a held lease is renewed, not claimed")
        assertTrue(dispatch.claimLease("s2", "relay-b", until = t.plusSeconds(30), now = t), "another saga's lease")
        assertTrue(dispatch.renewLease("s1", "relay-a", until = t.plusSeconds(60)))
        assertFalse(dispatch.renewLease("s1", "relay-b", until = t.plusSeconds(60)), "not relay-b's")
        assertFalse(dispatch.claimLease("s1", "relay-b", until = t.plusSeconds(90), now = t.plusSeconds(45)), "renewed until t+60")
        assertTrue(dispatch.claimLease("s1", "relay-b", until = t.plusSeconds(90), now = t.plusSeconds(61)), "expired: taken over")
        assertFalse(dispatch.renewLease("s1", "relay-a", until = t.plusSeconds(120)), "relay-a lost it")
        dispatch.releaseLease("s1", "relay-a") // not relay-a's any more: no effect
        assertFalse(dispatch.claimLease("s1", "relay-c", until = t.plusSeconds(90), now = t.plusSeconds(62)))
        dispatch.releaseLease("s1", "relay-b")
        assertTrue(dispatch.claimLease("s1", "relay-c", until = t.plusSeconds(90), now = t.plusSeconds(62)), "released: free")
    }

    @Test
    fun dueDeadlinesAreClaimedOnceUntilTheirLeaseExpires() {
        val deadlines = store.sagas.deadlines
        val t = now()
        fun deadline(id: String, name: String, dueAt: Instant) = DeadlineDocument(id, "t.Order", "s-$id", name, null, null, dueAt, "c-$id")
        val first = deadline("d1", "remind", t.minusSeconds(20))
        val second = deadline("d2", "remind", t.minusSeconds(10))
        val third = deadline("d3", "expire", t.minusSeconds(5))
        val later = deadline("d4", "expire", t.plusSeconds(60))
        listOf(later, third, second, first).forEach(deadlines::insert)
        deadlines.insert(deadline("d5", "remind", t.minusSeconds(30)))
        deadlines.cancel("s-d5", "d5", t) // not SCHEDULED: never due

        assertEquals(mapOf("remind" to 2L, "expire" to 1L), deadlines.countOverdueByName(t))
        assertEquals(first.copy(claimedBy = "a", claimedUntil = t.plusSeconds(30)), deadlines.claimDue("a", t, t.plusSeconds(30)), "due first")
        assertEquals("d2", deadlines.claimDue("b", t, t.plusSeconds(30))?.id, "d1 is held")
        assertEquals("d3", deadlines.claimDue("a", t, t.plusSeconds(30))?.id)
        assertNull(deadlines.claimDue("b", t, t.plusSeconds(30)), "every due deadline is held, d4 is not due")
        assertTrue(deadlines.markFired("d2", t))
        assertEquals(mapOf("remind" to 1L, "expire" to 1L), deadlines.countOverdueByName(t), "claimed but not fired: still overdue")

        // d1's claimer died: once its lease passed it is claimed again; a fired deadline never is.
        assertEquals(first.copy(claimedBy = "b", claimedUntil = t.plusSeconds(61)), deadlines.claimDue("b", t.plusSeconds(31), t.plusSeconds(61)))
        assertEquals("d3", deadlines.claimDue("b", t.plusSeconds(31), t.plusSeconds(61))?.id)
        assertNull(deadlines.claimDue("b", t.plusSeconds(31), t.plusSeconds(61)))
        assertEquals("d4", deadlines.claimDue("b", t.plusSeconds(60), t.plusSeconds(90))?.id, "due now")
        assertEquals(emptyMap<String, Long>(), deadlines.countOverdueByName(t.minusSeconds(60)))
    }

    // -------------------------------------------------------------------------------------------------------------- helpers

    /**
     * Runs [work] on two threads (owners `a` and `b`) released together by a barrier, each bounded by a timeout so a broken
     * store fails the check instead of hanging it; returns what each returned.
     */
    private fun <T> race(work: (owner: String) -> T): Map<String, T> {
        val start = CyclicBarrier(2)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val futures = listOf("a", "b").associateWith { owner ->
                pool.submit(Callable { start.await(10, TimeUnit.SECONDS); work(owner) })
            }
            return futures.mapValues { it.value.get(30, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    /** Runs [block] in a transaction of [MessageStore.transactions] on this thread: committed, or rolled back when [rollback]. */
    private fun inTransaction(rollback: Boolean = false, block: suspend () -> Unit) {
        TransactionTemplate(store.transactions).executeWithoutResult { status ->
            runBlocking { block() }
            if (rollback) status.setRollbackOnly()
        }
    }

    private class HandlerFailed : RuntimeException("the handler failed")

    private fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    private fun entry(
        id: String, createdAt: Instant, seq: Long, key: String? = "key-$id",
        headers: Map<String, String> = mapOf("correlationId" to "corr-$id"),
    ) = OutboxEntry(id, "t.Happened", key, headers, """{"id":"$id","n":$seq,"nested":{"ok":true}}""".toByteArray(), createdAt, seq)

    private fun assertEntry(expected: OutboxEntry, actual: OutboxEntry) {
        assertEquals(expected.id, actual.id)
        assertEquals(expected.type, actual.type)
        assertEquals(expected.key, actual.key)
        assertEquals(expected.headers, actual.headers)
        assertJson(expected.payload, actual.payload)
        assertEquals(expected.createdAt, actual.createdAt)
        assertEquals(expected.seq, actual.seq)
        assertEquals(expected.attempts, actual.attempts)
    }

    /** JSON compared as a tree: a store may keep it as a document and write it back with other spacing. */
    private fun assertJson(expected: ByteArray, actual: ByteArray) =
        assertEquals(json.readTree(expected), json.readTree(actual), String(actual, Charsets.UTF_8))

    private fun marker(
        id: String, processedAt: Instant = now(), processor: String? = null, topic: String? = null, partition: Int? = null, offset: Long? = null,
    ) = ProcessedMarker(id, "t.Happened", processedAt, processor, topic, partition?.toString(), offset?.toString())

    private fun letter(
        processor: String, key: String?, partition: Int, offset: Long, parkedAt: Instant,
        reason: DeadLetterReason = DeadLetterReason.HANDLER_FAILED, blockedBy: String? = null, replay: Boolean = false,
        payload: String? = """{"offset":$offset}""", payloadBinary: ByteArray? = null,
        headers: Map<String, String> = mapOf("type" to "t.Happened", "id" to "evt-$offset"),
    ) = DeadLetter(
        id = "$processor:$TOPIC:$partition:$offset",
        processor = processor,
        source = TOPIC,
        segment = partition.toString(),
        position = offset.toString(),
        key = key,
        eventId = headers["id"],
        eventType = headers["type"],
        headers = headers,
        payload = payload,
        payloadBinary = payloadBinary,
        reason = reason,
        error = when (reason) {
            DeadLetterReason.SEQUENCED -> DeadLetterError.sequenced(blockedBy!!)
            DeadLetterReason.INVALID -> DeadLetterError.invalid("not a JSON object")
            DeadLetterReason.HANDLER_FAILED -> DeadLetterError("IllegalStateException", "boom at $offset")
        },
        blockedBy = blockedBy,
        attempts = if (reason == DeadLetterReason.HANDLER_FAILED) 3 else 0,
        parkedAt = parkedAt,
        lastAttemptAt = parkedAt,
        expireAt = parkedAt.plus(Duration.ofDays(14)),
        replay = replay,
    )

    private companion object {
        const val PROCESSOR = "billing"
        const val TOPIC = "kraken.events"
        val json: JsonMapper = JsonMapper.builder().build()
    }
}
