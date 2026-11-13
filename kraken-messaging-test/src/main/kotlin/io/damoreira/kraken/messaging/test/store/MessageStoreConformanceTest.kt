package io.damoreira.kraken.messaging.test.store

import io.damoreira.kraken.messaging.control.ReplayRange
import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.external.ExternalCallStatus
import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DispatchKind
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
 * auto-configuration leaves it. The checks use the wall clock with leases of about a second, never a fake clock.
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
        suspend fun relay(owner: String) {
            val mine = published.getOrPut(owner) { mutableListOf() }
            while (true) {
                val batch = store.outbox.claim(owner, Duration.ofSeconds(60), limit = 7)
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
        assertEquals(emptySet<String>(), a.toSet() intersect b.toSet(), "an entry was published by both relays")
        assertEquals(all.map { it.id }.toSet(), a.toSet() + b.toSet(), "every entry was published")
        assertEquals(0L, store.outbox.pending())
    }

    @Test
    fun expiredLeaseIsReclaimed() = runBlocking {
        val base = now()
        listOf("e1", "e2", "e3").forEachIndexed { i, id -> store.outbox.append(entry(id, createdAt = base, seq = i.toLong())) }
        val lease = Duration.ofSeconds(1)

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
        for (l in listOf(head, waiting5, waiting7, invalid, otherKey, noKey, invalidOnly, otherProcessor)) assertTrue(letters.park(l), "parked ${l.id}")
        assertFalse(letters.park(head.copy(attempts = 9)), "a letter is parked once")

        assertEquals(head.id, letters.head(PROCESSOR, "k1"), "the lowest offset that is not INVALID")
        assertEquals(otherKey.id, letters.head(PROCESSOR, "k2"))
        assertNull(letters.head(PROCESSOR, "k3"))
        assertEquals(otherProcessor.id, letters.head("audit", "k1"), "sequencing is per processor")
        assertEquals(head.id, letters.earlier(PROCESSOR, "k1", 7))
        assertNull(letters.earlier(PROCESSOR, "k1", 3), "the INVALID letter below never blocks")
        assertTrue(letters.exists(waiting5.id))
        assertFalse(letters.exists("$PROCESSOR:$TOPIC:0:99"))

        assertEquals(mapOf(0 to setOf("k1"), 1 to setOf("k2")), letters.keys(PROCESSOR, TOPIC, listOf(0, 1)), "keys of blocking letters by partition, no INVALID-only key")
        assertEquals(mapOf(1 to setOf("k2")), letters.keys(PROCESSOR, TOPIC, listOf(1)))
        assertEquals(emptyMap<Int, Set<String>>(), letters.keys(PROCESSOR, TOPIC, emptyList()))
        assertEquals(emptyMap<Int, Set<String>>(), letters.keys(PROCESSOR, "other.topic", listOf(0, 1)))

        assertEquals(7L, letters.count(PROCESSOR))
        assertEquals(8L, letters.count(null))
        assertEquals(
            listOf(invalid, head, waiting5, waiting7, otherKey, noKey, invalidOnly).map { it.id },
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
            listOf(head, waiting5, waiting7, invalid, otherKey, noKey, invalidOnly).map { DeadLetterStore.Summary(it.id, it.key, it.offset, it.reason, it.parkedAt) }.toSet(),
            letters.summaries(PROCESSOR).toSet(),
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
        assertEquals(5L, letters.count(PROCESSOR))
    }

    // ----------------------------------------------------------------------------------------------------- processor control

    @Test
    fun controlLockIsExclusiveUntilLeaseExpires() = runBlocking {
        val control = store.processorControl
        val lease = Duration.ofSeconds(1)
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

        // dispatch: recorded (the relay reads it)
        ports.dispatch.record(emptyList())
        ports.dispatch.record(
            listOf(
                SagaDispatchDocument("s1:1000", "s1", 1000, DispatchKind.COMMAND, "t.Charge", """{"orderId":"o-1"}""", mapOf("correlationId" to "c-1"), "o-1", t),
                SagaDispatchDocument("s1:1001", "s1", 1001, DispatchKind.EVENT, "t.Charged", """{"orderId":"o-1"}""", createdAt = t),
            ),
        )

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

    // -------------------------------------------------------------------------------------------------------------- helpers

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
    ) = ProcessedMarker(id, "t.Happened", processedAt, processor, topic, partition, offset)

    private fun letter(
        processor: String, key: String?, partition: Int, offset: Long, parkedAt: Instant,
        reason: DeadLetterReason = DeadLetterReason.HANDLER_FAILED, blockedBy: String? = null, replay: Boolean = false,
        payload: String? = """{"offset":$offset}""", payloadBinary: ByteArray? = null,
        headers: Map<String, String> = mapOf("type" to "t.Happened", "id" to "evt-$offset"),
    ) = DeadLetter(
        id = "$processor:$TOPIC:$partition:$offset",
        processor = processor,
        topic = TOPIC,
        partition = partition,
        offset = offset,
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
