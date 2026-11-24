package com.damoreira.snodra.messaging.jdbc

import com.zaxxer.hikari.HikariDataSource
import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetterError
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import com.damoreira.snodra.messaging.external.ExternalCallStatus
import com.damoreira.snodra.messaging.saga.Association
import com.damoreira.snodra.messaging.saga.DeadlineDocument
import com.damoreira.snodra.messaging.saga.DispatchKind
import com.damoreira.snodra.messaging.saga.SagaDispatchDocument
import com.damoreira.snodra.messaging.saga.SagaDocument
import com.damoreira.snodra.messaging.saga.SagaFailureDocument
import com.damoreira.snodra.messaging.store.ExternalCallRecord
import com.damoreira.snodra.messaging.store.OutboxEntry
import com.damoreira.snodra.messaging.store.ProcessedMarker
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.simple.JdbcClient
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * The column contract, the same on both databases: every bounded value is accepted at its bound and refused one character
 * over it (counted in characters, multi-byte ones included) before anything is written; unbounded values go through their
 * hash whole; free text is cut to 16 KiB; ids differing only by trailing spaces are distinct.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JdbcBoundsTest(private val database: TestDatabase) {
    private lateinit var dataSource: HikariDataSource
    private lateinit var store: JdbcMessageStore
    private lateinit var jdbc: JdbcClient

    @BeforeAll
    fun start() {
        dataSource = database.dataSource()
        store = JdbcMessageStore(dataSource, database.dialect).also { it.initSchema() }
        jdbc = JdbcClient.create(dataSource)
    }

    @AfterAll
    fun stop() = dataSource.close()

    private val t = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    /** [n] characters, multi-byte ones (`é`: two bytes of UTF-8; `😀`: four), so a byte count would be over the bound. */
    private fun chars(n: Int, prefix: String = ""): String {
        val sb = StringBuilder(prefix)
        var i = 0
        while (sb.codePointCount(0, sb.length) < n) sb.appendCodePoint(if (i++ % 2 == 0) 'é'.code else 0x1F600)
        return sb.toString()
    }

    private fun refused(field: String, block: suspend () -> Unit) {
        val e = assertThrows(IllegalArgumentException::class.java) { runBlocking { block() } }
        assertTrue(e.message!!.contains(field), e.message)
    }

    @Test
    fun outboxBounds() = runBlocking {
        fun entry(id: String, type: String) = OutboxEntry(id, type, "k", emptyMap(), "{}".toByteArray(), t, 1)
        store.outbox.append(entry(chars(Limits.ID, "bound-"), chars(Limits.NAME)))
        refused("outbox entry's id") { store.outbox.append(entry(chars(Limits.ID + 1, "over-"), "t")) }
        refused("outbox entry's type") { store.outbox.append(entry("over-type", chars(Limits.NAME + 1))) }
        assertEquals(1L, jdbc.sql("SELECT COUNT(*) FROM snodra_outbox WHERE id LIKE 'bound-%'").query(Long::class.javaObjectType).single())
        assertEquals(0L, jdbc.sql("SELECT COUNT(*) FROM snodra_outbox WHERE id LIKE 'over-%'").query(Long::class.javaObjectType).single(), "nothing over a bound was written")
    }

    @Test
    fun markerBoundsAndUnboundedIds() = runBlocking {
        val p = chars(Limits.PROCESSOR)
        val longId = "processor:default:" + chars(10_000)
        assertTrue(store.processedEvents.record(ProcessedMarker(longId, chars(5_000), t, p, chars(Limits.SOURCE), chars(Limits.SEGMENT), chars(Limits.POSITION))))
        assertTrue(store.processedEvents.exists(longId), "a marker id of any length, by its hash")
        assertFalse(store.processedEvents.exists(longId.dropLast(1)))
        refused("marker's processor") { store.processedEvents.record(ProcessedMarker("m1", "t", t, chars(Limits.PROCESSOR + 1))) }
        refused("marker's source") { store.processedEvents.record(ProcessedMarker("m2", "t", t, "p", chars(Limits.SOURCE + 1), "0", "1")) }
        refused("marker's segment") { store.processedEvents.record(ProcessedMarker("m3", "t", t, "p", "s", chars(Limits.SEGMENT + 1), "1")) }
        refused("marker's position") { store.processedEvents.record(ProcessedMarker("m4", "t", t, "p", "s", "0", chars(Limits.POSITION + 1))) }
        store.sagas.markers.insert("saga:t.Order:" + chars(10_000), "t", t)
        assertTrue(store.sagas.markers.exists("saga:t.Order:" + chars(10_000)))
    }

    @Test
    fun deadLetterBoundsAndUnboundedKeys() = runBlocking {
        val p = chars(Limits.PROCESSOR)
        val s = chars(Limits.SOURCE)
        val seg = chars(Limits.SEGMENT)
        val pos = chars(Limits.POSITION)
        val key = chars(10_000)
        val longest = letter("$p:$s:$seg:$pos", p, s, seg, pos, key)
        assertEquals(Limits.DEAD_LETTER_ID, longest.id.codePointCount(0, longest.id.length))
        assertTrue(store.deadLetters.park(longest), "the longest id the bounds build")
        assertEquals(longest.id, store.deadLetters.head(p, key), "a record key of any length, by its hash")
        assertEquals(longest, store.deadLetters.get(longest.id))
        refused("dead letter's processor") { store.deadLetters.park(letter("x:s:0:1", chars(Limits.PROCESSOR + 1), "s", "0", "1", "k")) }
        refused("dead letter's source") { store.deadLetters.park(letter("p:x:0:1", "p", chars(Limits.SOURCE + 1), "0", "1", "k")) }
        refused("dead letter's segment") { store.deadLetters.park(letter("p:s:x:1", "p", "s", chars(Limits.SEGMENT + 1), "1", "k")) }
        refused("dead letter's position") { store.deadLetters.park(letter("p:s:0:x", "p", "s", "0", chars(Limits.POSITION + 1), "k")) }
        val error = "é".repeat(20_000) // 40 000 bytes (an error's message is cut at 2 000 characters already; its type is not)
        assertTrue(store.deadLetters.recordFailure(longest.id, DeadLetterError(error, "boom")))
        val kept = store.deadLetters.get(longest.id)!!.error.type
        assertTrue(kept.toByteArray().size <= Limits.FREE_TEXT_BYTES && error.startsWith(kept) && kept.length == Limits.FREE_TEXT_BYTES / 2, "cut at 16 KiB")
    }

    @Test
    fun processorControlBound() = runBlocking {
        store.processorControl.setPaused(chars(Limits.PROCESSOR), true, "local")
        assertTrue(store.processorControl.get(chars(Limits.PROCESSOR))!!.paused)
        refused("processor's name") { store.processorControl.setPaused(chars(Limits.PROCESSOR + 1), true, "local") }
        refused("processor's name") { store.processorControl.lock(chars(Limits.PROCESSOR + 1), "op", Duration.ofSeconds(5)) }
    }

    @Test
    fun externalCallKeysOfAnyLength() = runBlocking {
        val key = "print:" + chars(10_000)
        val call = ExternalCallRecord(key, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = t, leaseUntil = t.plusSeconds(60), owner = "o")
        assertTrue(store.externalCalls.insert(call))
        assertFalse(store.externalCalls.insert(call), "the key is taken")
        assertEquals(call, store.externalCalls.find(key))
        assertTrue(store.externalCalls.succeed(key, "o", "{}".toByteArray(), t))
    }

    @Test
    fun sagaBoundsAndUnboundedKeys() {
        val ports = store.sagas
        val value = "secret-" + chars(10_000)
        val startKey = "t.Order|orderId=$value"
        val saga = SagaDocument(chars(Limits.ID, "s"), chars(Limits.NAME), "{}", listOf(Association("orderId", value)), startKey, createdAt = t, updatedAt = t)
        ports.sagas.insert(saga)
        assertEquals(listOf(saga), ports.sagas.findLive(saga.type, "orderId", value), "an association of any length, by its hash")
        val duplicate = assertThrows(org.springframework.dao.DuplicateKeyException::class.java) { ports.sagas.insert(saga.copy(id = "other")) }
        assertFalse(duplicate.message!!.contains("secret-") || duplicate.message!!.contains("orderId="), "no start key in the message: ${duplicate.message}")
        assertThrows(IllegalArgumentException::class.java) { ports.sagas.insert(saga.copy(id = chars(Limits.ID + 1))) }
        assertThrows(IllegalArgumentException::class.java) { ports.sagas.insert(saga.copy(id = "s2", type = chars(Limits.NAME + 1))) }

        val entry = SagaDispatchDocument(chars(Limits.ID, "d"), chars(Limits.ID, "s"), 1000, DispatchKind.COMMAND, chars(Limits.NAME), "{}", createdAt = t)
        ports.dispatch.record(listOf(entry))
        assertEquals(entry, ports.dispatch.nextPending(entry.sagaId))
        for (bad in listOf(entry.copy(id = chars(Limits.ID + 1)), entry.copy(id = "d2", sagaId = chars(Limits.ID + 1)), entry.copy(id = "d3", type = chars(Limits.NAME + 1)))) {
            assertThrows(IllegalArgumentException::class.java) { ports.dispatch.record(listOf(bad)) }
        }
        assertThrows(IllegalArgumentException::class.java) { ports.dispatch.claimLease(chars(Limits.ID + 1), "o", t, t) }
        assertTrue(ports.dispatch.claimLease(chars(Limits.ID), "o", t.plusSeconds(5), t))

        val deadline = DeadlineDocument(chars(Limits.ID, "x"), chars(Limits.NAME), chars(Limits.ID, "s"), chars(Limits.NAME), null, null, t, "c")
        ports.deadlines.insert(deadline)
        assertNotNull(ports.deadlines.findScheduled(deadline.id))
        for (bad in listOf(deadline.copy(id = chars(Limits.ID + 1)), deadline.copy(id = "y", sagaId = chars(Limits.ID + 1)),
            deadline.copy(id = "z", sagaType = chars(Limits.NAME + 1)), deadline.copy(id = "w", name = chars(Limits.NAME + 1)))) {
            assertThrows(IllegalArgumentException::class.java) { ports.deadlines.insert(bad) }
        }

        val failure = SagaFailureDocument(chars(Limits.NAME) + ":" + chars(10_000), chars(Limits.NAME), chars(10_000), "t", "{}", emptyMap(), "x".repeat(40_000), 1, t, t)
        ports.failures.save(failure)
        val stored = ports.failures.find(failure.id)!!
        assertEquals(Limits.FREE_TEXT_BYTES, stored.error.length, "the error cut to 16 KiB")
        assertThrows(IllegalArgumentException::class.java) { ports.failures.save(failure.copy(sagaType = chars(Limits.NAME + 1))) }
    }

    @Test
    fun outboxErrorsAreCut() = runBlocking {
        store.outbox.append(OutboxEntry("cut", "t", "k", emptyMap(), "{}".toByteArray(), t, 1))
        store.outbox.markAttemptFailed("cut", "😀".repeat(10_000)) // 40 000 bytes
        val kept = jdbc.sql("SELECT last_error FROM snodra_outbox WHERE id = 'cut'").query(String::class.java).single()
        assertEquals(Limits.FREE_TEXT_BYTES, kept.toByteArray().size)
    }

    /** Ids differing only by trailing spaces are different ids (MySQL's PAD SPACE collations would make them collide). */
    @Test
    fun trailingSpacesMatter() = runBlocking {
        for (id in listOf("e-1", "e-1 ", "e-1  ")) store.outbox.append(OutboxEntry(id, "t", "k", emptyMap(), "{}".toByteArray(), t, 1))
        assertEquals(3L, store.outbox.pending())
        assertTrue(store.processedEvents.record(ProcessedMarker("processor:default:abc", "t", t)))
        assertTrue(store.processedEvents.record(ProcessedMarker("processor:default:abc ", "t", t)), "another marker, not a duplicate")
        store.processorControl.setPaused("audit", true, "local")
        store.processorControl.setPaused("audit ", false, "local")
        assertTrue(store.processorControl.get("audit")!!.paused)
        store.sagas.sagas.insert(SagaDocument("s1", "t.Order", "{}", createdAt = t, updatedAt = t))
        store.sagas.sagas.insert(SagaDocument("s1 ", "t.Order", "{}", createdAt = t, updatedAt = t))
        assertEquals(2L, store.sagas.sagas.countLiveByType()["t.Order"])
    }

    /** U+0000 is refused on both databases (PostgreSQL cannot store it; MySQL would), before anything is written. */
    @Test
    fun theNulCharacterIsRefused() = runBlocking {
        val nul = "a\u0000b"
        fun entry(id: String, headers: Map<String, String> = emptyMap(), payload: String = "{}") =
            OutboxEntry(id, "t", "k", headers, payload.toByteArray(), t, 1)
        refused("U+0000") { store.outbox.append(entry("nul-$nul")) }
        refused("U+0000") { store.outbox.append(entry("nul-1", headers = mapOf("h" to nul))) }
        refused("U+0000") { store.outbox.append(entry("nul-2", payload = """{"s":"a\u0000b"}""")) }
        refused("U+0000") { store.processedEvents.record(ProcessedMarker("processor:default:$nul", "t", t)) }
        assertThrows(IllegalArgumentException::class.java) {
            store.sagas.sagas.insert(SagaDocument("nul-s1", "t.Order", "{\"s\":\"$nul\"}", createdAt = t, updatedAt = t))
        }
        assertThrows(IllegalArgumentException::class.java) {
            store.sagas.sagas.insert(SagaDocument("nul-s2", "t.Order", "{}", listOf(Association("k", nul)), createdAt = t, updatedAt = t))
        }
        assertEquals(0L, jdbc.sql("SELECT COUNT(*) FROM snodra_outbox WHERE id LIKE 'nul-%'").query(Long::class.javaObjectType).single())
        assertEquals(null, store.sagas.sagas.findById("nul-s2"), "the association's refusal rolled its saga back")
    }

    /**
     * A dead letter whose payload is valid UTF-8 holding U+0000 is parked anyway, its bytes in `payload_binary` (the record must
     * not be lost because its text cannot be stored); it reads back with the same bytes.
     */
    @Test
    fun aDeadLetterPayloadHoldingNulIsKeptAsBytes() = runBlocking {
        val payload = "{\"s\":\"a\u0000b\"}"
        val nul = letter("p:s:0:nul", "p", "s", "0", "77", "k-nul").copy(payload = payload, payloadBinary = null)
        assertTrue(store.deadLetters.park(nul), "parked")
        val read = store.deadLetters.get(nul.id) ?: error("the letter is read back")
        assertEquals(payload, String(read.value(), Charsets.UTF_8), "the same bytes")
        assertEquals(null, read.payload, "not as text")
    }

    /** MySQL: a session without a STRICT sql_mode would truncate instead of refusing: the store refuses to start on it. */
    @Test
    fun mysqlNeedsAStrictSqlMode() {
        assumeTrue(database == TestDatabase.MYSQL)
        val loose = TestDatabase.MYSQL.create()
        val ds = loose.dataSource(poolSize = 2, initSql = "SET SESSION sql_mode = ''")
        try {
            val e = assertThrows(IllegalStateException::class.java) { JdbcMessageStore(ds, MySqlDialect).initSchema() }
            assertTrue(e.message!!.contains("strict sql_mode"), e.message)
        } finally {
            ds.close()
            loose.drop()
        }
    }

    private fun letter(id: String, processor: String, source: String, segment: String, position: String, key: String) = DeadLetter(
        id, processor, source, segment, position, key, "e", "t.Happened", emptyMap(), "{}", null,
        DeadLetterReason.HANDLER_FAILED, DeadLetterError("Boom", "boom"), null, 1, t, t, t.plus(Duration.ofDays(1)),
    )
}

class PostgresBoundsTest : JdbcBoundsTest(TestDatabase.POSTGRES)

class MySqlBoundsTest : JdbcBoundsTest(TestDatabase.MYSQL)
