package com.damoreira.snodra.messaging.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.external.ExternalCallStatus
import com.damoreira.snodra.messaging.saga.DeadlineDocument
import com.damoreira.snodra.messaging.store.ExternalCallRecord
import com.damoreira.snodra.messaging.test.SnodraContainers
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * The documents the MongoDB store writes, field by field, where the core's tests no longer read them raw (they run on the
 * in-memory store): existing data and other tools read these fields.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoDocumentShapesTest {
    private val container = SnodraContainers.mongo()
    private lateinit var client: MongoClient
    private lateinit var mongo: MongoTemplate
    private lateinit var store: MongoMessageStore

    @BeforeAll
    fun start() {
        container.start()
        client = MongoClients.create(container.connectionString + "/?directConnection=true")
        mongo = MongoTemplate(SimpleMongoClientDatabaseFactory(client, "shapes_" + UUID.randomUUID().toString().replace("-", "")))
        store = MongoMessageStore(mongo).also { it.ensureIndexes() }
    }

    @AfterAll
    fun stop() {
        client.close()
        container.stop()
    }

    private fun raw(collection: String, id: String): Document = mongo.findById(id, Document::class.java, collection)!!

    @Test
    fun `a deadline keeps the correlation id of the message that scheduled it`() {
        val due = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        store.sagas.deadlines.insert(DeadlineDocument("d1", "t.Order", "s1", "remind", """{"n":1}""", "t.Remind", due, "corr-e-40"))
        val stored = raw("deadlines", "d1")
        assertEquals("corr-e-40", stored.getString("correlationId"))
        assertEquals("SCHEDULED", stored.getString("status"))
        assertEquals(java.util.Date.from(due), stored.getDate("dueAt"))
    }

    @Test
    fun `an empty correlation id is stored as no field, and a deadline without one reads as empty`() {
        val due = Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(1)
        store.sagas.deadlines.insert(DeadlineDocument("d-empty", "t.Order", "s-empty", "remind", null, null, due, ""))
        assertNull(raw("deadlines", "d-empty")["correlationId"], "an empty correlation id is not written")
        assertEquals("", store.sagas.deadlines.findScheduled("d-empty")!!.correlationId)
    }

    @Test
    fun `documents the Rust store writes without its optional fields are read as absent`() {
        val now = java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS))
        val past = java.util.Date.from(Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS))
        val tag = UUID.randomUUID().toString().take(8)
        // deadlines: no payload, payloadType, correlationId, claimedBy, claimedUntil, finishedAt
        mongo.insert(Document("_id", "rd-$tag").append("sagaType", "t.Order").append("sagaId", "rs-$tag").append("name", "remind")
            .append("dueAt", past).append("status", "SCHEDULED"), "deadlines")
        val scheduled = store.sagas.deadlines.findScheduled("rd-$tag")!!
        assertEquals("", scheduled.correlationId)
        assertNull(scheduled.payload)
        val claimed = generateSequence { store.sagas.deadlines.claimDue("w-$tag", Instant.now(), Instant.now().plusSeconds(30)) }
            .first { it.id == "rd-$tag" }
        assertEquals("", claimed.correlationId)
        // sagas: no startKey, endedAt
        mongo.insert(Document("_id", "rs-$tag").append("type", "t.Order").append("state", "{}").append("associations", listOf<Document>())
            .append("ended", false).append("version", 0L).append("createdAt", now).append("updatedAt", now), "sagas")
        val saga = store.sagas.sagas.findById("rs-$tag")!!
        assertNull(saga.startKey)
        assertNull(saga.endedAt)
        // saga_dispatch: no routingKey, dispatchedAt, nextAttemptAt, failedAt, failureReason, failureMessage, notice*
        mongo.insert(Document("_id", "rs-$tag:1000").append("sagaId", "rs-$tag").append("seq", 1000L).append("kind", "COMMAND")
            .append("type", "t.Charge").append("payload", "{}").append("headers", Document()).append("createdAt", now).append("attempts", 0), "saga_dispatch")
        val pending = store.sagas.dispatch.nextPending("rs-$tag")!!
        assertNull(pending.routingKey)
        assertNull(pending.noticePending)
        // dead_letters: no key, eventId, eventType, payload, payloadBinary, blockedBy, replay
        mongo.insert(Document("_id", "p:s:0:$tag").append("processor", "p-$tag").append("topic", "s").append("partition", 0).append("offset", 7L)
            .append("headers", Document()).append("reason", "INVALID").append("error", Document("type", "InvalidMessage").append("message", "x"))
            .append("attempts", 1).append("parkedAt", now).append("lastAttemptAt", now).append("expireAt", now), "dead_letters")
        val letter = runBlocking { store.deadLetters.get("p:s:0:$tag") }!!
        assertNull(letter.key)
        assertNull(letter.payload)
        assertEquals(false, letter.replay)
        assertEquals(1, runBlocking { store.deadLetters.summaries("p-$tag") }.size)
        // outbox: no key, owner, leaseUntil
        mongo.insert(Document("_id", "ro-$tag").append("type", "t.Happened").append("headers", Document()).append("payload", Document("n", 1))
            .append("createdAt", past).append("seq", 1L).append("attempts", 0), "outbox")
        assertEquals(listOf("ro-$tag"), runBlocking { store.outbox.claim("relay-$tag", java.time.Duration.ofSeconds(30), limit = 100) }.map { it.id }.filter { it == "ro-$tag" })
        // external_calls: no reply, error, leaseUntil, owner
        mongo.insert(Document("_id", "rx-$tag").append("status", "UNKNOWN").append("attempts", 1).append("updatedAt", now), "external_calls")
        val call = runBlocking { store.externalCalls.find("rx-$tag") }!!
        assertNull(call.reply)
        assertNull(call.owner)
    }

    @Test
    fun `a relayed payload is compact JSON, numbers exactly as written`() {
        val payloads = listOf(
            """{"a":{"b":[1,2,{"c":"x y, z: w"}],"d":[]},"e":{}}""",
            """{"max":9223372036854775807,"min":-9223372036854775808,"small":7,"neg":-1}""",
            """{"f":1.5,"g":-0.25,"h":0.1,"big":1.0E+10}""",
            "{\"u\":\"\u00e9 \u2603 \\ud83d\\ude00\",\"q\":\"a \\\"quote\\\" and \\\\ backslash\"}",
        )
        // As BSON gives them back: a double in the driver's notation (1.0E+10), a character outside the BMP as its escape.
        val tag = UUID.randomUUID().toString().take(8)
        runBlocking {
            payloads.forEachIndexed { i, p ->
                store.outbox.append(com.damoreira.snodra.messaging.store.OutboxEntry("cj-$tag-$i", "t.Happened", "k-$tag", emptyMap(), p.toByteArray(), Instant.now(), i.toLong()))
            }
            val claimed = store.outbox.claim("relay-cj-$tag", java.time.Duration.ofSeconds(30), limit = 100).filter { it.id.startsWith("cj-$tag-") }.sortedBy { it.id }
            assertEquals(payloads, claimed.map { String(it.payload, Charsets.UTF_8) })
        }
    }

    @Test
    fun `an external call is stored with its status as the enum name and its reply as JSON text, and read back from that layout`() {
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        runBlocking {
            store.externalCalls.insert(ExternalCallRecord("job-1", ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.plusSeconds(300), owner = "call-A"))
            store.externalCalls.succeed("job-1", "call-A", """{"jobId":"j1","pages":2}""".toByteArray(), now)
        }
        val stored = raw("external_calls", "job-1")
        assertEquals("SUCCEEDED", stored.getString("status"))
        assertIs<String>(stored["reply"], "the reply is JSON text")
        assertEquals("""{"jobId":"j1","pages":2}""", stored.getString("reply"))
        assertNull(stored["owner"])
        assertNull(stored["leaseUntil"])

        // Another call (or an older version) writing the same layout is read as such.
        mongo.updateFirst(Query(Criteria.where("_id").`is`("job-1")), Update().set("status", ExternalCallStatus.UNKNOWN.name).unset("reply"), "external_calls")
        val read = runBlocking { store.externalCalls.find("job-1") }!!
        assertEquals(ExternalCallStatus.UNKNOWN, read.status)
        assertNull(read.reply)
    }
}
