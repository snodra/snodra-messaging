package io.damoreira.kraken.messaging.deadletter

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.ProcessedEventDocument
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

private class SteppingClock(var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC
    override fun withZone(zone: ZoneId?): Clock = this
    override fun instant(): Instant = now.also { now = now.plusSeconds(1) }
}

@ProcessingGroup("orders")
class OrdersHandler(private val mongo: MongoTemplate) {
    val seen = CopyOnWriteArrayList<String>()
    val failing = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
    var duringHandling: (String) -> Unit = {}

    @EventHandler fun on(event: Happened, ctx: MessageContext) {
        seen += event.id
        mongo.insert(Document("_id", "row:${event.id}"), "projection")
        duringHandling(event.id)
        if (event.id in failing) throw IllegalStateException("still broken: ${event.id}")
    }
}

/** The admin API of the dead letters (ADR 0022): every retry result, its transaction, retryAll's order, discard, modes, checks. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadLettersTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private lateinit var store: DeadLetterStore
    private lateinit var handler: OrdersHandler
    private lateinit var deadLetters: DeadLetters
    private lateinit var dispatcher: LocalEventDispatcher
    private val topic = "kraken.events"

    @BeforeAll fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "dead_letters_admin")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        mongo.createCollection("projection")
        mongo.createCollection("processed_events")
        mongo.createCollection(DeadLetterStore.COLLECTION)
    }

    @AfterAll fun stop() = mongoContainer.stop()

    @BeforeEach fun setUp() {
        listOf("projection", "processed_events", DeadLetterStore.COLLECTION).forEach { mongo.remove(Query(), it) }
        store = DeadLetterStore(mongo, Duration.ofDays(30), SteppingClock(Instant.parse("2026-11-04T10:00:00Z")))
        handler = OrdersHandler(mongo)
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handler) }
        dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), tx, mongo)
        deadLetters = DeadLetters(DeadLetterMode.COLLECTION, store, dispatcher)
    }

    private fun record(offset: Long, key: String?, eventId: String?, type: String? = "test.Happened", partition: Int = 0): ConsumerRecord<String, ByteArray> {
        val r = ConsumerRecord<String, ByteArray>(topic, partition, offset, key, codec.encode(Happened(eventId ?: "fallback-$offset", Instant.EPOCH)))
        type?.let { r.headers().add("type", it.toByteArray()) }
        eventId?.let { r.headers().add("id", it.toByteArray()) }
        return r
    }

    private fun failed(offset: Long, key: String?, eventId: String?, processor: String = "orders", type: String = "test.Happened"): String {
        val doc = store.letter(processor, record(offset, key, eventId, type), DeadLetterReason.HANDLER_FAILED, DeadLetterError("IllegalStateException", "boom"), attempts = 5)
        store.park(doc)
        return doc.getString("_id")
    }

    private fun sequenced(offset: Long, key: String, eventId: String, head: String): String {
        val doc = store.sequenced("orders", record(offset, key, eventId), head)
        store.park(doc)
        return doc.getString("_id")
    }

    private fun invalid(offset: Long, key: String?): String {
        val doc = store.letter("orders", record(offset, key, null, type = null), DeadLetterReason.INVALID, DeadLetterError.invalid("record without type header"), 0)
        store.park(doc)
        return doc.getString("_id")
    }

    private fun markers() = mongo.findAll(Document::class.java, "processed_events").map { it.getString("_id") }.toSet()
    private fun rows() = mongo.findAll(Document::class.java, "projection").map { it.getString("_id") }.toSet()

    @Test fun `Retried - stage 1 runs and the letter is deleted with the processor's marker`() {
        val id = failed(1, "k1", "e-1")
        assertEquals(RetryResult.Retried, deadLetters.retry(id))
        assertEquals(listOf("e-1"), handler.seen.toList())
        assertEquals(setOf("processor:orders:e-1"), markers())
        assertNull(store.get(id))
    }

    @Test fun `a letter without an id header retries under the consumer's fallback id`() {
        val id = failed(4, "k1", null)
        assertEquals(RetryResult.Retried, deadLetters.retry(id))
        assertEquals(setOf("processor:orders:$topic-0-4"), markers())
    }

    @Test fun `Duplicate - the marker exists, the handler does not run and the letter is deleted`() {
        mongo.insert(ProcessedEventDocument("processor:orders:e-2", "test.Happened", Instant.now()))
        val id = failed(2, "k1", "e-2")
        assertEquals(RetryResult.Duplicate, deadLetters.retry(id))
        assertEquals(emptyList(), handler.seen.toList())
        assertNull(store.get(id))
    }

    @Test fun `Failed - the letter stays with attempts + 1, the new error and reason HANDLER_FAILED, and nothing commits`() {
        val head = failed(3, "k9", "e-0")
        deadLetters.discard(head)
        val id = sequenced(4, "k9", "e-3", head)
        handler.failing += "e-3"
        val before = store.get(id)!!
        assertEquals(RetryResult.Failed("still broken: e-3"), deadLetters.retry(id))
        val after = store.get(id)!!
        assertEquals(DeadLetterReason.HANDLER_FAILED, after.reason)
        assertEquals(1, after.attempts)
        assertEquals(DeadLetterError("IllegalStateException", "still broken: e-3"), after.error)
        assertNull(after.blockedBy)
        assertTrue(after.lastAttemptAt > before.lastAttemptAt)
        assertEquals(before.expireAt, after.expireAt, "retention counts from parking")
        assertEquals(emptySet(), markers())
        assertEquals(emptySet(), rows(), "the handler's write rolled back")
    }

    @Test fun `a letter removed while it is retried is NotFound and its handling does not commit`() {
        val id = failed(5, "k1", "e-5")
        handler.duringHandling = { store.delete(id) } // another instance discards it while this one handles it
        assertEquals(RetryResult.NotFound, deadLetters.retry(id))
        assertEquals(emptySet(), markers(), "handling without the deletion never commits")
        assertEquals(emptySet(), rows())
    }

    @Test fun `Blocked, NotRetriable, NotFound and a type without a handler`() {
        val head = failed(10, "k1", "e-10")
        val next = sequenced(11, "k1", "e-11", head)
        assertEquals(RetryResult.Blocked(head), deadLetters.retry(next))
        assertEquals(RetryResult.NotRetriable, deadLetters.retry(invalid(12, "k1")))
        assertEquals(RetryResult.NotFound, deadLetters.retry("orders:$topic:0:999"))
        val unknown = failed(13, "k2", "e-13", type = "test.Gone")
        assertEquals(RetryResult.Failed("no handler for test.Gone in processor orders"), deadLetters.retry(unknown))
        assertEquals(1, store.get(unknown)!!.attempts - 5)
        assertEquals(emptyList(), handler.seen.toList())
    }

    @Test fun `discarding a head unblocks the next letter of its key`() {
        val head = failed(20, "k1", "e-20")
        val next = sequenced(21, "k1", "e-21", head)
        assertTrue(deadLetters.discard(head))
        assertFalse(deadLetters.discard(head))
        assertEquals(RetryResult.Retried, deadLetters.retry(next))
        assertEquals(listOf("e-21"), handler.seen.toList())
    }

    @Test fun `retryAll goes per key in offset order, stops a key at its first failure, then keyless letters, skipping INVALID`() {
        val a1 = failed(30, "ka", "a-1")
        sequenced(31, "ka", "a-2", a1)
        sequenced(35, "ka", "a-3", a1)
        val b1 = failed(32, "kb", "b-1")
        sequenced(33, "kb", "b-2", b1)
        failed(34, null, "n-1")
        failed(36, null, "n-2")
        invalid(37, "kb")
        handler.failing += "a-2"
        handler.failing += "n-1"

        assertEquals(RetryAllResult(retried = 4, failed = 2, blocked = 1, skipped = 1), deadLetters.retryAll("orders"))
        assertEquals(listOf("a-1", "a-2", "b-1", "b-2", "n-1", "n-2"), handler.seen.toList())
        assertEquals(
            listOf("orders:$topic:0:31", "orders:$topic:0:34", "orders:$topic:0:35", "orders:$topic:0:37"),
            deadLetters.list("orders").map { it.id }.sorted(),
        )
    }

    @Test fun `list sorts by parkedAt then id, honours the limit, count filters by processor`() {
        val first = failed(40, "k1", "e-40")
        val second = failed(41, "k2", "e-41")
        invalid(42, null)
        assertEquals(listOf(first, second), deadLetters.list("orders", limit = 2).map { it.id })
        assertEquals(3, deadLetters.list(null).size)
        assertEquals(3, deadLetters.count("orders"))
        assertEquals(3, deadLetters.count(null))
        assertEquals("e-40", deadLetters.get(first)!!.eventId)
        assertNull(deadLetters.get("orders:$topic:0:999"))
        assertThrows<IllegalArgumentException> { deadLetters.list("orders", limit = 0) }
    }

    @Test fun `a processor this instance does not run is an error naming it`() {
        val billing = failed(50, "k1", "e-50", processor = "billing")
        for (call in listOf<() -> Any?>(
            { deadLetters.list("billing") }, { deadLetters.count("billing") }, { deadLetters.retryAll("billing") },
            { deadLetters.retry(billing) }, { deadLetters.get(billing) }, { deadLetters.discard(billing) },
        )) {
            val e = assertThrows<IllegalArgumentException> { call() }
            assertTrue(e.message!!.contains("billing"), e.message)
        }
        assertEquals(1, deadLetters.count(null), "no processor: no check")
    }

    @Test fun `in modes topic and log every call is an error naming the mode`() {
        for (mode in listOf(DeadLetterMode.TOPIC, DeadLetterMode.LOG)) {
            val off = DeadLetters(mode, null, dispatcher)
            for (call in listOf<() -> Any?>(
                { off.list(null) }, { off.get("x") }, { off.count(null) }, { off.retry("x") }, { off.retryAll("orders") }, { off.discard("x") },
            )) {
                val e = assertThrows<IllegalStateException> { call() }
                assertTrue(e.message!!.contains(mode.wireName), e.message)
            }
        }
    }
}
