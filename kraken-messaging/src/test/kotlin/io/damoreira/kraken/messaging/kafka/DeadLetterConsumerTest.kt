package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.DEAD_LETTERS
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.RecordingMessagingMetrics
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterFactory
import io.damoreira.kraken.messaging.store.DeadLetterStore
import kotlinx.coroutines.runBlocking
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.saga.SagaStage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.awaitility.Awaitility.await
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.query.Query
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Records the saga stage's calls; interested in `test.Happened` only. */
private class SeenStage : SagaStage {
    val seen = CopyOnWriteArrayList<String>()
    override fun interested(type: String) = type == "test.Happened"
    override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean { seen += ctx.messageId; return true }
}

/** The MongoDB dead letters, whose parking fails while [down] is set (the store unreachable while parking). */
private class FlakyStore(private val inner: DeadLetterStore) : DeadLetterStore by inner {
    val down = AtomicBoolean(false)
    override suspend fun park(letter: DeadLetter): Boolean {
        if (down.get()) throw IllegalStateException("mongo down")
        return inner.park(letter)
    }
}

/** Dead letters of one event processor against a real Kafka and MongoDB (ADR 0022): each reason, sequencing, parking, modes. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadLetterConsumerTest {
    private val support = KafkaMongoSupport("dead_letter_consumer")
    private val mongo = support.mongo
    private lateinit var topic: String
    private lateinit var group: String
    private lateinit var handlers: RecordingHandlers
    private lateinit var registry: HandlerRegistry
    private var consumer: EventConsumer? = null
    private val store = FlakyStore(support.store.deadLetters)
    private val factory = DeadLetterFactory(Duration.ofDays(30))
    private lateinit var metrics: RecordingMessagingMetrics

    @AfterAll fun stop() = support.close()

    @BeforeEach fun setUp() {
        mongo.remove(Query(), "processed_events")
        mongo.remove(Query(), DEAD_LETTERS)
        store.down.set(false)
        topic = support.newTopic("kraken.events.dl")
        group = "group-" + UUID.randomUUID()
        handlers = RecordingHandlers()
        registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handlers) }
        metrics = RecordingMessagingMetrics()
    }

    @AfterEach fun tearDown() {
        consumer?.stop()
        consumer = null
    }

    private fun start(mode: DeadLetterMode = DeadLetterMode.COLLECTION, maxAttempts: Int = 3, stage: SagaStage = SagaStage.NONE): EventConsumer {
        val invoker = HandlerInvoker(registry, support.codec)
        val sink = when (mode) {
            DeadLetterMode.COLLECTION -> DeadLetterSink.Collection(store, factory)
            DeadLetterMode.TOPIC -> DeadLetterSink.Topic(support.kafka)
            DeadLetterMode.LOG -> DeadLetterSink.Log
        }
        return EventConsumer(
            support.consumerFactory(), topic, group, LocalEventDispatcher(registry, invoker, support.store, stage, metrics), maxAttempts, backOffMs = 50,
            codec = support.codec, deadLetters = sink,
            metrics = metrics,
        ).also { consumer = it; it.start() }
    }

    private fun letters(): List<Document> = mongo.findAll(Document::class.java, DEAD_LETTERS).sortedBy { (it["offset"] as Number).toLong() }
    private fun awaitSeen(vararg ids: String) =
        await().atMost(Duration.ofSeconds(15)).untilAsserted { assertTrue(handlers.seen.containsAll(ids.toList()), "seen=${handlers.seen}") }
    private fun awaitCommitted(offset: Long) =
        await().atMost(Duration.ofSeconds(15)).untilAsserted { assertEquals(offset, support.committed(group)[0]) }

    @Test fun `a record that fails every attempt becomes a HANDLER_FAILED letter and its offset is committed`() {
        handlers.failAlways.set("poison-1")
        start(maxAttempts = 3)
        support.send(topic, "poison-1", key = "k1")
        support.send(topic, "good-1", key = "k2")
        awaitSeen("good-1")
        awaitCommitted(2)
        val letter = letters().single()
        assertEquals("default:$topic:0:0", letter["_id"])
        assertEquals("HANDLER_FAILED", letter["reason"])
        assertEquals(3, letter["attempts"])
        assertEquals(Document(mapOf("type" to "IllegalStateException", "message" to "poison")), letter["error"])
        assertEquals(3, handlers.seen.count { it == "poison-1" })
        assertEquals(listOf(HAPPENED to PARKED, HAPPENED to HANDLED), metrics.outcomes("default"))
        assertEquals(4, metrics.handled.count { it.processor == "default" && it.type == HAPPENED }, "every stage-1 attempt is timed")
    }

    @Test fun `a failing key parks its later records SEQUENCED while other keys flow, and stage 2 still runs for them`() {
        handlers.failAlways.set("p-1")
        val stage = SeenStage()
        start(maxAttempts = 2, stage = stage)
        support.send(topic, "p-1", key = "k1")
        support.send(topic, "p-2", key = "k1")
        support.send(topic, "o-1", key = "k2")
        support.send(topic, "p-3", key = "k1")
        awaitSeen("o-1")
        awaitCommitted(4)
        val l = letters()
        assertEquals(listOf("HANDLER_FAILED", "SEQUENCED", "SEQUENCED"), l.map { it["reason"] })
        assertEquals(listOf(0L, 1L, 3L), l.map { (it["offset"] as Number).toLong() })
        assertEquals("default:$topic:0:0", l[1]["blockedBy"])
        assertEquals(Document(mapOf("type" to "Sequenced", "message" to "an earlier letter for this key is parked: default:$topic:0:0")), l[2]["error"])
        assertEquals(0, l[1]["attempts"])
        assertEquals(listOf("p-1", "p-1", "o-1"), handlers.seen.toList(), "stage 1 never ran for the sequenced records")
        assertTrue(stage.seen.containsAll(listOf("p-1", "p-2", "o-1", "p-3")), "stage 2 saw every record: ${stage.seen}")
        assertEquals(listOf(HAPPENED to PARKED, HAPPENED to PARKED, HAPPENED to HANDLED, HAPPENED to PARKED), metrics.outcomes("default"))
        assertEquals(3, metrics.handled.size, "p-1's two attempts and o-1: a sequenced record runs no stage 1")
    }

    @Test fun `a sequenced record's stage 2 is retried and then logged and skipped like any record's, and the record is parked`() {
        handlers.failAlways.set("p-1")
        val calls = CopyOnWriteArrayList<String>()
        val stage = object : SagaStage {
            override fun interested(type: String) = type == "test.Happened"
            override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
                calls += ctx.messageId
                if (ctx.messageId == "p-2") throw IllegalStateException("saga store down")
                return true
            }
        }
        val c = start(maxAttempts = 2, stage = stage)
        support.send(topic, "p-1", key = "k1")
        support.send(topic, "p-2", key = "k1")
        support.send(topic, "o-1", key = "k2")
        awaitSeen("o-1")
        awaitCommitted(3)
        assertEquals(listOf("HANDLER_FAILED", "SEQUENCED"), letters().map { it["reason"] })
        assertEquals(2, calls.count { it == "p-2" }, "stage 2 had the usual attempts, then the record moved on: $calls")
        val status = c.status()
        assertEquals(2L, status.failedTotal, "p-1's stage 1 and p-2's stage 2 exhausted their attempts")
        assertTrue(status.lastError.contains("saga store down"), status.lastError)
        assertEquals(listOf(HAPPENED to PARKED, HAPPENED to PARKED, HAPPENED to HANDLED), metrics.outcomes("default"))
    }

    @Test fun `a key whose letter was removed elsewhere is handled normally (the confirmation misses)`() {
        handlers.failAlways.set("m-1")
        start(maxAttempts = 1)
        support.send(topic, "m-1", key = "k1")
        await().atMost(Duration.ofSeconds(15)).until { letters().size == 1 }
        mongo.remove(Query(), DEAD_LETTERS) // another instance discarded it
        support.send(topic, "m-2", key = "k1")
        awaitSeen("m-2")
        awaitCommitted(2)
        assertEquals(emptyList(), letters())
    }

    @Test fun `INVALID records are parked without blocking their key`() {
        start()
        support.send(topic, "i-1", type = null, key = "k1")
        support.send(topic, "i-2", key = "k1", payload = "not json".toByteArray())
        support.send(topic, "i-3", key = "k1")
        support.send(topic, "i-4", type = "test.Unknown", key = "k1") // a type nobody here handles: skipped, no letter
        awaitSeen("i-3")
        awaitCommitted(4)
        val l = letters()
        assertEquals(listOf("INVALID", "INVALID"), l.map { it["reason"] })
        assertEquals(Document(mapOf("type" to "InvalidMessage", "message" to "record without type header")), l[0]["error"])
        assertEquals(Document(mapOf("type" to "InvalidMessage", "message" to "payload is not a JSON object")), l[1]["error"])
        assertEquals(0, l[0]["attempts"])
        assertNull(l[0]["eventType"])
        assertEquals(
            listOf(OTHER to PARKED, HAPPENED to PARKED, HAPPENED to HANDLED, OTHER to SKIPPED),
            metrics.outcomes("default"),
            "a missing or foreign type is labelled other",
        )
        assertEquals(1, metrics.handled.size, "an INVALID or skipped record runs no stage 1")
    }

    @Test fun `a record parked before a crash is acknowledged on redelivery without running its handler again`() {
        // The letter of offset 0 exists (parked, then the instance died before the commit): the redelivery must not park twice.
        val r0 = ConsumerRecord<String, ByteArray>(topic, 0, 0L, "k1", ByteArray(0))
        r0.headers().add("type", "test.Happened".toByteArray())
        runBlocking { store.park(factory.letter("default", r0, DeadLetterReason.HANDLER_FAILED, io.damoreira.kraken.messaging.deadletter.DeadLetterError("X", "first"), 3)) }
        start()
        support.send(topic, "r-1", key = "k1")
        support.send(topic, "r-2", key = "k2")
        awaitSeen("r-2")
        awaitCommitted(2)
        assertEquals(listOf("r-2"), handlers.seen.toList())
        assertEquals("first", (letters().single()["error"] as Document)["message"])
        assertEquals(listOf(HAPPENED to DUPLICATE, HAPPENED to HANDLED), metrics.outcomes("default"))
    }

    @Test fun `an invalid record whose letter exists already is counted duplicate, as the Rust library`() {
        // Its INVALID letter exists (parked, then the instance died before the commit); INVALID letters block no key, so
        // the redelivery parks again and the store reports the letter there.
        val r0 = ConsumerRecord<String, ByteArray>(topic, 0, 0L, null, ByteArray(0))
        runBlocking { store.park(factory.letter("default", r0, DeadLetterReason.INVALID, io.damoreira.kraken.messaging.deadletter.DeadLetterError("Invalid", "first"), 0)) }
        start()
        support.send(topic, null, type = null, key = null)
        support.send(topic, "i-2", key = "k2")
        awaitSeen("i-2")
        awaitCommitted(2)
        assertEquals("first", (letters().single()["error"] as Document)["message"])
        assertEquals(listOf(OTHER to DUPLICATE, HAPPENED to HANDLED), metrics.outcomes("default"))
    }

    @Test fun `MongoDB down while parking - the record is not acknowledged and is parked once it is back`() {
        handlers.failAlways.set("d-1")
        store.down.set(true)
        start(maxAttempts = 2)
        support.send(topic, "d-1", key = "k1")
        await().atMost(Duration.ofSeconds(15)).until { handlers.seen.count { it == "d-1" } == 2 }
        Thread.sleep(1000)
        assertEquals(null, support.committed(group)[0], "nothing is committed while the letter does not exist")
        assertEquals(emptyList(), letters())
        assertEquals(2, handlers.seen.count { it == "d-1" }, "the redeliveries park, they do not run the handler again")
        store.down.set(false)
        awaitCommitted(1)
        assertEquals(listOf("HANDLER_FAILED"), letters().map { it["reason"] })
        assertEquals(listOf(HAPPENED to PARKED), metrics.outcomes("default"), "parked once, when the sink took it")
    }

    @Test fun `a stage-2 failure after stage 1 succeeded is logged and skipped, no letter`() {
        val failing = object : SagaStage {
            override fun interested(type: String) = true
            override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean = throw IllegalStateException("saga store down")
        }
        start(maxAttempts = 2, stage = failing)
        support.send(topic, "s-1", key = "k1")
        awaitCommitted(1)
        assertEquals(emptyList(), letters())
        assertEquals(listOf("s-1"), handlers.seen.toList(), "stage 1 ran once: its marker skipped it on the second attempt")
        assertEquals(listOf(HAPPENED to FAILED), metrics.outcomes("default"))
        assertEquals(2, metrics.handled.size, "both stage-1 runs are timed (the second found its marker), the saga stage is not")
    }

    @Test fun `mode topic produces the failed and the INVALID record to the DLT with kraken-dlt headers`() {
        handlers.failAlways.set("t-1")
        support.ensureTopic("$topic.dlt")
        start(mode = DeadLetterMode.TOPIC, maxAttempts = 2)
        support.send(topic, "t-1", key = "k1", extra = mapOf("kraken-dlt-reason" to "stale"))
        support.send(topic, "t-2", key = "k1")
        support.send(topic, null, type = null, key = null)
        awaitSeen("t-2")
        awaitCommitted(3)
        val dlt = support.readAll("$topic.dlt", 2)
        assertEquals(2, dlt.size)
        val failed = dlt[0]
        assertEquals("k1", failed.key())
        fun h(r: ConsumerRecord<String, ByteArray>, name: String) = r.headers().headers(name).map { String(it.value()) }
        assertEquals(listOf("default"), h(failed, "kraken-dlt-processor"))
        assertEquals(listOf("HANDLER_FAILED"), h(failed, "kraken-dlt-reason"), "an original kraken-dlt-* header is replaced")
        assertEquals(listOf("IllegalStateException: poison"), h(failed, "kraken-dlt-error"))
        assertEquals(listOf(topic), h(failed, "kraken-dlt-original-topic"))
        assertEquals(listOf("0"), h(failed, "kraken-dlt-original-partition"))
        assertEquals(listOf("0"), h(failed, "kraken-dlt-original-offset"))
        assertEquals(listOf("t-1"), h(failed, "id"))
        assertNull(dlt[1].key())
        assertEquals(listOf("INVALID"), h(dlt[1], "kraken-dlt-reason"))
        assertEquals(listOf("2"), h(dlt[1], "kraken-dlt-original-offset"))
        assertEquals(emptyList(), letters(), "mode topic writes no letter")
        assertEquals(listOf("t-1", "t-1", "t-2"), handlers.seen.toList(), "not sequenced")
        assertEquals(listOf(HAPPENED to PARKED, HAPPENED to HANDLED, OTHER to PARKED), metrics.outcomes("default"))
    }

    @Test fun `mode log logs and skips, nothing is parked`() {
        handlers.failAlways.set("g-1")
        start(mode = DeadLetterMode.LOG, maxAttempts = 2)
        support.send(topic, "g-1", key = "k1")
        support.send(topic, "g-2", key = "k1")
        support.send(topic, "g-3", type = null, key = "k1")
        awaitSeen("g-2")
        awaitCommitted(3)
        assertEquals(emptyList(), letters())
        assertEquals(listOf(HAPPENED to FAILED, HAPPENED to HANDLED, OTHER to FAILED), metrics.outcomes("default"))
    }

    private companion object {
        const val HAPPENED = "test.Happened"
        const val OTHER = MessagingMetrics.OTHER
        val HANDLED = MessagingMetrics.Consumed.HANDLED
        val DUPLICATE = MessagingMetrics.Consumed.DUPLICATE
        val SKIPPED = MessagingMetrics.Consumed.SKIPPED
        val FAILED = MessagingMetrics.Consumed.FAILED
        val PARKED = MessagingMetrics.Consumed.PARKED
    }
}
