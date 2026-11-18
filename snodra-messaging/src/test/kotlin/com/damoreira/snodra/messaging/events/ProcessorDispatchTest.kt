package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.mongo.MongoMessageStore
import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.handler.DisallowReplay
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.ProcessingGroup
import com.damoreira.snodra.messaging.events.LocalEventDispatcher.Outcome
import com.damoreira.snodra.messaging.saga.SagaStage
import com.damoreira.snodra.messaging.test.SnodraContainers
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Writes a row per handled event to `projection` (inside the stage-1 transaction) and fails while [failing] is set. */
open class WritingProjection(private val mongo: MongoTemplate, private val name: String) {
    val seen = CopyOnWriteArrayList<String>()
    @Volatile var failing = false

    fun record(event: Happened) {
        seen += event.id
        mongo.insert(Document(mapOf("_id" to "$name:${event.id}")), "projection")
        if (failing) throw IllegalStateException("$name failed")
    }
}

class DefaultWriting(mongo: MongoTemplate) : WritingProjection(mongo, "default") {
    @EventHandler fun on(event: Happened) = record(event)
}

/** A `default` handler that must not run again in a replay (P6b), and one whose whole class must not. */
class NoReplayMethod {
    val seen = CopyOnWriteArrayList<String>()
    @DisallowReplay @EventHandler fun on(event: Happened) { seen += event.id }
}

@DisallowReplay
@ProcessingGroup("orders")
class NoReplayClass {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: Happened) { seen += event.id }
}

@ProcessingGroup("orders")
class OrdersWriting(mongo: MongoTemplate) : WritingProjection(mongo, "orders") {
    @EventHandler fun on(event: Happened) = record(event)
}

private class CountingStage(private val failing: Boolean = false) : SagaStage {
    val calls = CopyOnWriteArrayList<String>()
    override fun interested(type: String) = type == "test.Happened"
    override fun eventTypes() = setOf("test.Happened", "test.SagaOnly")
    override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
        calls += ctx.messageId
        if (failing) throw IllegalStateException("saga store down")
        return true
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcessorDispatchTest {
    private val mongoContainer = SnodraContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private lateinit var plain: DefaultWriting
    private lateinit var orders: OrdersWriting
    private lateinit var registry: HandlerRegistry

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "processor_dispatch_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        mongo.createCollection("projection")
        mongo.createCollection("processed_events")
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        mongo.remove(Query(), "processed_events")
        mongo.remove(Query(), "projection")
        plain = DefaultWriting(mongo)
        orders = OrdersWriting(mongo)
        registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(plain); it.register(orders) }
    }

    private fun dispatcher(stage: SagaStage = SagaStage.NONE) = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), MongoMessageStore(mongo), stage)
    private fun payload(id: String) = codec.encode(Happened(id, Instant.now()))
    private fun ctx(id: String) = MessageContext(id, "test.Happened", id, null, emptySet(), null, mapOf("id" to id))
    private fun markerIds() = mongo.findAll(Document::class.java, "processed_events").map { it.getString("_id") }.toSet()
    private fun rows() = mongo.findAll(Document::class.java, "projection").map { it.getString("_id") }.toSet()

    @Test fun `every processor runs only its handlers under its own marker family, default included`() {
        val d = dispatcher()
        assertEquals(Outcome.HANDLED, d.dispatchIn("orders", "e-1", "test.Happened", payload("e-1"), ctx("e-1")))
        assertEquals(listOf("e-1"), orders.seen.toList())
        assertEquals(emptyList(), plain.seen.toList())
        assertEquals(setOf("processor:orders:e-1"), markerIds())

        assertEquals(Outcome.HANDLED, d.dispatchIn("default", "e-1", "test.Happened", payload("e-1"), ctx("e-1")))
        assertEquals(listOf("e-1"), plain.seen.toList())
        assertEquals(setOf("processor:orders:e-1", "processor:default:e-1"), markerIds())

        // Each processor deduplicates on its own marker only.
        assertEquals(Outcome.DUPLICATE, d.dispatchIn("orders", "e-1", "test.Happened", payload("e-1"), ctx("e-1")))
        assertEquals(Outcome.DUPLICATE, d.dispatchIn("default", "e-1", "test.Happened", payload("e-1"), ctx("e-1")))
        assertEquals(listOf("e-1"), orders.seen.toList())
    }

    @Test fun `a failure in one processor leaves the other one's effects and marker alone`() {
        val d = dispatcher()
        orders.failing = true
        assertFailsWith<IllegalStateException> { d.dispatchIn("orders", "f-1", "test.Happened", payload("f-1"), ctx("f-1")) }
        assertEquals(Outcome.HANDLED, d.dispatchIn("default", "f-1", "test.Happened", payload("f-1"), ctx("f-1")))
        assertEquals(setOf("processor:default:f-1"), markerIds(), "the failed processor's marker rolled back")
        assertEquals(setOf("default:f-1"), rows(), "the failed processor's write rolled back")
    }

    @Test fun `sagas run only in default`() {
        val stage = CountingStage()
        val d = dispatcher(stage)
        assertTrue(d.interested("default", "test.SagaOnly").not(), "the stage says it is not interested in this type")
        assertTrue(d.interested("default", "test.Happened"))
        assertTrue(d.interested("orders", "test.Happened"))
        assertFalse(d.hasStage1("default", "test.Unknown"))
        d.dispatchIn("orders", "s-1", "test.Happened", payload("s-1"), ctx("s-1"))
        assertEquals(emptyList(), stage.calls.toList(), "a named processor has no stage 2")
        d.dispatchIn("default", "s-1", "test.Happened", payload("s-1"), ctx("s-1"))
        assertEquals(listOf("s-1"), stage.calls.toList())
        assertEquals(setOf("test.Happened"), d.eventTypes("orders"))
        assertEquals(setOf("test.Happened", "test.SagaOnly"), d.eventTypes("default"), "default also lists the saga event types")
    }

    @Test fun `a processor that is not interested in a type is not, even when sagas are`() {
        val d = dispatcher(CountingStage())
        assertFalse(d.interested("billing", "test.Happened"))
    }

    @Test fun `the local dispatch of external events reaches every processor with its own marker and the sagas once`() {
        val stage = CountingStage()
        val d = dispatcher(stage)
        assertEquals(Outcome.HANDLED, d.dispatch("printer:k-1", "test.Happened", payload("x-1"), ctx("printer:k-1")))
        assertEquals(setOf("processor:default:printer:k-1", "processor:orders:printer:k-1"), markerIds())
        assertEquals(listOf("x-1"), plain.seen.toList())
        assertEquals(listOf("x-1"), orders.seen.toList())
        assertEquals(listOf("printer:k-1"), stage.calls.toList())

        // One processor failing: the others still commit, the sagas still run, the failure is thrown.
        orders.failing = true
        assertFailsWith<IllegalStateException> { d.dispatch("printer:k-2", "test.Happened", payload("x-2"), ctx("printer:k-2")) }
        assertTrue("processor:default:printer:k-2" in markerIds())
        assertFalse("processor:orders:printer:k-2" in markerIds())
        assertEquals(listOf("printer:k-1", "printer:k-2"), stage.calls.toList())
    }

    @Test fun `a stage-2 failure after a stage 1 that succeeded is told apart from a stage-1 failure`() {
        val d = dispatcher(CountingStage(failing = true))
        val e = assertFailsWith<SagaStageException> { d.dispatchIn("default", "t-1", "test.Happened", payload("t-1"), ctx("t-1")) }
        assertEquals("saga store down", e.cause!!.message)
        assertEquals(setOf("processor:default:t-1"), markerIds(), "stage 1 committed")

        plain.failing = true
        val first = assertFailsWith<IllegalStateException> { d.dispatchIn("default", "t-2", "test.Happened", payload("t-2"), ctx("t-2")) }
        assertEquals("default failed", first.message, "stage 1's failure wins, stage 2's is suppressed in it")
        assertEquals("saga store down", first.suppressed.single().message)
    }

    @Test fun `runStage1 runs the extra work in the marker transaction and rolls everything back when it throws`() {
        val d = dispatcher()
        assertFailsWith<IllegalArgumentException> {
            d.runStage1("orders", "r-1", "test.Happened", payload("r-1"), ctx("r-1")) { throw IllegalArgumentException("gone") }
        }
        assertEquals(emptySet(), markerIds())
        assertEquals(emptySet(), rows())

        assertEquals(LocalEventDispatcher.Stage1.HANDLED, d.runStage1("orders", "r-1", "test.Happened", payload("r-1"), ctx("r-1")) {
            mongo.insert(Document(mapOf("_id" to "extra")), "projection")
        })
        assertEquals(setOf("processor:orders:r-1"), markerIds())
        assertEquals(setOf("orders:r-1", "extra"), rows())
        assertEquals(LocalEventDispatcher.Stage1.DUPLICATE, d.runStage1("orders", "r-1", "test.Happened", payload("r-1"), ctx("r-1")) {})
        assertEquals(LocalEventDispatcher.Stage1.NO_HANDLER, d.runStage1("orders", "r-2", "test.Unknown", payload("r-2"), ctx("r-2")) {})
    }

    private fun marker(id: String) = mongo.findById(id, Document::class.java, "processed_events")!!

    @Test fun `a delivered event's marker carries its position, the local dispatch's markers none (P6b)`() {
        val d = dispatcher(CountingStage())
        d.dispatchIn("orders", "p-1", "test.Happened", payload("p-1"), ctx("p-1"), RecordPosition("snodra.events", "2", "41"))
        val m = marker("processor:orders:p-1")
        assertEquals(listOf<Any?>("orders", "snodra.events", 2, 41L), listOf(m["processor"], m["topic"], m["partition"], m["offset"]), "decimal: Int32 and Int64 as always")
        d.dispatch("printer:k-9", "test.Happened", payload("x-9"), ctx("printer:k-9"))
        for (id in listOf("processor:default:printer:k-9", "processor:orders:printer:k-9")) {
            assertEquals(setOf("_id", "type", "processedAt"), marker(id).keys - "_class", "$id has no position")
        }
        d.runStage1("orders", "r-9", "test.Happened", payload("r-9"), ctx("r-9"), RecordPosition("snodra.events", "0", "7")) {}
        assertEquals(7L, marker("processor:orders:r-9")["offset"], "a dead-letter retry's marker carries the letter's position")
    }

    @Test fun `a replayed record skips the sagas and the DisallowReplay handlers (P6b)`() {
        val method = NoReplayMethod()
        val klass = NoReplayClass()
        registry.register(method)
        registry.register(klass)
        val stage = CountingStage()
        val d = dispatcher(stage)
        val at = RecordPosition("snodra.events", "0", "3")
        assertEquals(Outcome.HANDLED, d.dispatchIn("default", "y-1", "test.Happened", payload("y-1"), ctx("y-1"), at, replay = true))
        assertEquals(listOf("y-1"), plain.seen.toList())
        assertEquals(emptyList(), method.seen.toList(), "@DisallowReplay on the method")
        assertEquals(emptyList(), stage.calls.toList(), "stage 2 never runs in a replay")
        d.dispatchIn("orders", "y-1", "test.Happened", payload("y-1"), ctx("y-1"), at, replay = true)
        assertEquals(listOf("y-1"), orders.seen.toList())
        assertEquals(emptyList(), klass.seen.toList(), "@DisallowReplay on the class")

        d.dispatchIn("default", "y-2", "test.Happened", payload("y-2"), ctx("y-2"), at.copy(position = "4"))
        assertEquals(listOf("y-2"), method.seen.toList(), "outside a replay every handler runs")
        assertEquals(listOf("y-2"), stage.calls.toList())
    }
}
