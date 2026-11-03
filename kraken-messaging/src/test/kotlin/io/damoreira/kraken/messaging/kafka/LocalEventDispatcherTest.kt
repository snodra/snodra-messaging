package io.damoreira.kraken.messaging.kafka

import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher.Outcome
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A saga stage that records what it was given and what the ordinary handlers had done by then. */
private class RecordingStage(private val interest: Boolean, private val mongo: MongoTemplate, private val handlers: RecordingHandlers) : SagaStage {
    class Call(val type: String, val payload: ByteArray, val ctx: MessageContext, val markerCommitted: Boolean, val handlersSeen: List<String>)

    val calls = CopyOnWriteArrayList<Call>()

    override fun interested(type: String) = interest && type == "test.Happened"

    override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
        // Outside any transaction, so a marker that is visible here has been committed.
        val committed = mongo.count(Query(Criteria.where("_id").`is`(ctx.messageId)), "processed_events") == 1L
        calls += Call(type, payload, ctx, committed, handlers.seen.toList())
        return true
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalEventDispatcherTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private lateinit var handlers: RecordingHandlers
    private lateinit var registry: HandlerRegistry

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "local_dispatcher_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        mongo.remove(Query(), "processed_events")
        handlers = RecordingHandlers()
        registry = HandlerRegistry(MessageTypeResolver("test")).also { it.register(handlers) }
    }

    private fun dispatcher(stage: SagaStage = SagaStage.NONE, withHandlers: Boolean = true): LocalEventDispatcher {
        val reg = if (withHandlers) registry else HandlerRegistry(MessageTypeResolver("test"))
        return LocalEventDispatcher(reg, HandlerInvoker(reg, codec), tx, mongo, stage)
    }

    private fun payload(id: String) = codec.encode(Happened(id, Instant.now()))

    private fun ctx(id: String, type: String = "test.Happened") =
        MessageContext(id, type, "corr-$id", null, emptySet(), null, mapOf("id" to id, "type" to type))

    private fun markers() = mongo.count(Query(), "processed_events")

    @Test
    fun `a record with no ordinary handler but saga interest reaches the saga stage`() {
        val stage = RecordingStage(true, mongo, handlers)

        val outcome = dispatcher(stage, withHandlers = false).dispatch("e-1", "test.Happened", payload("e-1"), ctx("e-1"))

        assertEquals(Outcome.HANDLED, outcome)
        val call = stage.calls.single()
        assertEquals("test.Happened", call.type)
        assertEquals("e-1", call.ctx.messageId)
        assertEquals("e-1", codec.decodeTree(call.payload)["id"].asString())
        assertEquals(emptyList(), handlers.seen.toList())
        assertEquals(0, markers(), "stage 1 did not run, so it left no marker")
    }

    @Test
    fun `a redelivered record skips stage 1 by its marker and still runs stage 2`() {
        val stage = RecordingStage(true, mongo, handlers)
        val dispatcher = dispatcher(stage)

        assertEquals(Outcome.HANDLED, dispatcher.dispatch("r-1", "test.Happened", payload("r-1"), ctx("r-1")))
        assertEquals(Outcome.HANDLED, dispatcher.dispatch("r-1", "test.Happened", payload("r-1"), ctx("r-1")))

        assertEquals(listOf("r-1"), handlers.seen.toList(), "the ordinary handler ran once")
        assertEquals(2, stage.calls.size, "the saga stage runs again: its own markers decide")
        assertEquals(1, markers())
    }

    @Test
    fun `stage 2 runs after stage 1 committed`() {
        val stage = RecordingStage(true, mongo, handlers)

        dispatcher(stage).dispatch("o-1", "test.Happened", payload("o-1"), ctx("o-1"))

        val call = stage.calls.single()
        assertTrue(call.markerCommitted, "stage 1's marker was committed when stage 2 started")
        assertEquals(listOf("o-1"), call.handlersSeen)
    }

    @Test
    fun `a stage 1 failure still runs stage 2 and is then rethrown (the record is redelivered)`() {
        val stage = RecordingStage(true, mongo, handlers)
        val dispatcher = dispatcher(stage)
        handlers.failAlways.set("poison-1")

        assertFailsWith<IllegalStateException> { dispatcher.dispatch("poison-1", "test.Happened", payload("poison-1"), ctx("poison-1")) }

        assertEquals(1, stage.calls.size, "the sagas must not miss the event because an ordinary handler failed")
        assertEquals(0, markers(), "the marker rolled back with the handler")

        handlers.failAlways.set(null) // fixed: the redelivery runs both stages again (stage 2's own markers make that safe)
        assertEquals(Outcome.HANDLED, dispatcher.dispatch("poison-1", "test.Happened", payload("poison-1"), ctx("poison-1")))
        assertEquals(2, stage.calls.size)
        assertEquals(1, markers())
    }

    @Test
    fun `the outcome is DUPLICATE when stage 1's marker exists and no saga is interested`() {
        val dispatcher = dispatcher(SagaStage.NONE)

        assertEquals(Outcome.HANDLED, dispatcher.dispatch("d-1", "test.Happened", payload("d-1"), ctx("d-1")))
        assertEquals(Outcome.DUPLICATE, dispatcher.dispatch("d-1", "test.Happened", payload("d-1"), ctx("d-1")))

        assertEquals(listOf("d-1"), handlers.seen.toList())
        assertEquals(1, markers())
    }

    @Test
    fun `a type no handler and no saga wants is no interest and leaves no marker`() {
        val dispatcher = dispatcher(RecordingStage(false, mongo, handlers))

        assertFalse(dispatcher.interested("test.Unknown"))
        assertTrue(dispatcher.interested("test.Happened"))
        assertEquals(Outcome.NO_INTEREST, dispatcher.dispatch("u-1", "test.Unknown", payload("u-1"), ctx("u-1", "test.Unknown")))
        assertEquals(0, markers())
        assertFalse(SagaStage.NONE.interested("test.Happened"))
    }
}
