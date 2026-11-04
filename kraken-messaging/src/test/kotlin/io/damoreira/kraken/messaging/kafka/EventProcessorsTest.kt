package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.deadletter.DeadLetterStore
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.ProcessingGroup
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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PlainHandlers {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: Happened) { seen += event.id }
}

@ProcessingGroup("orders")
class OrdersHandlers {
    val seen = CopyOnWriteArrayList<String>()
    @Volatile var failing: String? = null
    @Volatile var gate: CountDownLatch? = null
    @EventHandler fun on(event: Happened) {
        gate?.await(30, TimeUnit.SECONDS)
        seen += event.id
        if (event.id == failing) throw IllegalStateException("orders cannot take ${event.id}")
    }
}

/** Named processors against a real Kafka and MongoDB (ADR 0022): groups, markers, independence, and the status they report. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventProcessorsTest {
    private val support = KafkaMongoSupport("event_processors")
    private val mongo = support.mongo
    private lateinit var topic: String
    private lateinit var group: String
    private lateinit var plain: PlainHandlers
    private lateinit var orders: OrdersHandlers
    private val started = mutableListOf<EventProcessors>()

    @AfterAll fun stop() = support.close()

    @BeforeEach fun setUp() {
        mongo.remove(Query(), "processed_events")
        mongo.remove(Query(), DeadLetterStore.COLLECTION)
        topic = support.newTopic("kraken.events.proc", partitions = 2)
        group = "svc-" + UUID.randomUUID()
        plain = PlainHandlers()
        orders = OrdersHandlers()
    }

    @AfterEach fun tearDown() {
        started.forEach { it.stop() }
        started.clear()
    }

    private fun processors(vararg beans: Any, adminServers: String = support.bootstrapServers): EventProcessors {
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { r -> beans.forEach { r.register(it) } }
        val invoker = HandlerInvoker(registry, support.codec)
        val dispatcher = LocalEventDispatcher(registry, invoker, support.tx, mongo)
        val store = DeadLetterStore(mongo, Duration.ofDays(30))
        return EventProcessors(
            dispatcher,
            consumerFor = { processor ->
                EventConsumer(
                    support.consumerFactory(), topic, io.damoreira.kraken.messaging.handler.Processors.groupId(group, processor), registry, invoker,
                    support.tx, mongo, maxAttempts = 2, backOffMs = 50, codec = support.codec, dispatcher = dispatcher,
                    processor = processor, deadLetters = DeadLetterSink.Collection(store),
                )
            },
            adminConfig = mapOf("bootstrap.servers" to adminServers, "request.timeout.ms" to 2000, "default.api.timeout.ms" to 2000),
        ).also { started += it }
    }

    private fun EventProcessors.named(name: String): ProcessorStatus = processorStatuses().single { it.name == name }

    private fun markers() = mongo.findAll(Document::class.java, "processed_events").map { it.getString("_id") }.toSet()

    @Test fun `each processor has its own group and marker family, and both get the event`() {
        val p = processors(plain, orders).also { it.start() }
        assertEquals(listOf("default", "orders"), p.consumers().map { it.processor })
        assertEquals(listOf(group, "$group.orders"), p.consumers().map { it.group })
        support.send(topic, "e-1", key = "k1", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { plain.seen.size == 1 && orders.seen.size == 1 }
        await().atMost(Duration.ofSeconds(10)).until { support.committed(group)[0] == 1L && support.committed("$group.orders")[0] == 1L }
        assertEquals(setOf("e-1", "processor:orders:e-1"), markers())
    }

    @Test fun `stage-1 markers carry the processor and the record's position (P6b)`() {
        val p = processors(plain, orders).also { it.start() }
        support.send(topic, "e-1", key = "k1", partition = 0)
        val offset = support.send(topic, "e-2", key = "k1", partition = 1)
        await().atMost(Duration.ofSeconds(20)).until { plain.seen.size == 2 && orders.seen.size == 2 }
        await().atMost(Duration.ofSeconds(10)).until { mongo.findAll(Document::class.java, "processed_events").size == 4 }
        val marker = mongo.findById("processor:orders:e-2", Document::class.java, "processed_events")!!
        assertEquals("orders", marker["processor"])
        assertEquals(topic, marker["topic"])
        assertEquals(1, marker["partition"], "Int32")
        assertEquals(offset, marker["offset"], "Int64")
        val plainMarker = mongo.findById("e-1", Document::class.java, "processed_events")!!
        assertEquals(listOf("default", topic, 0, 0L), listOf(plainMarker["processor"], plainMarker["topic"], plainMarker["partition"], plainMarker["offset"]))
        assertEquals("test.Happened", plainMarker["type"])
        p.stop()
    }

    @Test fun `a failure in one processor parks there only and does not hold the other`() {
        orders.failing = "bad-1"
        val p = processors(plain, orders).also { it.start() }
        support.send(topic, "bad-1", key = "k1", partition = 0)
        support.send(topic, "ok-1", key = "k1", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { plain.seen.size == 2 && support.committed("$group.orders")[0] == 2L }
        val letters = mongo.findAll(Document::class.java, DeadLetterStore.COLLECTION)
        assertEquals(listOf("orders" to "HANDLER_FAILED", "orders" to "SEQUENCED"), letters.map { it.getString("processor") to it.getString("reason") }.sortedBy { it.second })
        assertEquals(listOf("bad-1", "ok-1"), plain.seen.toList())
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            val o = p.named("orders")
            assertEquals(ProcessorState.PROCESSOR_FAILING, o.state)
            assertEquals(1, o.failedTotal)
            assertEquals(2, o.deadLetters)
            assertEquals("IllegalStateException: orders cannot take bad-1", o.lastError)
            assertTrue(o.lastErrorAtMs > 0)
            val d = p.named("default")
            assertEquals(ProcessorState.PROCESSOR_RUNNING, d.state)
            assertEquals(2, d.processedTotal)
            assertEquals(0, d.deadLetters)
            assertEquals("", d.lastError)
        }
    }

    @Test fun `status reports the assignment, positions, committed offsets, high watermarks and lag`() {
        val first = processors(orders).also { it.start() }
        assertEquals(ProcessorState.PROCESSOR_STARTING, first.named("orders").state, "no partition assigned yet")
        support.send(topic, "a-1", key = "k1", partition = 0)
        support.send(topic, "a-2", key = "k1", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { orders.seen.size == 2 && support.committed("$group.orders")[0] == 2L }
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            val s = first.named("orders")
            assertEquals(ProcessorState.PROCESSOR_RUNNING, s.state)
            assertEquals(listOf(0, 1), s.partitionsList.map { it.partition })
            val p0 = s.partitionsList[0]
            assertEquals(2, p0.position)
            assertEquals(2, p0.committed)
            assertEquals(2, p0.highWatermark)
            assertEquals(0, p0.lag)
            val p1 = s.partitionsList[1]
            assertEquals(-1, p1.position, "nothing acknowledged on partition 1 yet")
            assertEquals(-1, p1.committed)
            assertEquals(0, p1.highWatermark)
            assertEquals(-1, p1.lag, "no committed offset and no position: unknown")
            assertEquals("$group.orders", s.groupId)
            assertEquals(topic, s.topic)
            assertEquals(listOf("test.Happened"), s.eventTypesList)
            assertEquals("collection", s.deadLetterMode)
            assertEquals(2, s.processedTotal)
        }
        first.stop()
        assertEquals(ProcessorState.PROCESSOR_STOPPED, first.named("orders").state)

        // Produced while stopped, then a restarted processor that cannot make progress: the lag is what waits.
        repeat(3) { support.send(topic, "b-$it", key = "k1", partition = 0) }
        orders.gate = CountDownLatch(1)
        val second = processors(orders).also { it.start() }
        try {
            await().atMost(Duration.ofSeconds(20)).untilAsserted {
                val p0 = second.named("orders").partitionsList.single { it.partition == 0 }
                assertEquals(2, p0.committed)
                assertEquals(5, p0.highWatermark)
                assertEquals(3, p0.lag)
                assertEquals(-1, p0.position)
            }
        } finally {
            orders.gate!!.countDown()
        }
    }

    @Test fun `partitions follow a rebalance between two instances`() {
        val one = processors(orders).also { it.start() }
        await().atMost(Duration.ofSeconds(20)).until { one.named("orders").partitionsCount == 2 }
        val two = processors(orders).also { it.start() }
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val a = one.named("orders")
            val b = two.named("orders")
            assertEquals(ProcessorState.PROCESSOR_RUNNING, a.state)
            assertEquals(ProcessorState.PROCESSOR_RUNNING, b.state)
            assertEquals(1, a.partitionsCount)
            assertEquals(1, b.partitionsCount)
            assertEquals(setOf(0, 1), (a.partitionsList + b.partitionsList).map { it.partition }.toSet())
        }
        two.stop()
        await().atMost(Duration.ofSeconds(30)).until { one.named("orders").partitionsCount == 2 }
    }

    @Test fun `only processors with handlers start, and building the status never waits for Kafka`() {
        val p = processors(orders, adminServers = "localhost:1").also { it.start() }
        assertEquals(listOf("orders"), p.consumers().map { it.processor }, "default has no handler and no saga: not started")
        repeat(5) {
            val began = System.nanoTime()
            val s = p.processorStatuses()
            assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofMillis(200), "status took too long")
            assertEquals(listOf("orders"), s.map { it.name })
            Thread.sleep(300)
        }
        assertTrue(processors(adminServers = "localhost:1").also { it.start() }.processorStatuses().isEmpty())
    }
}
