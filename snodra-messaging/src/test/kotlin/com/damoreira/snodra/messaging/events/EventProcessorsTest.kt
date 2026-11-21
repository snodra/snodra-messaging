package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.server.v1.ProcessorState
import com.damoreira.snodra.server.v1.ProcessorStatus
import com.damoreira.snodra.messaging.DEAD_LETTERS
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.deadletter.DeadLetterFactory
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.ProcessingGroup
import com.damoreira.snodra.messaging.handler.Processors
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.query.Query
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
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

class CountingHandlers {
    val count = AtomicInteger()
    @EventHandler fun on(event: Happened) { count.incrementAndGet() }
}

/**
 * Named processors against a real Kafka (the event-store add-on) and MongoDB: subscriptions (today's consumer
 * groups), markers, independence, and the status they report.
 */
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
        mongo.remove(Query(), DEAD_LETTERS)
        topic = support.newTopic("snodra.events.proc", partitions = 2)
        group = "svc-" + UUID.randomUUID()
        plain = PlainHandlers()
        orders = OrdersHandlers()
    }

    @AfterEach fun tearDown() {
        started.forEach { it.stop() }
        started.clear()
    }

    private fun processors(vararg beans: Any): EventProcessors {
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { r -> beans.forEach { r.register(it) } }
        val invoker = HandlerInvoker(registry, support.codec)
        val dispatcher = LocalEventDispatcher(registry, invoker, support.store)
        val sink = DeadLetterSink.Collection(support.store.deadLetters, DeadLetterFactory(Duration.ofDays(30)))
        return EventProcessors(
            dispatcher,
            consumerFor = { processor ->
                EventConsumer(
                    support.events, SubscriptionSpec(Processors.subscriptionName(group, processor), topic), dispatcher,
                    maxAttempts = 2, backOffMs = 50, codec = support.codec, processor = processor, deadLetters = sink,
                )
            },
        ).also { started += it }
    }

    private fun EventProcessors.named(name: String): ProcessorStatus = processorStatuses().single { it.name == name }

    private fun markers() = mongo.findAll(Document::class.java, "processed_events").map { it.getString("_id") }.toSet()

    @Test fun `each processor has its own subscription, today's consumer group, and marker family, and both get the event`() {
        val p = processors(plain, orders).also { it.start() }
        assertEquals(listOf("default", "orders"), p.consumers().map { it.processor })
        assertEquals(listOf("$group.default", "$group.orders"), p.consumers().map { it.subscription }, "default included, as deployed")
        support.send(topic, "e-1", key = "k1", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { plain.seen.size == 1 && orders.seen.size == 1 }
        await().atMost(Duration.ofSeconds(10)).until { support.committed("$group.default")[0] == 1L && support.committed("$group.orders")[0] == 1L }
        assertEquals(setOf("processor:default:e-1", "processor:orders:e-1"), markers())
    }

    @Test fun `stage-1 markers carry the processor and the record's position`() {
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
        val plainMarker = mongo.findById("processor:default:e-1", Document::class.java, "processed_events")!!
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
        val letters = mongo.findAll(Document::class.java, DEAD_LETTERS)
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

    @Test fun `status reports the event store, the assignment, positions, committed positions, heads and lag`() {
        val first = processors(orders).also { it.start() }
        assertEquals(ProcessorState.PROCESSOR_STARTING, first.named("orders").state, "no partition assigned yet")
        support.send(topic, "a-1", key = "k1", partition = 0)
        support.send(topic, "a-2", key = "k1", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { orders.seen.size == 2 && support.committed("$group.orders")[0] == 2L }
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            val s = first.named("orders")
            assertEquals(ProcessorState.PROCESSOR_RUNNING, s.state)
            assertEquals(listOf("0", "1"), s.segmentsList.map { it.segment })
            val p0 = s.segmentsList[0]
            assertEquals("2", p0.position)
            assertEquals("2", p0.committed)
            assertEquals("2", p0.head)
            assertEquals(0, p0.lag)
            val p1 = s.segmentsList[1]
            assertEquals("", p1.committed, "nothing committed on segment 1")
            assertEquals("0", p1.head)
            assertEquals("$group.orders", s.subscription)
            assertEquals(topic, s.source)
            assertEquals("kafka", s.eventStore)
            assertEquals(listOf("browse", "lag", "native-dead-letter", "replay"), s.capabilitiesList)
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
                val p0 = second.named("orders").segmentsList.single { it.segment == "0" }
                assertEquals("2", p0.committed)
                assertEquals("5", p0.head)
                assertEquals(3, p0.lag, "the lag shows while the handler is busy")
                assertEquals("3", p0.position, "the record in hand was delivered, not acknowledged")
            }
        } finally {
            orders.gate!!.countDown()
        }
    }

    @Test fun `segments follow a rebalance between two instances`() {
        val one = processors(orders).also { it.start() }
        await().atMost(Duration.ofSeconds(20)).until { one.named("orders").segmentsCount == 2 }
        val two = processors(orders).also { it.start() }
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val a = one.named("orders")
            val b = two.named("orders")
            assertEquals(ProcessorState.PROCESSOR_RUNNING, a.state)
            assertEquals(ProcessorState.PROCESSOR_RUNNING, b.state)
            assertEquals(1, a.segmentsCount)
            assertEquals(1, b.segmentsCount)
            assertEquals(setOf("0", "1"), (a.segmentsList + b.segmentsList).map { it.segment }.toSet())
        }
        two.stop()
        await().atMost(Duration.ofSeconds(30)).until { one.named("orders").segmentsCount == 2 }
    }

    @Test fun `only processors with handlers start, and building the status never waits for the event store`() {
        val p = processors(orders).also { it.start() }
        assertEquals(listOf("orders"), p.consumers().map { it.processor }, "default has no handler and no saga: not started")
        repeat(5) {
            val began = System.nanoTime()
            val s = p.processorStatuses()
            assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofMillis(200), "status took too long")
            assertEquals(listOf("orders"), s.map { it.name })
            Thread.sleep(300)
        }
        assertTrue(processors().also { it.start() }.processorStatuses().isEmpty())
    }

    /**
     * On Kafka the event store keeps today's names: the letter id `<processor>:<topic>:<partition>:<offset>` with an Int32
     * `partition` and an Int64 `offset`, the commit in the consumer group `<service>.<processor>`, the status naming that group.
     */
    @Test fun deadLetterIdsUnchangedOnKafka() {
        orders.failing = "bad-1"
        val p = processors(orders).also { it.start() }
        support.send(topic, "ok-1", key = "k9", partition = 1)
        support.send(topic, "bad-1", key = "k1", partition = 1)
        await().atMost(Duration.ofSeconds(30)).until { support.committed("$group.orders")[1] == 2L }
        val letter = mongo.findById("orders:$topic:1:1", Document::class.java, DEAD_LETTERS)
        assertTrue(letter != null, "the letter id is <processor>:<topic>:<partition>:<offset>")
        assertEquals(1, letter["partition"], "Int32")
        assertEquals(1L, letter["offset"], "Int64")
        assertEquals(topic, letter["topic"])
        assertEquals("$group.orders", p.named("orders").subscription)
    }

    /**
     * Throughput of one processor end to end (subscription, marker transaction in MongoDB, acknowledgement), the shape of the
     * add-on's 10 000 event test; the spring-kafka consumer it replaces read the same 10 000 events at about 575 a second here.
     */
    /** A throughput floor: tagged `throughput`, run alone with `./gradlew throughputTest` (a shared Docker VM made it fail spuriously). */
    @Tag("throughput")
    @Test fun `a processor reads thousands of events end to end without falling behind the old consumer`() {
        val total = 10_000
        KafkaProducer(
            mapOf<String, Any>("bootstrap.servers" to support.bootstrapServers, "linger.ms" to 5),
            StringSerializer(), ByteArraySerializer(),
        ).use { producer ->
            repeat(total) { n ->
                val record = ProducerRecord<String, ByteArray>(
                    topic, "k${n % 50}", support.codec.encode(Happened("t-$n", Instant.now())),
                )
                record.headers().add("type", "test.Happened".toByteArray())
                record.headers().add("id", "t-$n".toByteArray())
                producer.send(record)
            }
            producer.flush()
        }
        val counting = CountingHandlers()
        val counter = counting.count
        processors(counting).also { it.start() }
        await().atMost(Duration.ofSeconds(60)).until { counter.get() > 0 }
        val started = System.nanoTime()
        await().atMost(Duration.ofSeconds(240)).until { counter.get() >= total }
        val seconds = (System.nanoTime() - started) / 1e9
        val rate = total / seconds
        LoggerFactory.getLogger(EventProcessorsTest::class.java)
            .info("THROUGHPUT event-store path: {} events in {} s = {} events/s", total, "%.2f".format(seconds), rate.toLong())
        assertTrue(rate > 200, "at least 200 events/s end to end (the old consumer: about 575), was ${rate.toLong()}")
    }
}
