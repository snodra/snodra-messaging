package io.damoreira.kraken.messaging.control

import io.damoreira.kraken.hub.v1.ControlRefusal
import io.damoreira.kraken.hub.v1.ControlReply
import io.damoreira.kraken.hub.v1.ControlRequest
import io.damoreira.kraken.hub.v1.DeadLetterDiscard
import io.damoreira.kraken.hub.v1.DeadLetterList
import io.damoreira.kraken.hub.v1.DeadLetterRetry
import io.damoreira.kraken.hub.v1.DeadLetterRetryAll
import io.damoreira.kraken.hub.v1.ProcessorPause
import io.damoreira.kraken.hub.v1.ProcessorResume
import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.hub.v1.ResetExecute
import io.damoreira.kraken.hub.v1.ResetFinish
import io.damoreira.kraken.hub.v1.ResetOutcome
import io.damoreira.kraken.hub.v1.ResetPrepare
import io.damoreira.kraken.hub.v1.SegmentPosition
import io.damoreira.kraken.hub.v1.SegmentPositions
import io.damoreira.kraken.messaging.DEAD_LETTERS
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.PROCESSOR_CONTROL
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.deadletter.DeadLetterFactory
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.events.DeadLetterSink
import io.damoreira.kraken.messaging.events.EventConsumer
import io.damoreira.kraken.messaging.events.EventProcessors
import io.damoreira.kraken.messaging.events.KafkaMongoSupport
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.DisallowReplay
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import io.damoreira.kraken.messaging.handler.Processors
import io.damoreira.kraken.messaging.handler.ResetHandler
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.bson.Document
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.query.Query
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import io.damoreira.kraken.hub.v1.ResetTarget as WireTarget

class ReplayedProjection {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: Happened) { seen += event.id }
}

class OnceOnly {
    val seen = CopyOnWriteArrayList<String>()
    @DisallowReplay @EventHandler fun on(event: Happened) { seen += event.id }
}

class DefaultResets {
    val contexts = CopyOnWriteArrayList<ResetContext>()
    @Volatile var failing = false
    @Volatile var gate: CountDownLatch? = null
    /** Runs inside the reset handler, before it records the context. */
    @Volatile var during: (() -> Unit)? = null
    @ResetHandler fun reset(ctx: ResetContext) {
        gate?.await(30, TimeUnit.SECONDS)
        if (failing) throw IllegalStateException("cannot clear the projection")
        during?.invoke()
        contexts += ctx
    }
}

class FlakyProjection {
    val seen = CopyOnWriteArrayList<String>()
    @Volatile var failing: String? = null
    @EventHandler fun on(event: Happened) {
        seen += event.id
        if (event.id == failing) throw IllegalStateException("the projection cannot take ${event.id}")
    }
}

@ProcessingGroup("audit")
class AuditTrail {
    val seen = CopyOnWriteArrayList<String>()
    @Volatile var resets = 0
    @Volatile var failing: String? = null
    @EventHandler fun on(event: Happened) {
        if (event.id == failing) throw IllegalStateException("audit cannot take ${event.id}")
        seen += event.id
    }
    @ResetHandler fun clear() { resets++ }
}

class CountingSagas : SagaStage {
    val calls = CopyOnWriteArrayList<String>()
    override fun interested(type: String) = type == "test.Happened"
    override fun eventTypes() = setOf("test.Happened")
    override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
        calls += ctx.messageId
        return true
    }
}

/**
 * The control documents with their reads held on request: a poll that read the documents waits for a permit before it
 * applies them, so a request lands between the poll's read and its apply.
 */
class HeldReads(private val store: ProcessorControlStore) : ProcessorControlStore by store {
    @Volatile var holding = false
    /** A permit per held read that has its documents. */
    val read = Semaphore(0)
    private val go = Semaphore(0)

    override suspend fun read(processors: Collection<String>): Map<String, ProcessorControlStore.Control> {
        val docs = store.read(processors)
        if (holding) {
            read.release()
            go.acquire()
        }
        return docs
    }

    /** Lets the held read go on to its apply. */
    fun proceed() = go.release()

    /** Runs [change] between a poll's read and its apply; returns once that poll applied what it read (the next one waits). */
    fun betweenReadAndApply(change: () -> Unit) {
        holding = true
        check(read.tryAcquire(10, TimeUnit.SECONDS)) { "no poll read the documents" }
        change()
        proceed()
        check(read.tryAcquire(10, TimeUnit.SECONDS)) { "the poll did not come back: it never finished applying" }
    }

    /** Stops holding reads (the waiting one goes on). */
    fun release() {
        holding = false
        proceed()
    }
}

/**
 * Processor control against a real Kafka and MongoDB (P6b, ADR 0023): pause and resume, the leased reset (prepare, execute,
 * finish, expiry), replay ranges, reset handlers and the dead letters, driven by the control requests the hub sends.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcessorControlTest {
    private val support = KafkaMongoSupport("processor_control")
    private val mongo = support.mongo
    private lateinit var topic: String
    private lateinit var group: String
    /** The subscription (the consumer group) of processor `default`. */
    private val defaultGroup: String get() = Processors.subscriptionName(group, Processors.DEFAULT)
    private lateinit var projection: ReplayedProjection
    private lateinit var once: OnceOnly
    private lateinit var resets: DefaultResets
    private lateinit var audit: AuditTrail
    private lateinit var sagas: CountingSagas
    private val instances = mutableListOf<Instance>()
    private val lease = 30_000L

    @AfterAll fun stop() = support.close()

    @BeforeEach fun setUp() {
        for (c in listOf("processed_events", DEAD_LETTERS, PROCESSOR_CONTROL)) mongo.remove(Query(), c)
        group = "svc-" + UUID.randomUUID()
        projection = ReplayedProjection()
        once = OnceOnly()
        resets = DefaultResets()
        audit = AuditTrail()
        sagas = CountingSagas()
    }

    @AfterEach fun tearDown() {
        instances.forEach { runCatching { it.processors.stop() } }
        instances.clear()
    }

    /** One instance of the service: its processors and their control, over the shared handler beans. */
    private inner class Instance(
        beans: List<Any>, controlled: Boolean = true, executeLeaseLimit: Duration = Duration.ofSeconds(120),
        val events: EventStore = support.events,
        controlStore: ProcessorControlStore = support.store.processorControl,
    ) {
        val registry = HandlerRegistry(MessageTypeResolver("test")).also { r -> beans.forEach { r.register(it) } }
        val invoker = HandlerInvoker(registry, support.codec)
        val dispatcher = LocalEventDispatcher(registry, invoker, support.store, sagas)
        val deadLetters = DeadLetters(DeadLetterMode.COLLECTION, support.store.deadLetters, dispatcher)
        val control = if (controlled) {
            ProcessorControl(
                controlStore, registry, deadLetters, events, pollInterval = Duration.ofMillis(300), executeLeaseLimit = executeLeaseLimit,
            )
        } else {
            null
        }
        val processors = EventProcessors(
            dispatcher,
            consumerFor = { processor ->
                EventConsumer(
                    events, SubscriptionSpec(Processors.subscriptionName(group, processor), topic), dispatcher,
                    maxAttempts = 2, backOffMs = 50, codec = support.codec, processor = processor,
                    deadLetters = DeadLetterSink.Collection(support.store.deadLetters, DeadLetterFactory(Duration.ofDays(30))),
                )
            },
            control = control,
        )

        fun status(processor: String = "default"): ProcessorStatus = processors.processorStatuses().single { it.name == processor }

        fun ask(request: ControlRequest, timeout: Duration = Duration.ofSeconds(60)): ControlReply {
            val answer = CompletableFuture<ControlReply>()
            control!!.handle(request) { answer.complete(it) }
            return answer.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    private fun instance(
        vararg beans: Any = arrayOf(projection, once, resets, audit), controlled: Boolean = true, executeLeaseLimit: Duration = Duration.ofSeconds(120),
        events: EventStore = support.events, controlStore: ProcessorControlStore = support.store.processorControl,
    ) = Instance(beans.toList(), controlled, executeLeaseLimit, events, controlStore).also { instances += it; it.processors.start() }

    private fun request(processor: String, build: ControlRequest.Builder.() -> Unit): ControlRequest =
        ControlRequest.newBuilder().setControlId(UUID.randomUUID().toString()).setProcessor(processor).apply(build).build()

    private fun pause(processor: String = "default") = request(processor) { pause = ProcessorPause.getDefaultInstance() }
    private fun resume(processor: String = "default") = request(processor) { resume = ProcessorResume.getDefaultInstance() }
    private fun prepare(op: String, processor: String = "default", leaseMs: Long = lease) =
        request(processor) { resetPrepare = ResetPrepare.newBuilder().setOperationId(op).setLeaseMs(leaseMs).build() }
    private fun execute(op: String, target: WireTarget, processor: String = "default") =
        request(processor) { resetExecute = ResetExecute.newBuilder().setOperationId(op).setTarget(target).build() }
    private fun finish(op: String, processor: String = "default", aborted: Boolean = false) =
        request(processor) { resetFinish = ResetFinish.newBuilder().setOperationId(op).setAborted(aborted).build() }

    private val earliest: WireTarget = WireTarget.newBuilder().setEarliest(true).build()
    private val latest: WireTarget = WireTarget.newBuilder().setLatest(true).build()
    private fun at(ms: Long): WireTarget = WireTarget.newBuilder().setTimestampMs(ms).build()
    private fun offsets(vararg o: Pair<Int, Long>): WireTarget = WireTarget.newBuilder().setPositions(
        SegmentPositions.newBuilder().addAllPositions(o.map { SegmentPosition.newBuilder().setSegment(it.first.toString()).setPosition(it.second.toString()).build() }),
    ).build()

    private fun ResetOutcome.triples() = rangesList.map { Triple(it.segment.toInt(), it.from.toLong(), it.until.toLong()) }

    private fun members(group: String = defaultGroup): Int = support.admin().use {
        try {
            it.describeConsumerGroups(listOf(group)).all().get()[group]!!.members().size
        } catch (e: java.util.concurrent.ExecutionException) {
            if (e.cause !is org.apache.kafka.common.errors.GroupIdNotFoundException) throw e
            0
        }
    }

    private fun markers(): Set<String> = mongo.findAll(Document::class.java, "processed_events").map { it.getString("_id") }.toSet()

    private fun control(processor: String = "default") = runBlocking { support.store.processorControl.get(processor) }

    private fun ControlReply.refusal(): ControlRefusal? = if (hasRefused()) refused.code else null

    private fun awaitRunning(i: Instance, processor: String = "default") =
        await().atMost(Duration.ofSeconds(30)).until { i.status(processor).state == ProcessorState.PROCESSOR_RUNNING }

    @Test fun `pause holds the records, survives a restart and reaches a new instance, resume lets them flow`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection)
        support.send(topic, "e-1")
        await().atMost(Duration.ofSeconds(20)).until { "e-1" in projection.seen }
        val paused = pause()
        assertTrue(a.ask(paused).hasOk())
        assertEquals(ProcessorState.PROCESSOR_PAUSED, a.status().state)
        assertEquals(1, members(), "a pause keeps the group membership")
        val doc = control()!!
        assertTrue(doc.paused)
        assertEquals(paused.controlId, doc.pausedBy)
        support.send(topic, "e-2")
        Thread.sleep(2500)
        assertFalse("e-2" in projection.seen, "held while paused")

        a.processors.stop()
        val b = instance(projection)
        await().atMost(Duration.ofSeconds(30)).until { b.status().segmentsCount == 1 }
        assertEquals(ProcessorState.PROCESSOR_PAUSED, b.status().state)
        Thread.sleep(2000)
        assertFalse("e-2" in projection.seen, "a restarted instance starts paused")

        assertTrue(b.ask(resume()).hasOk())
        await().atMost(Duration.ofSeconds(20)).until { "e-2" in projection.seen }
        assertFalse(control()!!.paused)
        awaitRunning(b)
        assertEquals(ControlRefusal.CONTROL_REFUSAL_UNKNOWN_PROCESSOR, b.ask(pause("billing")).refusal())
    }

    @Test fun `a pause written through one instance reaches the others within a poll`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 2)
        val a = instance(projection)
        val b = instance(projection)
        await().atMost(Duration.ofSeconds(30)).until { a.status().segmentsCount + b.status().segmentsCount == 2 }
        assertTrue(a.ask(pause()).hasOk())
        await().atMost(Duration.ofSeconds(5)).until { b.status().state == ProcessorState.PROCESSOR_PAUSED }
        support.send(topic, "p-0", partition = 0)
        support.send(topic, "p-1", partition = 1)
        Thread.sleep(2000)
        assertEquals(emptyList(), projection.seen.toList())
        b.control!!.resume("default") // the local API, on the other instance
        assertEquals(null, control()!!.pausedBy)
        await().atMost(Duration.ofSeconds(20)).until { projection.seen.toSet() == setOf("p-0", "p-1") }
        a.control!!.pause("default")
        assertEquals("local", control()!!.pausedBy)
    }

    @Test fun `prepare leaves the group, a second operation is locked out, and an unrenewed lease resumes the instance`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection)
        awaitRunning(a)
        assertTrue(a.ask(prepare("op-1", leaseMs = 4000)).hasOk())
        assertEquals(0, members(), "left the group before answering")
        assertEquals(ProcessorState.PROCESSOR_STOPPED, a.status().state)
        assertEquals("op-1", a.status().operationId)
        val locked = a.ask(prepare("op-2"))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_LOCKED, locked.refusal())
        assertTrue("op-1" in locked.refused.message, locked.refused.message)

        val began = System.nanoTime()
        assertTrue(a.ask(prepare("op-1", leaseMs = 4000)).hasOk(), "a renewal")
        assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofSeconds(2))
        support.send(topic, "l-1")
        Thread.sleep(1500)
        assertFalse("l-1" in projection.seen)

        await().atMost(Duration.ofSeconds(30)).until { a.status().state == ProcessorState.PROCESSOR_RUNNING }
        assertEquals("", a.status().operationId)
        assertNull(control()!!.operation, "the expired lease was cleared")
        await().atMost(Duration.ofSeconds(20)).until { "l-1" in projection.seen }
    }

    @Test fun `an instance that starts under a held lock stays out of the group until the lock clears`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        runBlocking { support.store.processorControl.lock("default", "op-x", Duration.ofSeconds(4)) }
        val a = instance(projection)
        assertEquals(ProcessorState.PROCESSOR_STOPPED, a.status().state)
        assertEquals("op-x", a.status().operationId)
        support.send(topic, "h-1")
        Thread.sleep(1500)
        assertEquals(0, members())
        assertFalse("h-1" in projection.seen)
        await().atMost(Duration.ofSeconds(30)).until { "h-1" in projection.seen }
        assertEquals("", a.status().operationId)
    }

    @Test fun `a reset of a group that never committed makes no range and commits the target`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        repeat(2) { support.send(topic, "n-$it") }
        runBlocking { support.store.processorControl.lock("default", "op-1", Duration.ofSeconds(30)) } // before any instance joined the group
        val a = instance(projection, resets)
        assertTrue(a.ask(prepare("op-1")).hasOk())
        val reply = a.ask(execute("op-1", latest))
        assertTrue(reply.hasReset(), reply.toString())
        assertEquals(0, reply.reset.rangesCount)
        assertEquals(2L, support.committed(defaultGroup)[0])
        assertTrue(a.ask(finish("op-1")).hasOk())
        awaitRunning(a)
        Thread.sleep(1000)
        assertEquals(emptyList(), projection.seen.toList(), "moved to the end: nothing consumed")
    }

    @Test fun `finish is idempotent and never releases another operation's lock`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection)
        awaitRunning(a)
        assertTrue(a.ask(finish("op-0")).hasOk(), "nothing held")
        assertTrue(a.ask(prepare("op-1")).hasOk())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, a.ask(finish("op-2", aborted = true)).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, a.ask(execute("op-2", earliest)).refusal())
        assertEquals("op-1", control()!!.operation!!.id)
        assertEquals(ProcessorState.PROCESSOR_STOPPED, a.status().state)
        assertTrue(a.ask(finish("op-1")).hasOk())
        assertNull(control()!!.operation)
        awaitRunning(a)
        assertTrue(a.ask(finish("op-1")).hasOk(), "again: nothing held")
    }

    @Test fun `execute while a consumer of the group is active is SUBSCRIBERS_ACTIVE and changes nothing`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        val old = instance(projection, controlled = false) // an instance without processor control keeps consuming
        repeat(3) { support.send(topic, "g-$it") }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 3 && support.committed(defaultGroup)[0] == 3L }
        val before = markers()
        assertTrue(a.ask(prepare("op-1")).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { members() == 1 }
        val reply = a.ask(execute("op-1", earliest))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE, reply.refusal())
        assertTrue("still has 1 subscriber(s)" in reply.refused.message, reply.refused.message)
        assertEquals(3L, support.committed(defaultGroup)[0])
        assertEquals(before, markers())
        assertEquals(emptyList(), control()!!.replay)
        assertEquals(emptyList(), resets.contexts.toList())
        assertTrue(a.ask(finish("op-1", aborted = true)).hasOk())
        old.processors.stop()
    }

    @Test fun `a reset to earliest with two instances replays the range once, without sagas and DisallowReplay handlers, then ends`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 2)
        val a = instance()
        val b = instance()
        for (p in 0..1) repeat(3) { support.send(topic, "r-$p-$it", partition = p) }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 6 && audit.seen.size == 6 && sagas.calls.size == 6 }
        await().atMost(Duration.ofSeconds(20)).until { support.committed(defaultGroup) == mapOf(0 to 3L, 1 to 3L) }
        // Markers without a position (a saga's, an external source's, a dispatch outside Kafka) and of another processor.
        val others = listOf("saga:Order:r-0-0", "processor:audit:other-1", "processor:default:printer:k-1", "processor:default:other-2")
        others.forEach { mongo.insert(Document(mapOf("_id" to it, "type" to "test.Happened", "processedAt" to java.util.Date())), "processed_events") }
        val auditMarkers = markers().filter { it.startsWith("processor:audit:r-") }.toSet()
        assertEquals(6, auditMarkers.size)

        assertTrue(a.ask(prepare("op-1")).hasOk())
        assertTrue(b.ask(prepare("op-1")).hasOk())
        assertEquals(0, members())
        val reply = a.ask(execute("op-1", earliest))
        assertTrue(reply.hasReset(), reply.toString())
        assertEquals(listOf(Triple(0, 0L, 3L), Triple(1, 0L, 3L)), reply.reset.triples())
        assertEquals(6, reply.reset.markersDeleted, "the six positioned markers of default in the range")
        assertEquals(1, reply.reset.resetHandlers)
        val ctx = resets.contexts.single()
        assertEquals(ResetTarget.Earliest, ctx.target)
        assertEquals("op-1", ctx.operationId)
        assertEquals(listOf(ReplayRange(topic, "0", "0", "3"), ReplayRange(topic, "1", "0", "3")), ctx.ranges)
        assertEquals(0, audit.resets, "another processor's reset handler does not run")
        assertEquals(mapOf(0 to 0L, 1 to 0L), support.committed(defaultGroup))
        assertTrue(markers().containsAll(auditMarkers + others), "only default's markers in the range went")
        assertEquals(listOf(ReplayRange(topic, "0", "0", "3"), ReplayRange(topic, "1", "0", "3")), control()!!.replay)

        assertTrue(a.ask(finish("op-1")).hasOk())
        assertTrue(b.ask(finish("op-1")).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 12 }
        assertEquals(projection.seen.groupingBy { it }.eachCount().values.toSet(), setOf(2), "every record once more, overall")
        await().atMost(Duration.ofSeconds(10)).until { control()!!.replay.isEmpty() }
        await().atMost(Duration.ofSeconds(10)).until { !a.status().replaying && !b.status().replaying }
        assertEquals(6, once.seen.size, "@DisallowReplay did not run again")
        assertEquals(6, sagas.calls.size, "sagas never replay")
        assertEquals(6, audit.seen.size, "another processor is not reset")

        support.send(topic, "r-new", partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { "r-new" in once.seen && "r-new" in sagas.calls }
    }

    @Test fun `a reset of a paused processor keeps the pause and reports its open replay ranges`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 2)
        val a = instance(projection)
        support.send(topic, "k-0", partition = 0)
        support.send(topic, "k-1", partition = 0)
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 2 && support.committed(defaultGroup)[0] == 2L }
        assertTrue(a.ask(pause()).hasOk())
        assertTrue(a.ask(prepare("op-1")).hasOk())
        assertTrue(a.ask(execute("op-1", earliest)).hasReset())
        assertTrue(a.ask(finish("op-1")).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { a.status().segmentsCount == 2 }
        val status = a.status()
        assertEquals(ProcessorState.PROCESSOR_PAUSED, status.state, "finishing a reset keeps the pause")
        assertEquals("", status.operationId)
        assertTrue(status.replaying)
        assertEquals(mapOf("0" to "2", "1" to ""), status.segmentsList.associate { it.segment to it.replayUntil })
        Thread.sleep(1000)
        assertEquals(2, projection.seen.size, "nothing replays while paused")
        assertTrue(a.ask(resume()).hasOk())
        await().atMost(Duration.ofSeconds(20)).until { projection.seen.size == 4 }
        await().atMost(Duration.ofSeconds(10)).until { !a.status().replaying && control()!!.replay.isEmpty() }
    }

    @Test fun `a timestamp or explicit offsets replay from there, latest makes no range, bad offsets are refused`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        repeat(3) { support.send(topic, "t-$it") }
        Thread.sleep(50)
        val cut = Instant.now().toEpochMilli()
        Thread.sleep(50)
        repeat(3) { support.send(topic, "t-${it + 3}") }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 6 && support.committed(defaultGroup)[0] == 6L }

        fun reset(op: String, target: WireTarget): ControlReply {
            assertTrue(a.ask(prepare(op)).hasOk())
            return a.ask(execute(op, target)).also { assertTrue(a.ask(finish(op, aborted = !it.hasReset())).hasOk()) }
        }

        val byTime = reset("op-t", at(cut))
        assertTrue(byTime.hasReset(), byTime.toString())
        assertEquals(listOf(Triple(0, 3L, 6L)), byTime.reset.triples())
        assertEquals(3, byTime.reset.markersDeleted)
        assertEquals(ResetTarget.Timestamp(cut), resets.contexts.last().target)
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 9 }
        await().atMost(Duration.ofSeconds(10)).until { control()!!.replay.isEmpty() }

        val byOffset = reset("op-o", offsets(0 to 5L))
        assertTrue(byOffset.hasReset(), byOffset.toString())
        assertEquals(listOf(Triple(0, 5L, 6L)), byOffset.reset.triples())
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 10 }
        assertEquals("t-5", projection.seen.last())

        val handlers = resets.contexts.size
        val toEnd = reset("op-l", latest)
        assertTrue(toEnd.hasReset(), toEnd.toString()) // a refusal has no outcome: its zero counts below would pass
        assertEquals(0, toEnd.reset.rangesCount)
        assertEquals(0, toEnd.reset.markersDeleted)
        assertEquals(0, toEnd.reset.resetHandlers)
        assertEquals(handlers, resets.contexts.size, "no range: no reset handler")

        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-1", offsets(9 to 0L)).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-2", offsets(0 to -1L)).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-3", offsets(0 to 7L)).refusal(), "past the high watermark")
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-4", WireTarget.getDefaultInstance()).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-5", offsets()).refusal(), "no offsets")
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reset("op-6", offsets(0 to 1L, 0 to 2L)).refusal(), "a partition twice")
        awaitRunning(a)
        assertEquals(6L, support.committed(defaultGroup)[0])
    }

    @Test fun `a poll that read the document before a prepare keeps the operation prepared`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val reads = HeldReads(support.store.processorControl)
        val a = instance(projection, resets, controlStore = reads)
        support.send(topic, "q-0")
        await().atMost(Duration.ofSeconds(30)).until { support.committed(defaultGroup)[0] == 1L }
        reads.betweenReadAndApply { assertTrue(a.ask(prepare("op-1")).hasOk()) }
        assertEquals(ProcessorState.PROCESSOR_STOPPED, a.status().state, "a document without the lock does not start the consumer again")
        assertEquals("op-1", a.status().operationId)
        reads.release()
        val reply = a.ask(execute("op-1", earliest))
        assertTrue(reply.hasReset(), reply.toString())
        assertTrue(a.ask(finish("op-1")).hasOk())
    }

    @Test fun `a poll that read the document before a pause keeps the processor paused`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val reads = HeldReads(support.store.processorControl)
        val a = instance(projection, controlStore = reads)
        awaitRunning(a)
        reads.betweenReadAndApply { assertTrue(a.ask(pause()).hasOk()) }
        assertEquals(ProcessorState.PROCESSOR_PAUSED, a.status().state, "a document without the pause does not resume")
        reads.release()
        support.send(topic, "q-1")
        Thread.sleep(1500)
        assertFalse("q-1" in projection.seen, "held while paused")
    }

    @Test fun `a poll that read the document before a finish keeps the processor consuming`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val reads = HeldReads(support.store.processorControl)
        val a = instance(projection, controlStore = reads)
        awaitRunning(a)
        assertTrue(a.ask(prepare("op-1")).hasOk())
        reads.betweenReadAndApply { assertTrue(a.ask(finish("op-1")).hasOk()) }
        val state = a.status().state
        assertTrue(state == ProcessorState.PROCESSOR_STARTING || state == ProcessorState.PROCESSOR_RUNNING, "a document with the released lock does not stop the consumer: $state")
        assertEquals("", a.status().operationId)
        reads.release()
        awaitRunning(a)
    }

    @Test fun `a failing reset handler fails the reset, says it ran, and touches no marker or offset`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        repeat(2) { support.send(topic, "f-$it") }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 2 && support.committed(defaultGroup)[0] == 2L }
        val before = markers()
        resets.failing = true
        assertTrue(a.ask(prepare("op-1")).hasOk())
        val reply = a.ask(execute("op-1", earliest))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_RESET_HANDLER_FAILED, reply.refusal(), reply.toString())
        assertTrue("cannot clear the projection" in reply.refused.message, reply.refused.message)
        assertTrue("reset handlers ran (1), no markers deleted, positions not moved" in reply.refused.message, reply.refused.message)
        assertEquals(2L, support.committed(defaultGroup)[0])
        assertEquals(before, markers())
        assertEquals(emptyList(), control()!!.replay)
        assertTrue(a.ask(finish("op-1", aborted = true)).hasOk())
        awaitRunning(a)
    }

    @Test fun `renewals are answered at once while the execute runs, and the executor keeps its own lease`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        support.send(topic, "w-1")
        await().atMost(Duration.ofSeconds(30)).until { support.committed(defaultGroup)[0] == 1L }
        assertTrue(a.ask(prepare("op-1", leaseMs = 1500)).hasOk())
        resets.gate = CountDownLatch(1)
        val executed = CompletableFuture<ControlReply>()
        a.control!!.handle(execute("op-1", earliest)) { executed.complete(it) }
        Thread.sleep(500)
        val began = System.nanoTime()
        assertTrue(a.ask(prepare("op-1", leaseMs = 1500), Duration.ofSeconds(2)).hasOk(), "not queued behind the execute")
        assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofSeconds(1))
        Thread.sleep(3000) // twice the lease without a renewal from the hub
        assertEquals("op-1", control()!!.operation!!.id, "the executor extended its lease")
        assertFalse(control()!!.operation!!.expired(Instant.now()))
        resets.gate!!.countDown()
        assertTrue(executed.get(30, TimeUnit.SECONDS).hasReset())
        assertTrue(a.ask(finish("op-1")).hasOk())
    }

    @Test fun `dead letters are listed beside a running execute and a change to them is refused LOCKED at once`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        support.send(topic, "w-1")
        await().atMost(Duration.ofSeconds(30)).until { support.committed(defaultGroup)[0] == 1L }
        assertTrue(a.ask(prepare("op-1")).hasOk())
        resets.gate = CountDownLatch(1)
        val executed = CompletableFuture<ControlReply>()
        a.control!!.handle(execute("op-1", earliest)) { executed.complete(it) }
        Thread.sleep(500)

        val began = System.nanoTime()
        val page = a.ask(request("default") { deadLetterList = DeadLetterList.newBuilder().setLimit(10).build() }, Duration.ofSeconds(2))
        assertTrue(page.hasDeadLetters(), "the list is read beside the execute: $page")
        assertEquals(0L, page.deadLetters.total)
        val letter = "default:$topic:0:0"
        for (change in listOf(
            request("default") { deadLetterRetry = DeadLetterRetry.newBuilder().setLetterId(letter).build() },
            request("default") { deadLetterRetryAll = DeadLetterRetryAll.getDefaultInstance() },
            request("default") { deadLetterDiscard = DeadLetterDiscard.newBuilder().setLetterId(letter).build() },
        )) {
            val reply = a.ask(change, Duration.ofSeconds(2))
            assertEquals(ControlRefusal.CONTROL_REFUSAL_LOCKED, reply.refusal(), "$reply")
            assertEquals("processor 'default' is executing reset operation op-1; try again when it finished", reply.refused.message)
        }
        assertTrue(Duration.ofNanos(System.nanoTime() - began) < Duration.ofSeconds(2), "answered at once, not behind the execute")
        assertFalse(executed.isDone, "the execute is still running")

        resets.gate!!.countDown()
        assertTrue(executed.get(30, TimeUnit.SECONDS).hasReset())
        assertTrue(a.ask(finish("op-1")).hasOk())
        assertTrue(a.ask(request("default") { deadLetterRetryAll = DeadLetterRetryAll.getDefaultInstance() }).hasRetryAll(), "after the execute as before")
    }

    @Test fun `a prepare or renewal with a lease under a second is invalid`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection)
        for (leaseMs in listOf(-1L, 0L, 1L, 999L)) {
            val reply = a.ask(prepare("op-1", leaseMs = leaseMs))
            assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, reply.refusal(), "lease $leaseMs ms")
            assertEquals("a prepare needs an operation id and a lease of at least 1000 ms", reply.refused.message)
            assertNull(control()?.operation, "no lock taken")
        }
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, a.ask(prepare("op-1", processor = "elsewhere", leaseMs = 1)).refusal(), "checked before the processor")
        assertTrue(a.ask(prepare("op-1", leaseMs = 1000)).hasOk(), "a second is a lease")
        val renewal = a.ask(prepare("op-1", leaseMs = 999))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, renewal.refusal(), "a renewal too")
        assertTrue(a.ask(finish("op-1")).hasOk())
    }

    @Test fun `dead letters are listed, retried, retried all and discarded through control`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        audit.failing = "d-1"
        val a = instance(projection, audit)
        support.send(topic, "d-1", key = "k1")
        support.send(topic, "d-2", key = "k1")
        support.send(topic, "d-3", key = "k2", type = null)
        await().atMost(Duration.ofSeconds(30)).until { a.deadLetters.count("audit") == 3L }
        val list = a.ask(request("audit") { deadLetterList = DeadLetterList.newBuilder().setLimit(10).build() })
        assertEquals(3, list.deadLetters.total)
        val byEvent = list.deadLetters.lettersList.associateBy { it.eventId }
        val head = byEvent.getValue("d-1")
        assertEquals("audit", head.processor)
        assertEquals(topic, head.source)
        assertEquals("0", head.segment)
        assertEquals("0", head.position)
        assertEquals("k1", head.key)
        assertEquals("test.Happened", head.eventType)
        assertEquals("HANDLER_FAILED", head.reason)
        assertEquals("IllegalStateException", head.errorType)
        assertEquals("audit cannot take d-1", head.errorMessage)
        assertEquals(2, head.attempts)
        assertTrue(head.parkedAtMs > 0 && head.lastAttemptAtMs >= head.parkedAtMs)
        val follower = byEvent.getValue("d-2")
        assertEquals("SEQUENCED", follower.reason)
        assertEquals(head.id, follower.blockedBy)
        val invalid = list.deadLetters.lettersList.single { it.reason == "INVALID" }

        fun retry(id: String) = a.ask(request("audit") { deadLetterRetry = DeadLetterRetry.newBuilder().setLetterId(id).build() }).retry
        val failed = retry(head.id)
        assertEquals("Failed", failed.result)
        assertEquals("audit cannot take d-1", failed.detail)
        assertEquals(listOf("Blocked", head.id), retry(follower.id).let { listOf(it.result, it.detail) })
        assertEquals("NotRetriable", retry(invalid.id).result)
        assertEquals("NotFound", retry("audit:$topic:0:99").result)
        assertEquals("NotFound", a.ask(request("default") { deadLetterRetry = DeadLetterRetry.newBuilder().setLetterId(head.id).build() }).retry.result, "another processor's letter")
        audit.failing = null
        assertEquals("Retried", retry(head.id).result)

        val all = a.ask(request("audit") { deadLetterRetryAll = DeadLetterRetryAll.getDefaultInstance() }).retryAll
        assertEquals(listOf(1L, 0L, 0L, 1L), listOf(all.retried, all.failed, all.blocked, all.skipped))
        fun discard(id: String) = a.ask(request("audit") { deadLetterDiscard = DeadLetterDiscard.newBuilder().setLetterId(id).build() })
        assertTrue(discard(invalid.id).hasOk())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_FOUND, discard(invalid.id).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, discard("").refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, a.ask(request("audit") { deadLetterList = DeadLetterList.newBuilder().setLimit(0).build() }).refusal())
        assertEquals(ControlRefusal.CONTROL_REFUSAL_INVALID, a.ask(request("audit") {}).refusal(), "no action")
        assertEquals(listOf("d-1", "d-2"), audit.seen.toList())
    }

    @Test fun `a late renewal never takes the lock again`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection)
        awaitRunning(a)
        assertTrue(a.ask(prepare("op-1")).hasOk())
        assertTrue(a.ask(finish("op-1")).hasOk())
        val late = a.ask(prepare("op-1"))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, late.refusal(), late.toString())
        assertTrue("op-1 already finished" in late.refused.message, late.refused.message)
        assertNull(control()!!.operation)
        awaitRunning(a)

        // The lock is cleared under the held operation (another instance cleared its passed lease).
        assertTrue(a.ask(prepare("op-2")).hasOk())
        runBlocking { support.store.processorControl.release("default", "op-2") }
        val lost = a.ask(prepare("op-2"))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, lost.refusal(), lost.toString())
        // Whichever notices the lost lock first answers (ADR 0023 ruling 19): the renewal ("lock lost"), or the 300 ms poll,
        // which forgets op-2 so this prepare is a late one ("already finished"). Either way the lock is not taken (below).
        assertTrue("lock lost" in lost.refused.message || "op-2 already finished" in lost.refused.message, lost.refused.message)
        assertNull(control()!!.operation)
        assertEquals(ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, a.ask(prepare("op-2")).refusal(), "and so is every later one")
        awaitRunning(a)

        assertTrue(a.ask(prepare("op-3")).hasOk(), "a first prepare still locks")
        assertEquals("op-3", control()!!.operation!!.id)
        assertTrue(a.ask(finish("op-3")).hasOk())
    }

    @Test fun `a lease lost while the reset handlers run stops the reset before the markers and the offsets`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        repeat(2) { support.send(topic, "s-$it") }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 2 && support.committed(defaultGroup)[0] == 2L }
        val before = markers()
        val store = support.store.processorControl
        // Another instance cleared the passed lease and took the lock.
        resets.during = { runBlocking { store.release("default", "op-1"); store.lock("default", "op-other", Duration.ofSeconds(60)) } }
        assertTrue(a.ask(prepare("op-1")).hasOk())
        val reply = a.ask(execute("op-1", earliest))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_FAILED, reply.refusal(), reply.toString())
        for (said in listOf("lease lost", "reset handlers ran (1)", "no markers deleted", "positions not moved")) {
            assertTrue(said in reply.refused.message, "$said: ${reply.refused.message}")
        }
        assertEquals(2L, support.committed(defaultGroup)[0])
        assertEquals(before, markers())
        assertEquals(emptyList(), control()!!.replay)
    }

    @Test fun `an execute past its lease limit stops before the markers`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets, executeLeaseLimit = Duration.ofSeconds(1))
        support.send(topic, "c-0")
        await().atMost(Duration.ofSeconds(30)).until { support.committed(defaultGroup)[0] == 1L }
        val before = markers()
        resets.during = { Thread.sleep(1500) }
        assertTrue(a.ask(prepare("op-1")).hasOk())
        val reply = a.ask(execute("op-1", earliest))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_FAILED, reply.refusal(), reply.toString())
        assertTrue("lease lost: the reset ran longer than 1 s" in reply.refused.message, reply.refused.message)
        assertEquals(1L, support.committed(defaultGroup)[0])
        assertEquals(before, markers())
        assertTrue(a.ask(finish("op-1", aborted = true)).hasOk())
    }

    @Test fun `a reset handler that throws an Error is answered RESET_HANDLER_FAILED`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, resets)
        support.send(topic, "e-0")
        await().atMost(Duration.ofSeconds(30)).until { support.committed(defaultGroup)[0] == 1L }
        resets.during = { TODO("the projection reset") }
        assertTrue(a.ask(prepare("op-1")).hasOk())
        val reply = a.ask(execute("op-1", earliest), Duration.ofSeconds(15))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_RESET_HANDLER_FAILED, reply.refusal(), reply.toString())
        assertTrue("NotImplementedError" in reply.refused.message, reply.refused.message)
        assertTrue(a.ask(finish("op-1", aborted = true)).hasOk())
        awaitRunning(a)
    }

    @Test fun `a letter parked during a replay is retried as a replay, without its DisallowReplay handlers`() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val flaky = FlakyProjection()
        val a = instance(flaky, once)
        support.send(topic, "x-1")
        await().atMost(Duration.ofSeconds(30)).until { "x-1" in once.seen && support.committed(defaultGroup)[0] == 1L }
        flaky.failing = "x-1"
        assertTrue(a.ask(prepare("op-1")).hasOk())
        assertTrue(a.ask(execute("op-1", earliest)).hasReset())
        assertTrue(a.ask(finish("op-1")).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { a.deadLetters.count("default") == 1L }
        val id = DeadLetterStore.id("default", topic, "0", "0")
        val parked = mongo.db.getCollection(DEAD_LETTERS).find(Document("_id", id)).first()!!
        assertEquals(true, parked.getBoolean("replay"), parked.toJson())
        assertTrue(a.deadLetters.get(id)!!.replay)
        flaky.failing = null
        val retried = a.ask(request("default") { deadLetterRetry = DeadLetterRetry.newBuilder().setLetterId(id).build() }).retry
        assertEquals("Retried", retried.result, retried.detail)
        assertEquals(4, flaky.seen.count { it == "x-1" }, "handled, failed twice in the replay, retried")
        assertEquals(1, once.seen.count { it == "x-1" }, "the retry of a replayed record is a replay")
    }

    /** The Kafka event store, declaring no `replay` (as an event store that cannot move positions would). */
    private fun withoutReplay(): EventStore = object : EventStore by support.events {
        override val capabilities: Set<Capability> = setOf(Capability.BROWSE, Capability.LAG)
        override fun supports(capability: Capability): Boolean = capability in capabilities
        override fun admin(spec: SubscriptionSpec): SubscriptionAdmin = throw EventStoreException.Unsupported(Capability.REPLAY)
    }

    @Test fun controlActionWithoutCapabilityIsUnsupported() {
        topic = support.newTopic("kraken.events.ctl", partitions = 1)
        val a = instance(projection, events = withoutReplay())
        support.send(topic, "u-1")
        await().atMost(Duration.ofSeconds(30)).until { "u-1" in projection.seen }
        awaitRunning(a)
        val status = a.status()
        assertEquals("kafka", status.eventStore)
        assertEquals(listOf("browse", "lag"), status.capabilitiesList)
        assertEquals(defaultGroup, status.subscription)
        assertEquals(topic, status.source)

        val prepared = a.ask(prepare("op-u"))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_UNSUPPORTED, prepared.refusal(), prepared.toString())
        assertTrue("replay" in prepared.refused.message, prepared.refused.message)
        assertEquals(ControlRefusal.CONTROL_REFUSAL_UNSUPPORTED, a.ask(execute("op-u", earliest)).refusal())
        assertNull(control()?.operation, "no lock was taken")
        assertEquals(ProcessorState.PROCESSOR_RUNNING, a.status().state, "the processor keeps its subscription")
        assertEquals("", a.status().operationId)
        assertEquals(1, members(), "still a member of its subscription")
        support.send(topic, "u-2")
        await().atMost(Duration.ofSeconds(20)).until { "u-2" in projection.seen }
        assertTrue(a.ask(pause()).hasOk(), "pause and resume need no capability")
        assertTrue(a.ask(resume()).hasOk())
    }

    /**
     * Instance A holds both segments and resets to earliest while paused; B joins and takes one segment: each reports the
     * replay range of its own segment, replays only that one after the resume (without the DisallowReplay handler), the
     * records published after the reset are no replay, nothing is handled twice elsewhere, and each owner removes its range.
     */
    @Test fun replayRangeSurvivesReassignment() {
        topic = support.newTopic("kraken.events.ctl", partitions = 2)
        val a = instance(projection, once)
        for (p in 0..1) repeat(2) { support.send(topic, "n-$p-$it", key = "k$p", partition = p) }
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 4 && support.committed(defaultGroup) == mapOf(0 to 2L, 1 to 2L) }
        assertTrue(a.ask(pause()).hasOk())
        assertTrue(a.ask(prepare("op-r")).hasOk())
        val reset = a.ask(execute("op-r", earliest))
        assertEquals(listOf(Triple(0, 0L, 2L), Triple(1, 0L, 2L)), reset.reset.triples())
        assertTrue(a.ask(finish("op-r")).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { a.status().segmentsCount == 2 }

        val b = instance(projection, once)
        await().atMost(Duration.ofSeconds(40)).until { a.status().segmentsCount == 1 && b.status().segmentsCount == 1 }
        val mine = a.status().segmentsList.single()
        val theirs = b.status().segmentsList.single()
        assertEquals(setOf("0", "1"), setOf(mine.segment, theirs.segment))
        assertEquals("2", mine.replayUntil, "A shows the range of its own segment")
        assertEquals("2", theirs.replayUntil, "B shows the range of the segment it took over")
        assertEquals(ProcessorState.PROCESSOR_PAUSED, b.status().state, "B starts paused")

        for (p in 0..1) support.send(topic, "after-$p", key = "k$p", partition = p)
        assertTrue(b.ask(resume()).hasOk())
        await().atMost(Duration.ofSeconds(30)).until { projection.seen.size == 10 }
        Thread.sleep(1000)
        assertEquals(10, projection.seen.size, "nothing more")
        val counts = projection.seen.groupingBy { it }.eachCount()
        assertEquals((0..1).flatMap { p -> (0..1).map { "n-$p-$it" } }.associateWith { 2 } + mapOf("after-0" to 1, "after-1" to 1), counts)
        assertEquals(listOf("n-0-0", "n-0-1", "n-1-0", "n-1-1", "after-0", "after-1").sorted(), once.seen.sorted(), "DisallowReplay skipped the replay, not the later records")
        await().atMost(Duration.ofSeconds(20)).until { control()!!.replay.isEmpty() }
        await().atMost(Duration.ofSeconds(10)).until { !a.status().replaying && !b.status().replaying }
    }
}
