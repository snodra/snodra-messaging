package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.hub.v1.PartitionStatus
import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.control.ReplayRange
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.deadletter.DeadLetterStore
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.Processors
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.MessagingMetrics.Consumed
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.util.backoff.FixedBackOff
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * One event processor (ADR 0022): consumes one event topic in its own consumer group with at-least-once delivery and
 * exactly-once handler effects, and puts what it cannot handle in its [DeadLetterSink].
 *
 * Per record the [LocalEventDispatcher] runs [processor]'s handlers (stage 1, marker [Processors.markerId]), then, for
 * `default` only, the saga stage. A handler failure is retried in place up to [maxAttempts] invocations in all, every
 * [backOffMs]; an [InvalidMessageException] (the payload does not fit the handler's message class) is not retried. Then the
 * record goes to the sink: mode `collection` parks a `HANDLER_FAILED` letter, `topic` produces it to `<topic>.dlt`, `log`
 * logs it (never the payload) and skips it. A record that only failed in stage 2 (the sagas could not run: their store is
 * down) is logged and skipped. A record without a `type` header, or whose payload is not a JSON object for a type this
 * processor handles, is `INVALID` (parked, produced, or logged; it never blocks its key). A type the processor does not
 * handle is acknowledged and skipped. A missing `id` header falls back to `<topic>-<partition>-<offset>`.
 *
 * **Sequencing** (mode `collection`): the keys of this processor's letters on the assigned partitions are loaded on
 * assignment and added to on every park. A record with a stage-1 handler whose key is in that set is confirmed against
 * the collection: when the key still has a head letter, the record is parked `SEQUENCED` without running stage 1 (stage 2
 * still runs for it in `default`, ADR 0021 ruling 26, with the attempts and the log-and-skip of any record's stage 2); a miss (the letter was retried, discarded or expired, possibly on
 * another instance) forgets the key and handles the record. A record whose own letter already exists was parked before a
 * crash: it is acknowledged.
 *
 * **Acknowledgement.** The offset is acknowledged only after the dispatch succeeded or the sink took the record (the
 * letter exists, the broker acknowledged the DLT record). When the sink fails (MongoDB or the DLT unreachable) the listener
 * throws, nothing is acknowledged and the container redelivers the record every [backOffMs] for as long as it takes; the
 * redelivery goes straight to the sink (it does not run the handlers again) unless the partition moved meanwhile.
 *
 * The record's correlation id is in the handlers' coroutine context ([io.damoreira.kraken.messaging.tracing.CorrelationContext])
 * and the MDC, so events they publish carry it. Handlers run via `runBlocking` on the listener thread so the thread-bound
 * Mongo transaction wraps them; handlers that switch threads lose the transaction.
 *
 * [status] reports the processor for the hub (ADR 0022): assignment and positions from the rebalance listener and the
 * acknowledged records, committed offsets and high watermarks from [refreshOffsets] (called off the consumer thread with
 * an `Admin`), the dead-letter count from [sampleDeadLetters].
 *
 * **Control** (P6b, ADR 0023, driven by [io.damoreira.kraken.messaging.control.ProcessorControl]): [pause] stops consuming
 * without leaving the group (kept across [stop] and [start]); the stage-1 marker of every record carries its position; a
 * record inside an open [replay] range runs stage 1 without its `@DisallowReplay` handlers and never stage 2. The status says
 * PAUSED, the [operationId] holding the processor's lock, `replaying` and each partition's `replay_until`.
 *
 * **Metrics** (P9): each record's final outcome in this processor goes to [metrics] once ([Consumed]; a parked record when
 * the sink took it, so a redelivery after a sink failure counts once, and as a duplicate when its letter was there already),
 * labelled with its type when the processor handles it,
 * else [MessagingMetrics.OTHER]. A stop in the middle of the attempts records nothing: the record comes back.
 */
class EventConsumer(
    consumerFactory: ConsumerFactory<String, ByteArray>,
    /** The event topic this processor consumes. */
    val topic: String,
    private val groupId: String,
    private val registry: HandlerRegistry,
    invoker: HandlerInvoker,
    tx: TransactionTemplate,
    mongo: MongoTemplate,
    private val maxAttempts: Int,
    private val backOffMs: Long = 1000L,
    private val codec: JsonCodec = JsonCodec(),
    private val dispatcher: LocalEventDispatcher = LocalEventDispatcher(registry, invoker, tx, mongo),
    /** The processor this consumer runs: `default` or a `@ProcessingGroup` name. */
    val processor: String = Processors.DEFAULT,
    private val deadLetters: DeadLetterSink = DeadLetterSink.Log,
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(EventConsumer::class.java)
    private val factory = consumerFactory
    private val running = AtomicBoolean(false)
    @Volatile private var started = false
    @Volatile private var container: ConcurrentMessageListenerContainer<String, ByteArray>? = null
    @Volatile private var pauseRequested = false
    @Volatile private var replayRanges: List<ReplayRange> = emptyList()

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
    }

    // --- status (written on the consumer thread or the refresher, read by status()) ---
    private class Track {
        val position = AtomicLong(-1)
        @Volatile var committed = -1L
        @Volatile var highWatermark = -1L
    }
    private val assigned = ConcurrentSkipListMap<Int, Track>()
    @Volatile private var assignedOnce = false
    @Volatile private var rebalancing = false
    private val processedTotal = AtomicLong()
    private val failedTotal = AtomicLong()
    @Volatile private var lastError: String = ""
    @Volatile private var lastErrorAtMs = 0L
    @Volatile private var failingSinceMs = 0L
    @Volatile private var deadLetterCount = 0L
    private val parkedSinceSample = AtomicBoolean(true)

    // --- consumer thread only ---
    private val parkedKeys = HashMap<Int, MutableSet<String>>() // partition -> keys with a blocking letter
    private val keysUnknown = HashSet<Int>() // partitions whose keys could not be loaded: every keyed record is confirmed
    private val pending = ConcurrentHashMap<String, () -> Unit>() // "<partition>:<offset>" -> what the sink still has to do

    private val store: DeadLetterStore? = (deadLetters as? DeadLetterSink.Collection)?.store

    @Synchronized
    override fun start() {
        if (running.get()) return
        val props = ContainerProperties(topic).apply {
            setGroupId(this@EventConsumer.groupId)
            setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
            setMessageListener(AcknowledgingMessageListener<String, ByteArray> { record, ack -> handle(record, ack!!) })
            setConsumerRebalanceListener(Rebalance())
            // A pause (P6b) takes effect after the record in hand: the container keeps the rest of the poll's records in memory
            // and hands them to the listener on resume.
            setPauseImmediate(true)
        }
        // Only what the listener throws reaches it: a sink that could not take the record, or a stop in the middle of the
        // attempts. Never recovered (skipped): the record is redelivered until it is handled or parked.
        val errorHandler = DefaultErrorHandler({ _, _ -> }, FixedBackOff(backOffMs, FixedBackOff.UNLIMITED_ATTEMPTS)).apply {
            setLogLevel(KafkaException.Level.DEBUG)
        }
        assignedOnce = false
        rebalancing = false
        started = true
        running.set(true) // before the container: a record may arrive before start() returns
        val up = try {
            ConcurrentMessageListenerContainer(factory, props).apply {
                concurrency = 1
                setCommonErrorHandler(errorHandler)
                if (pauseRequested) pause() // started paused: its partitions are paused as they are assigned
                start()
            }
        } catch (e: Exception) {
            running.set(false) // a failed start leaves the consumer stopped and restartable
            started = false
            throw e
        }
        container = up
        log.info(
            "Event processor {} started on topic {} group {} handling {} event types, dead letters: {}{}",
            processor, topic, groupId, dispatcher.eventTypes(processor).size, deadLetters.mode.wireName, if (pauseRequested) ", paused" else "",
        )
    }

    /**
     * Pauses consumption (P6b): every assigned partition, and those assigned later, without leaving the group; the record in
     * hand finishes, nothing else starts. Kept across [stop] and [start]. Idempotent.
     */
    @Synchronized
    fun pause() {
        pauseRequested = true
        container?.pause()
    }

    /** Resumes consumption after [pause]. Idempotent. */
    @Synchronized
    fun resume() {
        pauseRequested = false
        container?.resume()
    }

    /** Whether consumption is paused (or will start paused). */
    val paused: Boolean get() = pauseRequested

    /** The reset operation holding this processor's lock (P6b), reported as `operation_id`; empty when none. */
    @Volatile var operationId: String = ""

    /** Sets the open replay ranges (P6b) from the processor's control document; those of another topic are ignored. */
    fun replay(ranges: List<ReplayRange>) {
        replayRanges = ranges.filter { it.topic == topic }
    }

    /** The open replay ranges of this processor's topic. */
    fun replayRanges(): List<ReplayRange> = replayRanges

    /** The open ranges of the assigned partitions this instance's acknowledged position or the group's committed offset has reached the end of. */
    fun passedRanges(): List<ReplayRange> = replayRanges.filter { range ->
        val track = assigned[range.partition] ?: return@filter false
        maxOf(track.position.get(), track.committed) >= range.until
    }

    private fun inReplay(record: ConsumerRecord<*, *>): Boolean =
        replayRanges.any { it.partition == record.partition() && record.offset() < it.until }

    @Synchronized
    override fun stop() {
        if (!running.compareAndSet(true, false)) return
        container?.stop()
        container = null
        assigned.clear()
    }

    internal fun groupIdForTest(): String = groupId

    /** The Kafka consumer group of this processor. */
    val group: String get() = groupId

    override fun isRunning(): Boolean = running.get()
    override fun isAutoStartup(): Boolean = true
    /** Starts after and stops before [OutboxPublisher]; both stop after the web server's graceful drain. */
    override fun getPhase(): Int = PHASE

    private fun handle(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment) {
        pending[coords(record)]?.let { deliver(record, ack, it); return }
        val headers = HashMap<String, String>()
        record.headers().forEach { headers[it.key()] = String(it.value() ?: ByteArray(0), Charsets.UTF_8) }
        val type = headers[EventHeaders.TYPE]
        if (type.isNullOrBlank()) {
            invalid(record, ack, null, "record without type header"); return
        }
        if (!dispatcher.interested(processor, type)) {
            consumed(MessagingMetrics.OTHER, Consumed.SKIPPED)
            advance(record, ack); return
        }
        val id = headers[EventHeaders.ID]?.takeIf { it.isNotBlank() } ?: "${record.topic()}-${record.partition()}-${record.offset()}".also {
            log.warn("Record of type {} at {}-{}@{} has no id header, using {} for idempotency", type, record.topic(), record.partition(), record.offset(), it)
        }
        val payload = record.value() ?: ByteArray(0)
        val isObject = try { codec.decodeTree(payload).isObject } catch (_: Exception) { false }
        if (!isObject) {
            invalid(record, ack, id, "payload is not a JSON object", type); return
        }
        val ctx = MessageContext(
            messageId = id,
            type = type,
            correlationId = headers[EventHeaders.CORRELATION_ID] ?: id,
            userId = headers[EventHeaders.USER_ID]?.takeIf { it.isNotBlank() },
            roles = emptySet(),
            authorization = null,
            headers = headers,
        )
        val replay = inReplay(record)
        if (sequenced(record, ack, type, payload, ctx, replay)) return
        attempt(record, ack, id, type, payload, ctx, replay)
    }

    /** Parks [record] SEQUENCED when its key has a live letter (or acknowledges it when its own letter exists); true when done with it. */
    private fun sequenced(
        record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, type: String, payload: ByteArray, ctx: MessageContext, replay: Boolean,
    ): Boolean {
        val store = store ?: return false
        val key = record.key() ?: return false
        if (!dispatcher.hasStage1(processor, type)) return false // only stage 1 is sequenced
        val partition = record.partition()
        if (partition in keysUnknown) loadKeys(listOf(partition))
        if (partition !in keysUnknown && parkedKeys[partition]?.contains(key) != true) return false
        val head = try {
            store.head(processor, key)
        } catch (e: Exception) {
            failing(e, "could not check the dead letters of key at ${record.topic()}-$partition@${record.offset()}")
            throw e
        }
        if (head == null) {
            parkedKeys[partition]?.remove(key) // retried, discarded or expired, here or on another instance
            return false
        }
        val own = DeadLetterStore.id(processor, record.topic(), partition, record.offset())
        if (head == own || store.exists(own)) {
            log.info("Record {}-{}@{} is already parked in processor {}, acknowledging it", record.topic(), partition, record.offset(), processor)
            consumed(type, Consumed.DUPLICATE)
            advance(record, ack)
            return true
        }
        // The sagas see it even though stage 1 waits (ADR 0021 ruling 26), unless it is replayed (sagas never replay, P6b).
        if (processor == Processors.DEFAULT && !replay) stage2(record, type, payload, ctx)
        val letter = store.sequenced(processor, record, head, replay)
        log.info("Record {}-{}@{} of type {} parked SEQUENCED in processor {} behind {}", record.topic(), partition, record.offset(), type, processor, head)
        toSink(record, ack, type) { parked(store.park(letter)) }
        return true
    }

    /**
     * Stage 2 alone, for a record parked SEQUENCED in `default`, with the attempts and back-off of any record's stage 2: a
     * failure that outlives them is logged and skipped as for any record (counted in `failed_total`), and the record is parked.
     */
    private fun stage2(record: ConsumerRecord<String, ByteArray>, type: String, payload: ByteArray, ctx: MessageContext) {
        var attempts = 0
        while (true) {
            attempts++
            try {
                dispatcher.runStage2(type, payload, ctx)
                return
            } catch (e: Exception) {
                if (attempts >= maxAttempts) {
                    failedTotal.incrementAndGet()
                    failing(e, null)
                    log.error(
                        "Giving up on the saga stage of sequenced event type={} id={} at {}-{}@{} after {} attempts: {}: {}",
                        type, ctx.messageId, record.topic(), record.partition(), record.offset(), attempts, e::class.java.name, e.message,
                    )
                    return
                }
                if (!pause(backOffMs)) throw e // stopping: not acknowledged, redelivered after the restart or to the new owner
            }
        }
    }

    private fun attempt(
        record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, id: String, type: String, payload: ByteArray, ctx: MessageContext, replay: Boolean,
    ) {
        val position = RecordPosition(record.topic(), record.partition(), record.offset())
        var attempts = 0
        var failure: Exception
        while (true) {
            attempts++
            try {
                val outcome = dispatcher.dispatchIn(processor, id, type, payload, ctx, position, replay)
                processedTotal.incrementAndGet()
                consumed(type, if (outcome == LocalEventDispatcher.Outcome.DUPLICATE) Consumed.DUPLICATE else Consumed.HANDLED)
                advance(record, ack)
                return
            } catch (e: InvalidMessageException) {
                failure = e
                break // retrying cannot change the payload
            } catch (e: Exception) {
                failure = e
            }
            if (attempts >= maxAttempts) break
            if (!pause(backOffMs)) throw failure // stopping: not acknowledged, redelivered after the restart or to the new owner
        }
        exhausted(record, ack, id, type, failure, attempts, replay)
    }

    private fun exhausted(
        record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, id: String, type: String, failure: Exception, attempts: Int, replay: Boolean,
    ) {
        failedTotal.incrementAndGet()
        val cause = if (failure is SagaStageException) failure.cause ?: failure else failure
        val error = DeadLetterError.of(cause)
        failing(cause, null)
        if (failure is SagaStageException) {
            // Stage 1 is done (or had nothing to do); only the sagas could not run. Not a letter: stage 1 has nothing to retry.
            log.error(
                "Giving up on the saga stage of event type={} id={} at {}-{}@{} after {} attempts: {}: {}",
                type, id, record.topic(), record.partition(), record.offset(), attempts, cause::class.java.name, cause.message,
            )
            consumed(type, Consumed.FAILED)
            advance(record, ack); return
        }
        when (val sink = deadLetters) {
            is DeadLetterSink.Collection -> {
                val letter = sink.store.letter(processor, record, DeadLetterReason.HANDLER_FAILED, error, attempts, replay = replay)
                log.error(
                    "Event type={} id={} at {}-{}@{} failed {} attempts in processor {}, parked as dead letter {}: {}: {}",
                    type, id, record.topic(), record.partition(), record.offset(), attempts, processor, letter["_id"], cause::class.java.name, cause.message,
                )
                val key = record.key()
                toSink(record, ack, type) {
                    val new = sink.store.park(letter)
                    if (key != null) parkedKeys.getOrPut(record.partition()) { HashSet() } += key
                    parked(new)
                }
            }
            is DeadLetterSink.Topic -> {
                log.error(
                    "Event type={} id={} at {}-{}@{} failed {} attempts in processor {}, sending it to {}.dlt: {}: {}",
                    type, id, record.topic(), record.partition(), record.offset(), attempts, processor, record.topic(), cause::class.java.name, cause.message,
                )
                toSink(record, ack, type) { produce(sink, record, DeadLetterReason.HANDLER_FAILED, error); Consumed.PARKED }
            }
            DeadLetterSink.Log -> {
                log.error(
                    "Giving up on event type={} id={} at {}-{}@{} after {} attempts: {}: {}",
                    type, id, record.topic(), record.partition(), record.offset(), attempts, cause::class.java.name, cause.message,
                )
                consumed(type, Consumed.FAILED)
                advance(record, ack)
            }
        }
    }

    private fun invalid(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, id: String?, why: String, type: String? = null) {
        val error = DeadLetterError.invalid(why)
        val label = type ?: MessagingMetrics.OTHER
        when (val sink = deadLetters) {
            is DeadLetterSink.Collection -> {
                val letter = sink.store.letter(processor, record, DeadLetterReason.INVALID, error, attempts = 0, replay = inReplay(record))
                log.warn("Record {} of type {} at {}-{}@{} is invalid ({}), parked as dead letter {}", id, type, record.topic(), record.partition(), record.offset(), why, letter["_id"])
                toSink(record, ack, label) { parked(sink.store.park(letter)) }
            }
            is DeadLetterSink.Topic -> {
                log.warn("Record {} of type {} at {}-{}@{} is invalid ({}), sending it to {}.dlt", id, type, record.topic(), record.partition(), record.offset(), why, record.topic())
                toSink(record, ack, label) { produce(sink, record, DeadLetterReason.INVALID, error); Consumed.PARKED }
            }
            DeadLetterSink.Log -> {
                log.warn("Skipping record {} of type {} at {}-{}@{}: {}", id, type, record.topic(), record.partition(), record.offset(), why)
                consumed(label, Consumed.FAILED)
                advance(record, ack)
            }
        }
    }

    /**
     * Runs [work] (park, produce), counts the record as [type] with the outcome [work] returns and acknowledges; when [work]
     * throws the record stays unacknowledged and is kept, with its count, for the redelivery ([deliver]).
     */
    private fun toSink(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, type: String, work: () -> Consumed) =
        deliver(record, ack) { consumed(type, work()) }

    /**
     * The outcome of a letter [DeadLetterStore.park] took ([new]) or found there already: the record's own letter exists
     * (parked, then the instance died before the commit), so this delivery is a duplicate, as the Rust library counts it.
     */
    private fun parked(new: Boolean): Consumed {
        if (!new) return Consumed.DUPLICATE
        parkedSinceSample.set(true)
        return Consumed.PARKED
    }

    private fun deliver(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment, work: () -> Unit) {
        val coords = coords(record)
        pending[coords] = work
        try {
            work()
        } catch (e: Exception) {
            failing(e, "could not hand record ${record.topic()}-${record.partition()}@${record.offset()} to the dead letters (${deadLetters.mode.wireName}), not acknowledging it")
            throw e
        }
        pending.remove(coords)
        advance(record, ack)
    }

    private fun produce(sink: DeadLetterSink.Topic, record: ConsumerRecord<String, ByteArray>, reason: DeadLetterReason, error: DeadLetterError) {
        val out = ProducerRecord<String, ByteArray>("${record.topic()}.dlt", null, record.key(), record.value())
        record.headers().filter { it.key() !in DLT_HEADERS }.forEach { out.headers().add(it) }
        val add = { name: String, value: String -> out.headers().add(RecordHeader(name, value.toByteArray(Charsets.UTF_8))) }
        add(DLT_PROCESSOR, processor)
        add(DLT_REASON, reason.name)
        add(DLT_ERROR, DeadLetterError.cut("${error.type}: ${error.message}", DeadLetterError.MAX_MESSAGE))
        add(DLT_ORIGINAL_TOPIC, record.topic())
        add(DLT_ORIGINAL_PARTITION, record.partition().toString())
        add(DLT_ORIGINAL_OFFSET, record.offset().toString())
        sink.kafka.send(out).get(sink.sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun consumed(type: String, outcome: Consumed) = metrics.eventConsumed(processor, type, outcome)

    private fun advance(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment) {
        ack.acknowledge()
        assigned[record.partition()]?.position?.set(record.offset() + 1)
    }

    /** Sleeps [ms] in slices; false as soon as the consumer is stopping. */
    private fun pause(ms: Long): Boolean {
        val until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms)
        while (running.get()) {
            val left = until - System.nanoTime()
            if (left <= 0) return true
            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(left).coerceIn(1, 100))
        }
        return false
    }

    private fun failing(e: Throwable, context: String?) {
        val now = clock.millis()
        lastError = utf8Cut("${e::class.java.simpleName.ifEmpty { e::class.java.name }}: ${e.message ?: ""}", MAX_LAST_ERROR_BYTES)
        lastErrorAtMs = now
        failingSinceMs = now
        if (context != null) log.warn("Processor {}: {}: {}: {}", processor, context, e::class.java.name, e.message)
    }

    private fun loadKeys(partitions: Collection<Int>) {
        val store = store ?: return
        try {
            val keys = store.keys(processor, topic, partitions)
            for (p in partitions) {
                parkedKeys[p] = keys[p].orEmpty().toHashSet()
                keysUnknown.remove(p)
            }
        } catch (e: Exception) {
            keysUnknown += partitions // every keyed record of these partitions is confirmed until a load succeeds
            log.warn("Processor {}: could not load the keys of its dead letters for partitions {}: {}", processor, partitions, e.message)
        }
    }

    private inner class Rebalance : ConsumerAwareRebalanceListener {
        override fun onPartitionsRevokedBeforeCommit(consumer: Consumer<*, *>, partitions: Collection<TopicPartition>) {
            rebalancing = true
            forget(partitions)
        }

        override fun onPartitionsLost(consumer: Consumer<*, *>, partitions: Collection<TopicPartition>) {
            rebalancing = true
            forget(partitions)
        }

        override fun onPartitionsAssigned(consumer: Consumer<*, *>, partitions: Collection<TopicPartition>) {
            val numbers = partitions.filter { it.topic() == topic }.map { it.partition() }
            numbers.forEach { assigned.putIfAbsent(it, Track()) }
            loadKeys(numbers)
            assignedOnce = true
            rebalancing = false
        }

        private fun forget(partitions: Collection<TopicPartition>) {
            for (tp in partitions) {
                assigned.remove(tp.partition())
                parkedKeys.remove(tp.partition())
                keysUnknown.remove(tp.partition())
                pending.keys.removeIf { it.startsWith("${tp.partition()}:") } // the new owner parks it, from the start
            }
        }
    }

    /** This processor's status for the hub (ADR 0022), from in-memory state only: never blocks. */
    fun status(): ProcessorStatus {
        val now = clock.millis()
        val state = when {
            !running.get() -> ProcessorState.PROCESSOR_STOPPED
            pauseRequested -> ProcessorState.PROCESSOR_PAUSED
            failingSinceMs != 0L && now - failingSinceMs < FAILING_WINDOW_MS -> ProcessorState.PROCESSOR_FAILING
            !assignedOnce || rebalancing -> ProcessorState.PROCESSOR_STARTING
            else -> ProcessorState.PROCESSOR_RUNNING
        }
        val builder = ProcessorStatus.newBuilder()
            .setName(processor)
            .setGroupId(groupId)
            .setTopic(topic)
            .setState(state)
            .setProcessedTotal(processedTotal.get())
            .setFailedTotal(failedTotal.get())
            .setDeadLetters(if (deadLetters.mode == DeadLetterMode.COLLECTION) deadLetterCount else 0L)
            .setLastError(lastError)
            .setLastErrorAtMs(lastErrorAtMs)
            .addAllEventTypes(dispatcher.eventTypes(processor).sorted())
            .setDeadLetterMode(deadLetters.mode.wireName)
            .setOperationId(operationId)
        val ranges = replayRanges
        var replaying = false
        for ((partition, track) in assigned) {
            val position = track.position.get()
            val committed = track.committed
            val high = track.highWatermark
            val base = if (committed >= 0) committed else position
            val lag = if (high < 0 || base < 0) -1L else (high - base).coerceAtLeast(0)
            // Replaying until this instance (or the group) got past the range's end.
            val until = ranges.filter { it.partition == partition && maxOf(position, committed) < it.until }.maxOfOrNull { it.until } ?: 0L
            if (until > 0) replaying = true
            builder.addPartitions(
                PartitionStatus.newBuilder().setPartition(partition).setPosition(position).setCommitted(committed)
                    .setHighWatermark(high).setLag(lag).setReplayUntil(until),
            )
        }
        return builder.setReplaying(replaying).build()
    }

    /** Whether this processor was started (also while it is stopping or stopped: it then reports STOPPED). */
    val wasStarted: Boolean get() = started

    /**
     * Refreshes the committed offsets of this processor's group and the high watermarks of its assigned partitions through
     * [admin]. Blocking (at most [timeoutMs] per call): run it off the consumer thread and off the hub connection.
     */
    fun refreshOffsets(admin: Admin, timeoutMs: Long) {
        val partitions = assigned.keys.toList()
        if (partitions.isEmpty()) return
        val tps = partitions.map { TopicPartition(topic, it) }
        val committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(timeoutMs, TimeUnit.MILLISECONDS)
        val latest = admin.listOffsets(tps.associateWith { OffsetSpec.latest() }).all().get(timeoutMs, TimeUnit.MILLISECONDS)
        for (tp in tps) {
            val track = assigned[tp.partition()] ?: continue
            track.committed = committed[tp]?.offset() ?: -1L
            track.highWatermark = latest[tp]?.offset() ?: -1L
        }
    }

    /**
     * Samples this processor's letter count (`countDocuments({processor})`) when [force]d (every 30 s) or when a letter was
     * parked since the last sample. Blocking: run it off the consumer thread. No-op outside mode `collection`.
     */
    fun sampleDeadLetters(force: Boolean) {
        val store = store ?: return
        if (!force && !parkedSinceSample.get()) return
        parkedSinceSample.set(false)
        try {
            deadLetterCount = store.count(processor)
        } catch (e: Exception) {
            parkedSinceSample.set(true)
            throw e
        }
    }

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1900

        /** A processor reports FAILING this long after a record exhausted its attempts or could not be acknowledged. */
        const val FAILING_WINDOW_MS = 60_000L

        /** `ProcessorStatus.last_error` is cut to this many UTF-8 bytes. */
        const val MAX_LAST_ERROR_BYTES = 512

        const val DLT_PROCESSOR = "kraken-dlt-processor"
        const val DLT_REASON = "kraken-dlt-reason"
        const val DLT_ERROR = "kraken-dlt-error"
        const val DLT_ORIGINAL_TOPIC = "kraken-dlt-original-topic"
        const val DLT_ORIGINAL_PARTITION = "kraken-dlt-original-partition"
        const val DLT_ORIGINAL_OFFSET = "kraken-dlt-original-offset"
        private val DLT_HEADERS = setOf(DLT_PROCESSOR, DLT_REASON, DLT_ERROR, DLT_ORIGINAL_TOPIC, DLT_ORIGINAL_PARTITION, DLT_ORIGINAL_OFFSET)

        private fun coords(record: ConsumerRecord<*, *>) = "${record.partition()}:${record.offset()}"

        /** [text] cut to at most [maxBytes] UTF-8 bytes at a code point boundary. */
        internal fun utf8Cut(text: String, maxBytes: Int): String {
            if (text.toByteArray(Charsets.UTF_8).size <= maxBytes) return text
            var bytes = 0
            var i = 0
            while (i < text.length) {
                val cp = text.codePointAt(i)
                val n = when {
                    cp < 0x80 -> 1
                    cp < 0x800 -> 2
                    cp < 0x10000 -> 3
                    else -> 4
                }
                if (bytes + n > maxBytes) break
                bytes += n
                i += Character.charCount(cp)
            }
            return text.substring(0, i)
        }
    }
}
