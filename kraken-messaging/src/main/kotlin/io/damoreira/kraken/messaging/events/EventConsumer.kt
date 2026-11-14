package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.hub.v1.ProcessorState
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.hub.v1.SegmentStatus
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.MAX_POSITION_BYTES
import io.damoreira.kraken.messaging.eventstore.MAX_SEGMENT_BYTES
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.SegmentOrder
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.eventstore.deadLetterSource
import io.damoreira.kraken.messaging.handler.Processors
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.MessagingMetrics.Consumed
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import io.damoreira.kraken.messaging.store.DeadLetterStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * One event processor (ADR 0022, ADR 0027): reads the subscription [spec] (`<prefix>.<processor>` on the service's events
 * source) of the event store [events] with at-least-once delivery and exactly-once handler effects, and puts what it cannot
 * handle in its [DeadLetterSink].
 *
 * Per record the [LocalEventDispatcher] runs [processor]'s handlers (stage 1, marker [Processors.markerId]), then, for
 * `default` only, the saga stage. A handler failure is retried in place up to [maxAttempts] invocations in all, every
 * [backOffMs]; an [InvalidMessageException] (the payload does not fit the handler's message class) is not retried. Then the
 * record goes to the sink: mode `collection` parks a `HANDLER_FAILED` letter, `event-store` publishes it to the event store's
 * `<source>.dlt`, `log` logs it (never the payload) and skips it. A record that only failed in stage 2 (the sagas could not
 * run: their store is down) is logged and skipped. A record without a `type` header, or whose payload is not a JSON object for
 * a type this processor handles, is `INVALID` (parked, published, or logged; it never blocks its key). A type the processor
 * does not handle is acknowledged and skipped. A missing `id` header falls back to `<source>-<segment>-<position>`.
 *
 * **Sequencing** (mode `collection`): the keys of this processor's letters on the assigned segments are loaded on
 * assignment and added to on every park. A record with a stage-1 handler whose key is in that set is confirmed against
 * the store: when the key still has a head letter, the record is parked `SEQUENCED` without running stage 1 (stage 2
 * still runs for it in `default`, ADR 0021 ruling 26, with the attempts and the log-and-skip of any record's stage 2); a miss
 * (the letter was retried, discarded or expired, possibly on another instance) forgets the key and handles the record. A
 * record whose own letter already exists was parked before a crash: it is acknowledged.
 *
 * **Acknowledgement.** A record is acknowledged ([Subscription.ack]) only after the dispatch succeeded or the sink took it (the
 * letter exists, the event store took the dead letter). When the sink fails (the store or the event store unreachable) nothing
 * is acknowledged: the subscription hands the record out again ([Subscription.redeliver]) [backOffMs] later, for as long as it
 * takes; the redelivery goes straight to the sink (it does not run the handlers again) unless the segment moved meanwhile.
 *
 * **The subscription.** A thread of its own reads it (`kraken-processor-<name>`): handlers run there, so the thread-bound
 * store transaction wraps them (handlers that switch threads lose the transaction). A subscription that fails is closed and
 * subscribed again after a backoff ([Resubscribe]); the failure shows on the status (`subscription: ...`), as does what the
 * subscription recovered from by itself ([Subscription.takeError]).
 *
 * The record's correlation id is in the handlers' coroutine context ([io.damoreira.kraken.messaging.tracing.CorrelationContext])
 * and the MDC, so events they publish carry it.
 *
 * [status] reports the processor for the hub (ADR 0022): the event store and its capabilities, the segments and their positions
 * as the subscription reports them ([Subscription.segments], never blocking), the dead-letter count from [sampleDeadLetters].
 *
 * **Control** (P6b, ADR 0023, driven by [io.damoreira.kraken.messaging.control.ProcessorControl]): [pause] pauses the
 * subscription without leaving it (kept across [stop] and [start]; a record handed out before the pause took effect is handed
 * out again on the resume); the stage-1 marker of every record carries its position; a record inside an open [replay] range
 * (by the event store's [EventStore.compare]) runs stage 1 without its `@DisallowReplay` handlers and never stage 2. The status
 * says PAUSED, the [operationId] holding the processor's lock, `replaying` and each segment's `replay_until`.
 *
 * **Metrics** (P9): each record's final outcome in this processor goes to [metrics] once ([Consumed]; a parked record when
 * the sink took it, so a redelivery after a sink failure counts once, and as a duplicate when its letter was there already),
 * labelled with its type when the processor handles it, else [MessagingMetrics.OTHER]. A stop in the middle of the attempts
 * records nothing: the record comes back.
 */
class EventConsumer(
    private val events: EventStore,
    /** The subscription this processor reads. */
    val spec: SubscriptionSpec,
    private val dispatcher: LocalEventDispatcher,
    private val maxAttempts: Int,
    private val backOffMs: Long = 1000L,
    private val codec: JsonCodec = JsonCodec(),
    /** The processor this consumer runs: `default` or a `@ProcessingGroup` name. */
    val processor: String = Processors.DEFAULT,
    private val deadLetters: DeadLetterSink = DeadLetterSink.Log,
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
    private val resubscribe: () -> Resubscribe = { Resubscribe() },
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(EventConsumer::class.java)
    private val running = AtomicBoolean(false)
    @Volatile private var started = false
    @Volatile private var thread: Thread? = null
    @Volatile private var job: Job? = null
    @Volatile private var pauseRequested = false
    @Volatile private var replayRanges: List<ReplayRange> = emptyList()

    /** The subscription being read; null between subscriptions. [pause]/[resume] and setting it are guarded by [pauseLock]. */
    @Volatile private var current: Subscription? = null
    private val pauseLock = Any()

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
    }

    /** The source this processor reads (the service's events source). */
    val source: String get() = spec.source

    /** The event store's name of this processor's subscription. */
    val subscription: String get() = spec.name

    // --- status (written on the processor thread, read by status()) ---
    @Volatile private var assignedOnce = false
    @Volatile private var rebalancing = false
    private val processedTotal = AtomicLong()
    private val failedTotal = AtomicLong()
    @Volatile private var lastError: String = ""
    @Volatile private var lastErrorAtMs = 0L
    @Volatile private var failingSinceMs = 0L
    @Volatile private var deadLetterCount = 0L
    private val parkedSinceSample = AtomicBoolean(true)

    // --- processor thread only ---
    private val parkedKeys = HashMap<String, MutableSet<String>>() // segment -> keys with a blocking letter
    private val keysUnknown = HashSet<String>() // segments whose keys could not be loaded: every keyed record is confirmed
    private val pending = HashMap<Pair<String, String>, () -> Unit>() // (segment, position) -> what the sink still has to do

    private val store: DeadLetterStore? = (deadLetters as? DeadLetterSink.Collection)?.store

    @Synchronized
    override fun start() {
        if (running.get()) return
        assignedOnce = false
        rebalancing = false
        started = true
        running.set(true)
        val reader = Job()
        job = reader
        thread = Thread({ read(reader) }, "kraken-processor-$processor").apply {
            isDaemon = true
            start()
        }
        log.info(
            "Event processor {} started on {} subscription {} of source {} handling {} event types, dead letters: {}{}",
            processor, events.name, spec.name, spec.source, dispatcher.eventTypes(processor).size, deadLetters.mode.wireName,
            if (pauseRequested) ", paused" else "",
        )
    }

    /**
     * Pauses consumption (P6b): the subscription delivers nothing more without being left (a record handed out before the pause
     * took effect is handed out again on the resume); the record in hand finishes. Kept across [stop] and [start]. Idempotent.
     */
    fun pause() = synchronized(pauseLock) {
        pauseRequested = true
        current?.pause()
    }

    /** Resumes consumption after [pause]. Idempotent. */
    fun resume() = synchronized(pauseLock) {
        pauseRequested = false
        current?.resume()
    }

    /** Whether consumption is paused (or will start paused). */
    val paused: Boolean get() = pauseRequested

    /** The reset operation holding this processor's lock (P6b), reported as `operation_id`; empty when none. */
    @Volatile var operationId: String = ""

    /** Sets the open replay ranges (P6b) from the processor's control document; those of another source are ignored. */
    fun replay(ranges: List<ReplayRange>) {
        replayRanges = ranges.filter { it.source == spec.source }
    }

    /** The open replay ranges of this processor's source. */
    fun replayRanges(): List<ReplayRange> = replayRanges

    /**
     * The open ranges of the assigned segments this instance's subscription is at or past the end of: its committed position
     * (its position while nothing is committed) reached `until`, by the event store's order.
     */
    fun passedRanges(): List<ReplayRange> {
        val segments = current?.segments()?.associateBy { it.segment } ?: return emptyList()
        return replayRanges.filter { range -> segments[range.segment]?.let { passed(it.committed ?: it.position, range.until) } ?: false }
    }

    /** Whether [at] (a position, null unknown) is at or past [until] by the event store's order. */
    private fun passed(at: String?, until: String): Boolean =
        at != null && runCatching { events.compare(at, until) >= 0 }.getOrDefault(false)

    private fun inReplay(record: Delivery): Boolean = replayRanges.any {
        it.segment == record.segment && runCatching {
            events.compare(it.from, record.position) <= 0 && events.compare(record.position, it.until) < 0
        }.getOrDefault(false)
    }

    @Synchronized
    override fun stop() {
        if (!running.compareAndSet(true, false)) return
        job?.cancel()
        val reader = thread
        if (reader != null && reader !== Thread.currentThread()) {
            reader.join(STOP_JOIN_MS)
            if (reader.isAlive) log.warn("Event processor {} did not stop within {} ms: a handler is still running", processor, STOP_JOIN_MS)
        }
        thread = null
        job = null
    }

    override fun isRunning(): Boolean = running.get()
    override fun isAutoStartup(): Boolean = true

    /** Starts after and stops before [OutboxPublisher]; both stop after the web server's graceful drain. */
    override fun getPhase(): Int = PHASE

    // --- the processor thread ---

    private fun read(reader: Job) {
        try {
            runBlocking(reader) {
                resubscribe().run(
                    subscribe = {
                        events.subscribe(spec).also { sub ->
                            synchronized(pauseLock) {
                                current = sub
                                if (pauseRequested) sub.pause()
                            }
                        }
                    },
                    read = { sub ->
                        try {
                            readSubscription(sub)
                        } finally {
                            synchronized(pauseLock) { current = null }
                            forget(parkedKeys.keys.toList() + keysUnknown)
                            rebalancing = true // STARTING until the next subscription's assignment
                        }
                    },
                    failed = { e, wait ->
                        log.warn("Processor {}: the subscription {} failed, subscribing again in {}: {}", processor, spec.name, wait, e.toString())
                        failing("subscription: ${e.message ?: e::class.java.simpleName}")
                    },
                )
            }
        } catch (_: CancellationException) {
            // stopped
        } catch (e: Throwable) {
            log.error("Event processor {} stopped reading: {}", processor, e.toString(), e)
        }
    }

    /** Reads [sub] until the processor stops or [sub] fails. */
    private suspend fun readSubscription(sub: Subscription): Resubscribe.Ended {
        var delivered = false
        while (true) {
            val event = try {
                sub.next()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                sub.takeError()?.let(::failing)
                return Resubscribe.Ended.Failed(e, delivered)
            }
            when (event) {
                is SubscriptionEvent.Assigned -> {
                    val segments = event.segments.sortedWith(SegmentOrder)
                    log.info("Processor {}: segments {} of {} assigned", processor, segments, spec.source)
                    loadKeys(segments)
                    assignedOnce = true
                    rebalancing = false
                }
                is SubscriptionEvent.Revoked -> {
                    val segments = event.segments.sortedWith(SegmentOrder)
                    log.info("Processor {}: segments {} of {} revoked", processor, segments, spec.source)
                    rebalancing = true
                    forget(segments)
                }
                is Delivery -> {
                    delivered = true
                    if (pauseRequested) {
                        // Paused after the subscription handed it out: not handled, handed out again on the resume.
                        sub.redeliver(event.segment, event.position)
                        continue
                    }
                    try {
                        handle(sub, event)
                    } catch (e: Exception) {
                        if (!running.get()) return Resubscribe.Ended.Stopped // stopping: not acknowledged, delivered again later
                        // The sink could not take it: not acknowledged, handed out again after the back-off.
                        sub.redeliver(event.segment, event.position)
                        delay(backOffMs)
                    }
                }
            }
            sub.takeError()?.let(::failing)
        }
    }

    private fun handle(sub: Subscription, record: Delivery) {
        pending[record.segment to record.position]?.let { deliver(sub, record, it); return }
        val headers = record.headers
        val type = headers[EventHeaders.TYPE]
        if (type.isNullOrBlank()) {
            invalid(sub, record, null, "record without type header"); return
        }
        if (!dispatcher.interested(processor, type)) {
            consumed(MessagingMetrics.OTHER, Consumed.SKIPPED)
            advance(sub, record); return
        }
        val id = headers[EventHeaders.ID]?.takeIf { it.isNotBlank() } ?: "${spec.source}-${record.segment}-${record.position}".also {
            log.warn("Record of type {} at {}-{}@{} has no id header, using {} for idempotency", type, spec.source, record.segment, record.position, it)
        }
        val payload = record.payload
        val isObject = try { codec.decodeTree(payload).isObject } catch (_: Exception) { false }
        if (!isObject) {
            invalid(sub, record, id, "payload is not a JSON object", type); return
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
        if (sequenced(sub, record, type, ctx, replay)) return
        attempt(sub, record, id, type, ctx, replay)
    }

    private fun where(record: Delivery) = "${spec.source}-${record.segment}@${record.position}"

    /** Parks [record] SEQUENCED when its key has a live letter (or acknowledges it when its own letter exists); true when done with it. */
    private fun sequenced(sub: Subscription, record: Delivery, type: String, ctx: MessageContext, replay: Boolean): Boolean {
        val sink = deadLetters as? DeadLetterSink.Collection ?: return false
        val store = sink.store
        val key = record.key ?: return false
        if (!dispatcher.hasStage1(processor, type)) return false // only stage 1 is sequenced
        val segment = record.segment
        if (segment in keysUnknown) loadKeys(listOf(segment))
        if (segment !in keysUnknown && parkedKeys[segment]?.contains(key) != true) return false
        val head = try {
            runBlocking { store.head(processor, key) }
        } catch (e: Exception) {
            failing(e, "could not check the dead letters of key at ${where(record)}")
            throw e
        }
        if (head == null) {
            parkedKeys[segment]?.remove(key) // retried, discarded or expired, here or on another instance
            return false
        }
        val own = DeadLetterStore.id(processor, spec.source, segment, record.position)
        if (head == own || runBlocking { store.exists(own) }) {
            log.info("Record {} is already parked in processor {}, acknowledging it", where(record), processor)
            consumed(type, Consumed.DUPLICATE)
            advance(sub, record)
            return true
        }
        // The sagas see it even though stage 1 waits (ADR 0021 ruling 26), unless it is replayed (sagas never replay, P6b).
        if (processor == Processors.DEFAULT && !replay) stage2(record, type, ctx)
        val letter = sink.letters.sequenced(processor, spec.source, record, head, replay)
        log.info("Record {} of type {} parked SEQUENCED in processor {} behind {}", where(record), type, processor, head)
        toSink(sub, record, type) { parked(runBlocking { store.park(letter) }) }
        return true
    }

    /**
     * Stage 2 alone, for a record parked SEQUENCED in `default`, with the attempts and back-off of any record's stage 2: a
     * failure that outlives them is logged and skipped as for any record (counted in `failed_total`), and the record is parked.
     */
    private fun stage2(record: Delivery, type: String, ctx: MessageContext) {
        var attempts = 0
        while (true) {
            attempts++
            try {
                dispatcher.runStage2(type, record.payload, ctx)
                return
            } catch (e: Exception) {
                if (attempts >= maxAttempts) {
                    failedTotal.incrementAndGet()
                    failing(e, null)
                    log.error(
                        "Giving up on the saga stage of sequenced event type={} id={} at {} after {} attempts: {}: {}",
                        type, ctx.messageId, where(record), attempts, e::class.java.name, e.message,
                    )
                    return
                }
                if (!pause(backOffMs)) throw e // stopping: not acknowledged, delivered again after the restart or to the new owner
            }
        }
    }

    private fun attempt(sub: Subscription, record: Delivery, id: String, type: String, ctx: MessageContext, replay: Boolean) {
        val position = RecordPosition(spec.source, record.segment, record.position)
        var attempts = 0
        var failure: Exception
        while (true) {
            attempts++
            try {
                val outcome = dispatcher.dispatchIn(processor, id, type, record.payload, ctx, position, replay)
                processedTotal.incrementAndGet()
                consumed(type, if (outcome == LocalEventDispatcher.Outcome.DUPLICATE) Consumed.DUPLICATE else Consumed.HANDLED)
                advance(sub, record)
                return
            } catch (e: InvalidMessageException) {
                failure = e
                break // retrying cannot change the payload
            } catch (e: Exception) {
                failure = e
            }
            if (attempts >= maxAttempts) break
            if (!pause(backOffMs)) throw failure // stopping: not acknowledged, delivered again after the restart or to the new owner
        }
        exhausted(sub, record, id, type, failure, attempts, replay)
    }

    private fun exhausted(sub: Subscription, record: Delivery, id: String, type: String, failure: Exception, attempts: Int, replay: Boolean) {
        failedTotal.incrementAndGet()
        val cause = if (failure is SagaStageException) failure.cause ?: failure else failure
        val error = DeadLetterError.of(cause)
        failing(cause, null)
        if (failure is SagaStageException) {
            // Stage 1 is done (or had nothing to do); only the sagas could not run. Not a letter: stage 1 has nothing to retry.
            log.error(
                "Giving up on the saga stage of event type={} id={} at {} after {} attempts: {}: {}",
                type, id, where(record), attempts, cause::class.java.name, cause.message,
            )
            consumed(type, Consumed.FAILED)
            advance(sub, record); return
        }
        when (val sink = deadLetters) {
            is DeadLetterSink.Collection -> {
                val letter = sink.letters.letter(processor, spec.source, record, DeadLetterReason.HANDLER_FAILED, error, attempts, replay = replay)
                log.error(
                    "Event type={} id={} at {} failed {} attempts in processor {}, parked as dead letter {}: {}: {}",
                    type, id, where(record), attempts, processor, letter.id, cause::class.java.name, cause.message,
                )
                val key = record.key
                toSink(sub, record, type) {
                    val new = runBlocking { sink.store.park(letter) }
                    if (key != null) parkedKeys.getOrPut(record.segment) { HashSet() } += key
                    parked(new)
                }
            }
            is DeadLetterSink.EventStore -> {
                log.error(
                    "Event type={} id={} at {} failed {} attempts in processor {}, sending it to {}: {}: {}",
                    type, id, where(record), attempts, processor, deadLetterSource(spec.source), cause::class.java.name, cause.message,
                )
                toSink(sub, record, type) { publishDeadLetter(sink, record, DeadLetterReason.HANDLER_FAILED, error); Consumed.PARKED }
            }
            DeadLetterSink.Log -> {
                log.error(
                    "Giving up on event type={} id={} at {} after {} attempts: {}: {}",
                    type, id, where(record), attempts, cause::class.java.name, cause.message,
                )
                consumed(type, Consumed.FAILED)
                advance(sub, record)
            }
        }
    }

    private fun invalid(sub: Subscription, record: Delivery, id: String?, why: String, type: String? = null) {
        val error = DeadLetterError.invalid(why)
        val label = type ?: MessagingMetrics.OTHER
        when (val sink = deadLetters) {
            is DeadLetterSink.Collection -> {
                val letter = sink.letters.letter(processor, spec.source, record, DeadLetterReason.INVALID, error, attempts = 0, replay = inReplay(record))
                log.warn("Record {} of type {} at {} is invalid ({}), parked as dead letter {}", id, type, where(record), why, letter.id)
                toSink(sub, record, label) { parked(runBlocking { sink.store.park(letter) }) }
            }
            is DeadLetterSink.EventStore -> {
                log.warn("Record {} of type {} at {} is invalid ({}), sending it to {}", id, type, where(record), why, deadLetterSource(spec.source))
                toSink(sub, record, label) { publishDeadLetter(sink, record, DeadLetterReason.INVALID, error); Consumed.PARKED }
            }
            DeadLetterSink.Log -> {
                log.warn("Skipping record {} of type {} at {}: {}", id, type, where(record), why)
                consumed(label, Consumed.FAILED)
                advance(sub, record)
            }
        }
    }

    /**
     * Runs [work] (park, publish), counts the record as [type] with the outcome [work] returns and acknowledges; when [work]
     * throws the record stays unacknowledged and is kept, with its count, for the redelivery ([deliver]).
     */
    private fun toSink(sub: Subscription, record: Delivery, type: String, work: () -> Consumed) =
        deliver(sub, record) { consumed(type, work()) }

    /**
     * The outcome of a letter [DeadLetterStore.park] took ([new]) or found there already: the record's own letter exists
     * (parked, then the instance died before the acknowledgement), so this delivery is a duplicate, as the Rust library counts it.
     */
    private fun parked(new: Boolean): Consumed {
        if (!new) return Consumed.DUPLICATE
        parkedSinceSample.set(true)
        return Consumed.PARKED
    }

    private fun deliver(sub: Subscription, record: Delivery, work: () -> Unit) {
        val coords = record.segment to record.position
        pending[coords] = work
        try {
            work()
        } catch (e: Exception) {
            failing(e, "could not hand record ${where(record)} to the dead letters (${deadLetters.mode.wireName}), not acknowledging it")
            throw e
        }
        pending.remove(coords)
        advance(sub, record)
    }

    /** Publishes [record] to the dead-letter source, with its headers and the six `kraken-dlt-*` ones. */
    private fun publishDeadLetter(sink: DeadLetterSink.EventStore, record: Delivery, reason: DeadLetterReason, error: DeadLetterError) {
        val headers = LinkedHashMap<String, String>()
        record.headers.filterKeys { it !in DLT_HEADERS }.forEach { (k, v) -> headers[k] = v }
        headers[DLT_PROCESSOR] = processor
        headers[DLT_REASON] = reason.name
        headers[DLT_ERROR] = DeadLetterError.cut("${error.type}: ${error.message}", DeadLetterError.MAX_MESSAGE)
        headers[DLT_ORIGINAL_TOPIC] = spec.source
        headers[DLT_ORIGINAL_PARTITION] = record.segment
        headers[DLT_ORIGINAL_OFFSET] = record.position
        val event = OutgoingEvent(
            source = deadLetterSource(spec.source),
            id = record.headers[EventHeaders.ID]?.takeIf { it.isNotBlank() } ?: "${spec.source}-${record.segment}-${record.position}",
            typeName = record.headers[EventHeaders.TYPE].orEmpty(),
            key = record.key,
            headers = headers,
            payload = record.payload,
        )
        runBlocking { withTimeout(sink.sendTimeout.toMillis()) { sink.events.publish(event) } }
    }

    private fun consumed(type: String, outcome: Consumed) = metrics.eventConsumed(processor, type, outcome)

    private fun advance(sub: Subscription, record: Delivery) {
        sub.ack(record.segment, record.position)
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
        failing("${e::class.java.simpleName.ifEmpty { e::class.java.name }}: ${e.message ?: ""}")
        if (context != null) log.warn("Processor {}: {}: {}: {}", processor, context, e::class.java.name, e.message)
    }

    /** [error] becomes `last_error`, and the processor is FAILING for [FAILING_WINDOW_MS]. */
    private fun failing(error: String) {
        val now = clock.millis()
        lastError = utf8Cut(error, MAX_LAST_ERROR_BYTES)
        lastErrorAtMs = now
        failingSinceMs = now
    }

    private fun loadKeys(segments: Collection<String>) {
        val store = store ?: return
        if (segments.isEmpty()) return
        try {
            val keys = runBlocking { store.keys(processor, spec.source, segments) }
            for (s in segments) {
                parkedKeys[s] = keys[s].orEmpty().toHashSet()
                keysUnknown.remove(s)
            }
        } catch (e: Exception) {
            keysUnknown += segments // every keyed record of these segments is confirmed until a load succeeds
            log.warn("Processor {}: could not load the keys of its dead letters for segments {}: {}", processor, segments, e.message)
        }
    }

    private fun forget(segments: Collection<String>) {
        for (s in segments) {
            parkedKeys.remove(s)
            keysUnknown.remove(s)
        }
        pending.keys.removeIf { it.first in segments } // the new owner parks it, from the start
    }

    /** This processor's status for the hub (ADR 0022), from in-memory state only: never blocks. */
    fun status(): ProcessorStatus {
        val sub = current
        sub?.takeError()?.let(::failing)
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
            .setSubscription(spec.name)
            .setSource(spec.source)
            .setState(state)
            .setProcessedTotal(processedTotal.get())
            .setFailedTotal(failedTotal.get())
            .setDeadLetters(if (deadLetters.mode == DeadLetterMode.COLLECTION) deadLetterCount else 0L)
            .setLastError(lastError)
            .setLastErrorAtMs(lastErrorAtMs)
            .addAllEventTypes(dispatcher.eventTypes(processor).sorted())
            .setDeadLetterMode(deadLetters.mode.wireName)
            .setOperationId(operationId)
            .setEventStore(utf8Cut(events.name, MAX_NAME_BYTES))
            .addAllCapabilities(events.capabilities.map { utf8Cut(it.wireName, MAX_NAME_BYTES) }.distinct().sorted().take(MAX_CAPABILITIES))
        val ranges = replayRanges
        var replaying = false
        val segments = if (running.get()) sub?.segments().orEmpty() else emptyList()
        for (s in segments.filter { it.segment.toByteArray(Charsets.UTF_8).size <= MAX_SEGMENT_BYTES }.sortedWith(compareBy(SegmentOrder) { it.segment })) {
            // Replaying until this instance's subscription got past the range's end.
            val progress = s.committed ?: s.position
            val until = ranges.filter { it.segment == s.segment && !passed(progress, it.until) }.map { it.until }
                .maxWithOrNull { a, b -> runCatching { events.compare(a, b) }.getOrDefault(0) }.orEmpty()
            if (until.isNotEmpty()) replaying = true
            builder.addSegments(
                SegmentStatus.newBuilder().setSegment(s.segment).setPosition(bounded(s.position)).setCommitted(bounded(s.committed))
                    .setHead(bounded(s.head)).setLag(s.lag?.coerceAtLeast(0) ?: -1L).setReplayUntil(bounded(until)),
            )
        }
        return builder.setReplaying(replaying).build()
    }

    /** Whether this processor was started (also while it is stopping or stopped: it then reports STOPPED). */
    val wasStarted: Boolean get() = started

    /**
     * Samples this processor's letter count when [force]d (every 30 s) or when a letter was parked since the last sample.
     * Blocking: run it off the processor thread. No-op outside mode `collection`.
     */
    fun sampleDeadLetters(force: Boolean) {
        val store = store ?: return
        if (!force && !parkedSinceSample.get()) return
        parkedSinceSample.set(false)
        try {
            deadLetterCount = runBlocking { store.count(processor) }
        } catch (e: Exception) {
            parkedSinceSample.set(true)
            throw e
        }
    }

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1900

        /** A processor reports FAILING this long after a record exhausted its attempts or the subscription reported an error. */
        const val FAILING_WINDOW_MS = 60_000L

        /** `ProcessorStatus.last_error` is cut to this many UTF-8 bytes. */
        const val MAX_LAST_ERROR_BYTES = 512

        /** `ProcessorStatus.event_store` and each capability are cut to this many UTF-8 bytes (hub.proto bounds). */
        const val MAX_NAME_BYTES = 64

        /** The most capabilities a status lists (hub.proto bounds). */
        const val MAX_CAPABILITIES = 16

        /** How long [stop] waits for the record in hand to finish and the subscription to close. */
        private const val STOP_JOIN_MS = 30_000L

        const val DLT_PROCESSOR = "kraken-dlt-processor"
        const val DLT_REASON = "kraken-dlt-reason"
        const val DLT_ERROR = "kraken-dlt-error"

        /** The source the record was read from (on Kafka its topic). */
        const val DLT_ORIGINAL_TOPIC = "kraken-dlt-original-topic"

        /** Its segment (on Kafka its partition). */
        const val DLT_ORIGINAL_PARTITION = "kraken-dlt-original-partition"

        /** Its position (on Kafka its offset). */
        const val DLT_ORIGINAL_OFFSET = "kraken-dlt-original-offset"
        private val DLT_HEADERS = setOf(DLT_PROCESSOR, DLT_REASON, DLT_ERROR, DLT_ORIGINAL_TOPIC, DLT_ORIGINAL_PARTITION, DLT_ORIGINAL_OFFSET)

        /** A position as the status carries it: "" when unknown or longer than [MAX_POSITION_BYTES] (never cut: a cut one is wrong). */
        private fun bounded(position: String?): String =
            if (position == null || position.toByteArray(Charsets.UTF_8).size > MAX_POSITION_BYTES) "" else position

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
