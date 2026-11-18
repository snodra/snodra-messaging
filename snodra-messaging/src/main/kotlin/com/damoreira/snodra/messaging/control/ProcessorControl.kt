package com.damoreira.snodra.messaging.control

import com.damoreira.snodra.server.v1.ControlOk
import com.damoreira.snodra.server.v1.ControlRefusal
import com.damoreira.snodra.server.v1.ControlRefused
import com.damoreira.snodra.server.v1.ControlReply
import com.damoreira.snodra.server.v1.ControlRequest
import com.damoreira.snodra.server.v1.DeadLetterPage
import com.damoreira.snodra.server.v1.DeadLetterRetryAllOutcome
import com.damoreira.snodra.server.v1.DeadLetterRetryOutcome
import com.damoreira.snodra.server.v1.DeadLetterSummary
import com.damoreira.snodra.server.v1.ResetOutcome
import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.deadletter.RetryResult
import com.damoreira.snodra.messaging.events.EventConsumer
import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.eventstore.ResetTarget
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.hub.ControlInbound
import com.damoreira.snodra.messaging.store.ProcessorControlStore
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Control
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Lock
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import com.damoreira.snodra.server.v1.ReplayRange as WireRange
import com.damoreira.snodra.server.v1.ResetTarget as WireTarget

/**
 * The control of this instance's event processors (P6b, ADR 0023): the hub's `ControlRequest`s ([handle]), the local
 * [pause] and [resume], and the convergence of every processor on its `processor_control` document ([ProcessorControlStore]).
 *
 * [com.damoreira.snodra.messaging.events.EventProcessors] hands its consumers to [attach] at start: the documents are read
 * before any consumer polls, and each consumer starts paused, with its open replay ranges, or not at all while an unexpired
 * reset operation holds its lock. Then every [pollInterval] the documents are read again and applied: a pause or resume made
 * through another instance, a lock taken (the consumer leaves its subscription) or cleared (it subscribes again), a lease that passed
 * (the instance clears the operation it held and resumes), and the replay ranges this instance consumed past (removed).
 * A document is applied only when nothing changed the processor here since it was read: a poll that read it before a
 * prepare, pause, resume, execute or finish of this instance answered leaves it to the next poll, which reads what that
 * request wrote (otherwise it would undo the request: start the consumer again under the lock just taken, resume a pause).
 *
 * Requests run off the hub stream, one at a time per processor (a single thread each), except the renewal of the operation
 * this instance holds, which is answered at once, and the dead-letter requests while a reset of the processor executes here:
 * the list is read beside it, a retry, retry-all or discard is refused LOCKED at once (H1, as the Rust library). A prepare or
 * renewal with a lease under [MIN_RESET_LEASE_MS] is INVALID. A reset needs the event store's capability `replay`: without it a
 * prepare or an execute is refused UNSUPPORTED before anything changes (the processor keeps its subscription). A reset executes
 * here only for an operation this instance holds, through the event store's [com.damoreira.snodra.messaging.eventstore.SubscriptionAdmin]:
 * 1. the event store plans where the target moves each segment (positions it does not have are INVALID), 2. nobody may read
 *    the subscription (SUBSCRIBERS_ACTIVE otherwise), 3. a range `[target, before)` per segment the plan moves back (by the
 *    event store's order), extended by a still-open range of the segment, 4. the processor's `@ResetHandler`s run (any range
 *    only), 5. the markers in the ranges are deleted, the ranges written, then the positions moved. While it runs the executor extends its own
 *    lease; right after the reset handlers and again before the commit that extension must succeed (and the execute be within
 *    [executeLeaseLimit]), otherwise it stops with FAILED "lease lost". A refusal after the reset handlers ran says what
 *    changed. Every action is logged at INFO with the control id, processor, action and result.
 *
 * An operation that ended here (finished, aborted, its lease passed or its lock lost) is remembered for
 * max(2 x lease, [FINISHED_KEPT]): a prepare of it is a renewal that came too late and is refused NOT_PREPARED without
 * taking the lock, and a renewal whose lock is gone is refused NOT_PREPARED "lock lost", never turned into a prepare.
 */
class ProcessorControl(
    private val store: ProcessorControlStore,
    private val registry: HandlerRegistry,
    private val deadLetters: DeadLetters,
    private val events: EventStore,
    private val pollInterval: Duration = POLL_INTERVAL,
    /**
     * How long the executor of a reset extends its own lease (the hub's default `SNODRA_SERVER_RESET_TIMEOUT`): an execute that
     * hangs longer lets the lease lapse, so the other instances resume (the hub has given up and its finish waits behind it).
     */
    private val executeLeaseLimit: Duration = Duration.ofSeconds(120),
    private val clock: Clock = Clock.systemUTC(),
) : ControlInbound {
    private val log = LoggerFactory.getLogger(ProcessorControl::class.java)

    /** The operation this instance holds the lock for, with the lease it was asked for. */
    private data class Held(val id: String, val lease: Duration)

    /** One processor: its consumer, its request queue and the operation held for it (guarded by the slot). */
    private class Slot(val consumer: EventConsumer) {
        val queue: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "snodra-control-${consumer.processor}").apply { isDaemon = true } }
        @Volatile var held: Held? = null
        /** The operation whose execute runs on [queue] now; null when none. */
        @Volatile var executing: String? = null
        /** The operations that ended here, with the [System.nanoTime] until which they are remembered. */
        val finished = ConcurrentHashMap<String, Long>()
        /**
         * Counts the changes made to this processor here (by a request, or by applying a document; under the slot's lock): a
         * document read at another count is stale, never applied ([converge]).
         */
        @Volatile var changes = 0L
    }

    /** Remembers that [op] ended on [slot] (see the class docs). */
    private fun ended(slot: Slot, op: String, lease: Duration) {
        val now = System.nanoTime()
        slot.finished.values.removeIf { it - now <= 0 }
        slot.finished[op] = now + minOf(maxOf(FINISHED_KEPT, lease.multipliedBy(2)), FINISHED_KEPT_MAX).toNanos()
    }

    private fun hasEnded(slot: Slot, op: String): Boolean = slot.finished[op]?.let { it - System.nanoTime() > 0 } ?: false

    @Volatile private var slots: Map<String, Slot> = emptyMap()
    private var poller: ScheduledExecutorService? = null
    private var leases: ScheduledExecutorService? = null

    /**
     * Takes over starting [consumers]: reads their documents, starts each as its document says, and polls the documents from
     * then on. When the documents cannot be read no consumer starts (a pause must not be missed); the polls retry.
     */
    @Synchronized
    fun attach(consumers: List<EventConsumer>) {
        val docs = try {
            if (consumers.isEmpty()) emptyMap() else runBlocking { store.read(consumers.map { it.processor }) }
        } catch (e: Exception) {
            log.warn("Could not read the processor control documents: no event processor starts until a poll reads them: {}", e.toString())
            null
        }
        val attached = consumers.associate { it.processor to Slot(it) }
        val now = clock.instant()
        leases = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "snodra-control-lease").apply { isDaemon = true } }
        slots = attached
        try {
            if (docs != null) for (slot in attached.values) converge(slot, docs[slot.consumer.processor], now, readAt = 0L, failFast = true)
        } catch (e: Exception) {
            detach()
            throw e
        }
        poller = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "snodra-control-poll").apply { isDaemon = true } }.also {
            it.scheduleWithFixedDelay(::poll, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    /** Stops polling and taking requests (the consumers are stopped by their owner). */
    @Synchronized
    fun detach() {
        val gone = slots.values
        slots = emptyMap()
        val polling = poller
        poller?.shutdownNow()
        leases?.shutdownNow()
        poller = null
        leases = null
        // A poll in flight must not start a consumer again after its owner stopped it (it checks the slots, emptied above).
        polling?.awaitTermination(5, TimeUnit.SECONDS)
        gone.forEach { it.queue.shutdown() }
        gone.forEach { if (!it.queue.awaitTermination(2, TimeUnit.SECONDS)) it.queue.shutdownNow() }
    }

    /** Pauses [processor] on every instance (the document says so; the others follow within a poll), `pausedBy` `local`. */
    fun pause(processor: String) = setPaused(slot(processor), true, LOCAL)

    /** Resumes [processor] on every instance. */
    fun resume(processor: String) = setPaused(slot(processor), false, LOCAL)

    private fun slot(processor: String): Slot =
        requireNotNull(slots[processor]) { "processor $processor does not run in this instance (no @EventHandler in it)" }

    override fun handle(request: ControlRequest, reply: (ControlReply) -> Unit) {
        val slot = slots[request.processor]
        val answer = { result: ControlReply ->
            log.info("Control {} {} on processor {}: {}", request.controlId, actionOf(request), request.processor, describe(result))
            reply(result)
        }
        if (request.hasResetPrepare() && request.resetPrepare.leaseMs < MIN_RESET_LEASE_MS) {
            answer(refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, BAD_PREPARE))
            return
        }
        if (slot == null) {
            answer(refused(request, ControlRefusal.CONTROL_REFUSAL_UNKNOWN_PROCESSOR, "processor ${request.processor} does not run on this instance"))
            return
        }
        val queued = {
            try {
                slot.queue.execute { answer(guarded(request) { act(slot, request) }) }
            } catch (_: RejectedExecutionException) {
                answer(refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, "the event processors are stopping"))
            }
        }
        val prepare = request.resetPrepare
        val executing = slot.executing
        if (request.hasResetPrepare() && slot.held?.id == prepare.operationId) {
            // A renewal of the operation held here: answered at once, never behind an execute of it (ADR 0023).
            Thread.ofVirtual().name("snodra-control-renew").start { answer(guarded(request) { renew(slot, request) }) }
        } else if (executing != null && request.actionCase == ControlRequest.ActionCase.DEAD_LETTER_LIST) {
            // Dead letters never wait behind an execute (the hub gives a request 10 s): the list only reads (H1, ADR 0023).
            Thread.ofVirtual().name("snodra-control-letters").start { answer(guarded(request) { letters(request) { list(request) } }) }
        } else if (executing != null && request.actionCase in LETTER_CHANGES) {
            // A change beside the execute would race its reset handlers and marker deletion: refused at once.
            answer(
                refused(
                    request, ControlRefusal.CONTROL_REFUSAL_LOCKED,
                    "processor '${request.processor}' is executing reset operation $executing; try again when it finished",
                ),
            )
        } else {
            queued()
        }
    }

    private fun act(slot: Slot, request: ControlRequest): ControlReply = when (request.actionCase) {
        ControlRequest.ActionCase.PAUSE -> { setPaused(slot, true, request.controlId); ok(request, "paused") }
        ControlRequest.ActionCase.RESUME -> { setPaused(slot, false, request.controlId); ok(request, "resumed") }
        ControlRequest.ActionCase.RESET_PREPARE -> prepare(slot, request)
        ControlRequest.ActionCase.RESET_EXECUTE -> execute(slot, request)
        ControlRequest.ActionCase.RESET_FINISH -> finish(slot, request)
        ControlRequest.ActionCase.DEAD_LETTER_LIST -> letters(request) { list(request) }
        ControlRequest.ActionCase.DEAD_LETTER_RETRY -> letters(request) { retry(request) }
        ControlRequest.ActionCase.DEAD_LETTER_RETRY_ALL -> letters(request) {
            val r = deadLetters.retryAll(request.processor)
            reply(request).setRetryAll(
                DeadLetterRetryAllOutcome.newBuilder().setRetried(r.retried.toLong()).setFailed(r.failed.toLong()).setBlocked(r.blocked.toLong()).setSkipped(r.skipped.toLong()),
            ).build()
        }
        ControlRequest.ActionCase.DEAD_LETTER_DISCARD -> letters(request) { discard(request) }
        else -> refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the control request has no action this instance knows")
    }

    /** The refusal of an action the event store cannot do (ADR 0027). */
    private fun unsupported(request: ControlRequest, capability: Capability): ControlReply = refused(
        request, ControlRefusal.CONTROL_REFUSAL_UNSUPPORTED,
        "the event store ${events.name} lacks the capability $capability: processor ${request.processor} cannot be reset",
    )

    private fun setPaused(slot: Slot, paused: Boolean, by: String) {
        runBlocking { store.setPaused(slot.consumer.processor, paused, by) }
        synchronized(slot) {
            if (paused) slot.consumer.pause() else slot.consumer.resume()
            slot.changes++
        }
    }

    // --- reset ---

    /** The renewal of the operation held on [slot]. One whose lock is gone is refused, never turned into a prepare. */
    private fun renew(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetPrepare.operationId
        val lease = Duration.ofMillis(request.resetPrepare.leaseMs)
        val processor = slot.consumer.processor
        if (runBlocking { store.renew(processor, op, lease) }) return ok(request, "lease renewed")
        // Its lease passed and was cleared, or it finished meanwhile: a late renewal must not take the free lock (ADR 0023).
        synchronized(slot) {
            if (slot.held?.id == op) slot.held = null
            slot.changes++
        }
        ended(slot, op, lease)
        return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "lock lost: operation $op no longer holds the lock of processor $processor")
    }

    private fun prepare(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetPrepare.operationId
        val leaseMs = request.resetPrepare.leaseMs
        if (op.isBlank() || leaseMs < MIN_RESET_LEASE_MS) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, BAD_PREPARE)
        if (!events.supports(Capability.REPLAY)) return unsupported(request, Capability.REPLAY)
        if (hasEnded(slot, op)) return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation $op already finished on this instance")
        val lease = Duration.ofMillis(leaseMs)
        val processor = slot.consumer.processor
        return when (val lock = runBlocking { store.lock(processor, op, lease) }) {
            is Lock.Held -> refused(
                request, ControlRefusal.CONTROL_REFUSAL_LOCKED,
                lock.by?.let { "operation ${it.id} holds the lock of processor $processor until ${it.leaseUntil}" } ?: "another operation holds the lock of processor $processor",
            )
            Lock.Taken -> {
                synchronized(slot) {
                    slot.held = Held(op, lease)
                    slot.consumer.operationId = op
                    if (slot.consumer.isRunning) slot.consumer.stop() // finishes the record in hand, acknowledges it, leaves the subscription
                    slot.changes++
                }
                ok(request, "stopped, left the subscription ${slot.consumer.subscription}")
            }
        }
    }

    private fun execute(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetExecute.operationId
        if (!events.supports(Capability.REPLAY)) return unsupported(request, Capability.REPLAY)
        val held = slot.held
        if (held == null || held.id != op) return notPrepared(slot, request, op)
        slot.executing = op
        val period = minOf(LEASE_EXTENSION, held.lease.dividedBy(3)).toMillis()
        val until = System.nanoTime() + executeLeaseLimit.toNanos()
        val extension = leases?.scheduleAtFixedRate(
            {
                if (System.nanoTime() < until) {
                    runCatching { runBlocking { store.renew(slot.consumer.processor, op, held.lease) } }
                        .onFailure { log.debug("Could not extend the lease of reset operation {}: {}", op, it.toString()) }
                }
            },
            period, period, TimeUnit.MILLISECONDS,
        )
        try {
            return reset(slot, request, held, until)
        } finally {
            extension?.cancel(false)
            if (System.nanoTime() < until) runCatching { runBlocking { store.renew(slot.consumer.processor, op, held.lease) } } // a whole lease for the finish
            slot.executing = null
        }
    }

    /** Null when [held]'s lease was extended now and the execute is within [executeLeaseLimit] ([until]); else why not. */
    private fun leaseLost(slot: Slot, held: Held, until: Long): String? {
        if (System.nanoTime() - until >= 0) return "lease lost: the reset ran longer than ${executeLeaseLimit.toSeconds()} s"
        val renewed = try {
            runBlocking { store.renew(slot.consumer.processor, held.id, held.lease) }
        } catch (e: Exception) {
            return "lease lost: cannot extend it: ${e::class.java.simpleName}: ${e.message ?: ""}"
        }
        return if (renewed) null else "lease lost: operation ${held.id} no longer holds the lock of processor ${slot.consumer.processor}"
    }

    /** What an execute changed before it stopped. */
    private class Changed {
        var handlers = 0
        var markers = 0L
        var ranges = false

        /** [reply] with what changed when it is a refusal and anything did: "nothing changed" would be false then. */
        fun told(reply: ControlReply): ControlReply {
            if (!reply.hasRefused() || (handlers == 0 && markers == 0L && !ranges)) return reply
            val deleted = if (markers == 0L) "no markers deleted" else "$markers markers deleted"
            val written = if (ranges) ", replay ranges written" else ""
            val message = "${EventConsumer.utf8Cut(reply.refused.message, MAX_CAUSE_BYTES)}; reset handlers ran ($handlers), $deleted$written, positions not moved: retry the reset"
            return reply.toBuilder().setRefused(reply.refused.toBuilder().setMessage(EventConsumer.utf8Cut(message, MAX_TEXT_BYTES))).build()
        }
    }

    private fun reset(slot: Slot, request: ControlRequest, held: Held, until: Long): ControlReply {
        val op = held.id
        val consumer = slot.consumer
        val processor = consumer.processor
        val spec = consumer.spec
        val wire = request.resetExecute.target
        val target = when (wire.toCase) {
            WireTarget.ToCase.EARLIEST -> ResetTarget.Earliest
            WireTarget.ToCase.LATEST -> ResetTarget.Latest
            WireTarget.ToCase.TIMESTAMP_MS -> ResetTarget.Timestamp(wire.timestampMs)
            WireTarget.ToCase.POSITIONS -> {
                val listed = wire.positions.positionsList
                if (listed.isEmpty()) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the positions target lists no segment")
                val positions = listed.associate { it.segment to it.position }
                if (positions.size != listed.size) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the positions target names a segment twice")
                ResetTarget.Positions(positions)
            }
            else -> return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the reset has no target")
        }
        val compare = Comparator<String> { a, b -> events.compare(a, b) }
        val admin = try {
            events.admin(spec)
        } catch (e: EventStoreException) {
            return storeRefusal(request, e)
        }
        // 1. Where the target moves each segment, and 2. nobody reads the subscription.
        val (plan, members) = try {
            runBlocking { admin.plan(target) to admin.members() }
        } catch (e: EventStoreException) {
            return storeRefusal(request, e)
        }
        if (members.isNotEmpty()) {
            return refused(
                request, ControlRefusal.CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE,
                "subscription ${spec.name} still has ${members.size} subscriber(s): ${members.joinToString()}",
            )
        }
        // 3. The ranges that replay, extended by the still-open ones of their segments (their markers are gone).
        val open = runBlocking { store.get(processor) }?.replay.orEmpty()
        val ranges = ResetRanges.replaying(plan, target, open, compare)
        // 4. The reset handlers, before any marker or position changes.
        val done = Changed()
        if (ranges.isNotEmpty()) {
            val ctx = ResetContext(processor, op, target, ranges)
            for (handler in registry.resetHandlers(processor)) {
                done.handlers++
                try {
                    runBlocking { handler.invoke(ctx) }
                } catch (e: VirtualMachineError) {
                    throw e
                } catch (e: Throwable) {
                    log.warn("Reset handler {} of processor {} failed, the reset {} stops before the markers: {}", handler.label, processor, op, e.toString())
                    return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_RESET_HANDLER_FAILED, "${handler.label} failed: ${e::class.java.simpleName}: ${e.message ?: ""}"))
                }
            }
        }
        // 5. Markers and ranges first, then the positions: a crash in between only re-runs what the next attempt re-runs. The
        //    lease is checked before each: lost, another instance may consume or reset already.
        val replay: List<ReplayRange>
        try {
            leaseLost(slot, held, until)?.let { return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, it)) }
            if (ranges.isNotEmpty()) done.markers = runBlocking { store.deleteMarkers(processor, ranges) }
            replay = ResetRanges.merged(open, ranges, compare)
            if (ranges.isNotEmpty()) runBlocking { store.setReplay(processor, replay) }
            done.ranges = ranges.isNotEmpty()
            leaseLost(slot, held, until)?.let { return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, it)) }
            try {
                runBlocking { admin.apply(plan) }
            } catch (e: EventStoreException) {
                return done.told(storeRefusal(request, e))
            }
        } catch (e: Exception) {
            log.warn("Reset {} of processor {} failed after its reset handlers: {}", op, processor, e.toString())
            return done.told(failed(request, e))
        }
        val deleted = done.markers
        val handlers = done.handlers
        synchronized(slot) {
            consumer.replay(replay)
            slot.changes++
        }
        log.info(
            "Processor {} reset to {} by operation {}: positions {}, replay {}, {} markers deleted, {} reset handlers",
            processor, target, op, plan.map { "${it.segment}@${it.from}" }, ranges, deleted, handlers,
        )
        return reply(request).setReset(
            ResetOutcome.newBuilder()
                .addAllRanges(ranges.map { WireRange.newBuilder().setSegment(it.segment).setFrom(it.from).setUntil(it.until).build() })
                .setMarkersDeleted(deleted)
                .setResetHandlers(handlers),
        ).build()
    }

    /** The refusal of what the event store refused or failed at. */
    private fun storeRefusal(request: ControlRequest, e: EventStoreException): ControlReply = when (e) {
        is EventStoreException.Invalid -> refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, e.message ?: "invalid")
        is EventStoreException.SubscribersActive -> refused(request, ControlRefusal.CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE, e.message ?: "subscribers active")
        is EventStoreException.Unsupported -> unsupported(request, e.capability)
        is EventStoreException.Rejected, is EventStoreException.Failed -> {
            log.warn("Reset of processor {}: the event store failed: {}", request.processor, e.toString())
            refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, e.message ?: e::class.java.simpleName)
        }
    }

    private fun finish(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetFinish.operationId
        val processor = slot.consumer.processor
        ended(slot, op, slot.held?.takeIf { it.id == op }?.lease ?: Duration.ZERO)
        if (slot.held?.id == op) {
            runBlocking { store.release(processor, op) }
            synchronized(slot) {
                slot.held = null
                slot.changes++
            }
            converge(slot)
            return ok(request, "released, consuming again")
        }
        val current = runBlocking { store.get(processor) }?.operation
        return when {
            current == null || current.expired(clock.instant()) -> { converge(slot); ok(request, "no operation holds the lock") }
            current.id == op -> { runBlocking { store.release(processor, op) }; converge(slot); ok(request, "released, consuming again") }
            else -> refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation ${current.id} holds the lock of processor $processor, not $op")
        }
    }

    private fun notPrepared(slot: Slot, request: ControlRequest, op: String): ControlReply {
        val current = runBlocking { store.get(slot.consumer.processor) }?.operation?.takeIf { !it.expired(clock.instant()) }
        val holder = current?.let { "; operation ${it.id} holds the lock" }.orEmpty()
        return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation $op is not prepared on this instance$holder")
    }

    // --- convergence ---

    private fun poll() {
        val current = slots
        if (current.isEmpty()) return
        val readAt = current.mapValues { it.value.changes }
        val docs = try {
            runBlocking { store.read(current.keys) }
        } catch (e: Exception) {
            log.warn("Could not read the processor control documents, keeping the processors as they are: {}", e.toString())
            return
        }
        val now = clock.instant()
        for (slot in current.values) {
            try {
                converge(slot, docs[slot.consumer.processor], now, readAt.getValue(slot.consumer.processor))
                removePassed(slot)
            } catch (e: Exception) {
                log.warn("Could not apply the control document of processor {}: {}", slot.consumer.processor, e.toString())
            }
        }
    }

    private fun converge(slot: Slot) {
        val readAt = slot.changes
        converge(slot, runBlocking { store.get(slot.consumer.processor) }, clock.instant(), readAt)
    }

    /**
     * Applies [doc] to [slot]'s consumer: replay ranges, pause, and whether it runs (not while an unexpired operation holds the
     * lock, its own included). A lease of the operation held here that passed is cleared (conditional on its id) and the
     * consumer resumes. [readAt]: [Slot.changes] before [doc] was read; a change made here since then means [doc] may predate
     * it, and it is left to the next read. [failFast]: a consumer that cannot start throws (at start) instead of waiting for
     * the next poll.
     */
    private fun converge(slot: Slot, doc: Control?, now: Instant, readAt: Long, failFast: Boolean = false) = synchronized(slot) {
        val consumer = slot.consumer
        val processor = consumer.processor
        if (slots[processor] !== slot) return@synchronized // detached: the consumer is its owner's to stop, never to start again
        if (slot.changes != readAt) return@synchronized // read before a change made here: it would undo it
        slot.changes++
        var operation = doc?.operation
        val held = slot.held
        if (held != null) {
            if (operation == null || operation.id != held.id) {
                slot.held = null // released (another instance's finish) or taken over after it expired
                ended(slot, held.id, held.lease)
            } else if (operation.expired(now) && slot.executing == null) {
                if (!runBlocking { store.release(processor, held.id, expiredOnly = true) }) return@synchronized // renewed meanwhile: next poll
                log.warn("The lease of reset operation {} on processor {} passed without a renewal: released it, consuming again", held.id, processor)
                slot.held = null
                ended(slot, held.id, held.lease)
                operation = null
            }
        }
        val foreign = operation?.takeIf { !it.expired(now) && it.id != slot.held?.id }
        consumer.replay(doc?.replay.orEmpty())
        val paused = doc?.paused ?: false
        if (paused != consumer.paused) {
            if (paused) consumer.pause() else consumer.resume()
            log.info("Processor {} {} (processor_control)", processor, if (paused) "paused" else "resumed")
        }
        val announced = consumer.operationId
        consumer.operationId = slot.held?.id ?: foreign?.id ?: ""
        val run = slot.held == null && foreign == null
        if (run && !consumer.isRunning) {
            try {
                consumer.start(waitForPrevious = false) // never blocks the poll on a reader that outlived its stop
            } catch (e: Exception) {
                if (failFast) throw e
                log.warn("Could not start processor {}, retrying at the next poll: {}", processor, e.toString())
            }
        } else if (!run && consumer.isRunning) {
            log.info("Processor {} stops: reset operation {} holds its lock", processor, foreign?.id)
            consumer.stop()
        } else if (!run && foreign != null && announced != foreign.id) {
            log.info("Processor {} stays stopped: reset operation {} holds its lock until {}", processor, foreign.id, foreign.leaseUntil)
        }
    }

    /** Removes the replay ranges this instance's subscription is past the end of from the document. */
    private fun removePassed(slot: Slot) {
        val consumer = slot.consumer
        for (range in consumer.passedRanges()) {
            if (runBlocking { store.removeReplay(consumer.processor, range) }) {
                log.info("Processor {} replayed segment {} of {} up to position {}", consumer.processor, range.segment, range.source, range.until)
            }
            consumer.replay(consumer.replayRanges() - range)
        }
    }

    // --- dead letters ---

    /** Runs a dead-letter call: mode not `collection` is INVALID, a processor that does not run here UNKNOWN_PROCESSOR. */
    private fun letters(request: ControlRequest, call: () -> ControlReply): ControlReply = try {
        call()
    } catch (e: IllegalStateException) {
        refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, e.message ?: "dead letters are not kept")
    } catch (e: IllegalArgumentException) {
        refused(request, ControlRefusal.CONTROL_REFUSAL_UNKNOWN_PROCESSOR, e.message ?: "processor ${request.processor} does not run here")
    }

    private fun list(request: ControlRequest): ControlReply {
        val limit = request.deadLetterList.limit
        if (limit !in 1..MAX_LIST) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "limit must be 1 to $MAX_LIST, was $limit")
        val letters = deadLetters.list(request.processor, limit)
        val total = deadLetters.count(request.processor)
        return reply(request).setDeadLetters(DeadLetterPage.newBuilder().addAllLetters(letters.map(::summary)).setTotal(total)).build()
    }

    /** The letter [id] of the request's processor; null when there is none (or it belongs to another processor). */
    private fun letterOf(request: ControlRequest, id: String): DeadLetter? =
        try {
            deadLetters.get(id)?.takeIf { it.processor == request.processor }
        } catch (_: IllegalArgumentException) {
            null // a letter of a processor that does not run here
        }

    private fun retry(request: ControlRequest): ControlReply {
        val id = request.deadLetterRetry.letterId
        if (id.isBlank()) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "no letter id")
        val result = if (letterOf(request, id) == null) RetryResult.NotFound else deadLetters.retry(id)
        val (name, detail) = when (result) {
            RetryResult.Retried -> "Retried" to ""
            RetryResult.Duplicate -> "Duplicate" to ""
            is RetryResult.Failed -> "Failed" to result.message
            is RetryResult.Blocked -> "Blocked" to result.headId
            RetryResult.NotRetriable -> "NotRetriable" to ""
            RetryResult.NotFound -> "NotFound" to ""
        }
        return reply(request).setRetry(DeadLetterRetryOutcome.newBuilder().setResult(name).setDetail(cut(detail))).build()
    }

    private fun discard(request: ControlRequest): ControlReply {
        val id = request.deadLetterDiscard.letterId
        if (id.isBlank()) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "no letter id")
        if (letterOf(request, id) == null || !deadLetters.discard(id)) {
            return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_FOUND, "no dead letter $id in processor ${request.processor}")
        }
        return ok(request, "discarded")
    }

    private fun summary(letter: DeadLetter): DeadLetterSummary = DeadLetterSummary.newBuilder()
        .setId(letter.id)
        .setProcessor(letter.processor)
        .setSource(letter.source)
        .setSegment(letter.segment)
        .setPosition(letter.position)
        .setKey(letter.key.orEmpty())
        .setEventId(letter.eventId.orEmpty())
        .setEventType(letter.eventType.orEmpty())
        .setReason(letter.reason.name)
        .setErrorType(letter.error.type)
        .setErrorMessage(cut(letter.error.message))
        .setBlockedBy(letter.blockedBy.orEmpty())
        .setAttempts(letter.attempts)
        .setParkedAtMs(letter.parkedAt.toEpochMilli())
        .setLastAttemptAtMs(letter.lastAttemptAt.toEpochMilli())
        .build()

    // --- replies ---

    /** [work]'s reply; FAILED when it throws, an [Error] included (a `TODO()`), so the hub always gets an answer. */
    private fun guarded(request: ControlRequest, work: () -> ControlReply): ControlReply = try {
        work()
    } catch (e: VirtualMachineError) {
        throw e
    } catch (e: Throwable) {
        log.warn("Control {} on processor {} failed: {}", request.controlId, request.processor, e.toString())
        failed(request, e)
    }

    private fun failed(request: ControlRequest, e: Throwable): ControlReply {
        val cause = if (e is ExecutionException) e.cause ?: e else e
        return refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, "${cause::class.java.simpleName}: ${cause.message ?: ""}")
    }

    private fun reply(request: ControlRequest): ControlReply.Builder = ControlReply.newBuilder().setControlId(request.controlId)

    private fun ok(request: ControlRequest, detail: String): ControlReply = reply(request).setOk(ControlOk.newBuilder().setDetail(detail)).build()

    private fun refused(request: ControlRequest, code: ControlRefusal, message: String): ControlReply =
        reply(request).setRefused(ControlRefused.newBuilder().setCode(code).setMessage(cut(message))).build()

    private fun cut(text: String) = EventConsumer.utf8Cut(text, MAX_TEXT_BYTES)

    private fun actionOf(request: ControlRequest): String = request.actionCase.name.lowercase()

    private fun describe(reply: ControlReply): String = when (reply.resultCase) {
        ControlReply.ResultCase.OK -> "ok (${reply.ok.detail})"
        ControlReply.ResultCase.REFUSED -> "refused ${reply.refused.code.name.removePrefix("CONTROL_REFUSAL_")}: ${reply.refused.message}"
        ControlReply.ResultCase.RESET -> "reset (${reply.reset.rangesCount} ranges, ${reply.reset.markersDeleted} markers deleted, ${reply.reset.resetHandlers} reset handlers)"
        ControlReply.ResultCase.DEAD_LETTERS -> "${reply.deadLetters.lettersCount} of ${reply.deadLetters.total} letters"
        ControlReply.ResultCase.RETRY -> reply.retry.result
        ControlReply.ResultCase.RETRY_ALL -> reply.retryAll.let { "${it.retried} retried, ${it.failed} failed, ${it.blocked} blocked, ${it.skipped} skipped" }
        else -> reply.resultCase.name.lowercase()
    }

    companion object {
        /** How often the `processor_control` documents are read (P6b). */
        val POLL_INTERVAL: Duration = Duration.ofSeconds(5)

        /** The executor of a reset extends its lease at least this often (at most a third of the lease) while it runs. */
        val LEASE_EXTENSION: Duration = Duration.ofSeconds(10)

        /** `pausedBy` of a pause made through [pause] / [resume] on this instance. */
        const val LOCAL = "local"

        /** The shortest lease a `ResetPrepare` may ask for (the hub asks for 30 s): a shorter one would lapse before anything happened. */
        const val MIN_RESET_LEASE_MS = 1000L

        /** The refusal of a prepare without an operation id or with a lease under [MIN_RESET_LEASE_MS] (the Rust library's text). */
        private const val BAD_PREPARE = "a prepare needs an operation id and a lease of at least 1000 ms"

        /** The dead-letter requests that change letters: refused while a reset of the processor executes here. */
        private val LETTER_CHANGES = setOf(
            ControlRequest.ActionCase.DEAD_LETTER_RETRY, ControlRequest.ActionCase.DEAD_LETTER_RETRY_ALL, ControlRequest.ActionCase.DEAD_LETTER_DISCARD,
        )

        /** The most letters a `DeadLetterList` may ask for. */
        const val MAX_LIST = 100

        /** Refusal messages and retry details are cut to this many UTF-8 bytes. */
        const val MAX_TEXT_BYTES = 512

        /** The most bytes of the cause in a refusal that also says what the execute changed. */
        private const val MAX_CAUSE_BYTES = 300

        /** For how long at least an operation that ended here is remembered (twice its lease when that is longer). */
        val FINISHED_KEPT: Duration = Duration.ofSeconds(60)

        /** ... and at most this long (a lease of years would overflow the clock). */
        private val FINISHED_KEPT_MAX: Duration = Duration.ofDays(1)
    }
}
