package io.damoreira.kraken.messaging.control

import io.damoreira.kraken.hub.v1.ControlOk
import io.damoreira.kraken.hub.v1.ControlRefusal
import io.damoreira.kraken.hub.v1.ControlRefused
import io.damoreira.kraken.hub.v1.ControlReply
import io.damoreira.kraken.hub.v1.ControlRequest
import io.damoreira.kraken.hub.v1.DeadLetterPage
import io.damoreira.kraken.hub.v1.DeadLetterRetryAllOutcome
import io.damoreira.kraken.hub.v1.DeadLetterRetryOutcome
import io.damoreira.kraken.hub.v1.DeadLetterSummary
import io.damoreira.kraken.hub.v1.ResetOutcome
import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.deadletter.RetryResult
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.ControlInbound
import io.damoreira.kraken.messaging.kafka.EventConsumer
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.UnknownMemberIdException
import io.damoreira.kraken.hub.v1.ResetTarget as WireTarget
import io.damoreira.kraken.hub.v1.ReplayRange as WireRange
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

/**
 * The control of this instance's event processors (P6b, ADR 0023): the hub's `ControlRequest`s ([handle]), the local
 * [pause] and [resume], and the convergence of every processor on its `processor_control` document ([ProcessorControlStore]).
 *
 * [io.damoreira.kraken.messaging.kafka.EventProcessors] hands its consumers to [attach] at start: the documents are read
 * before any consumer polls, and each consumer starts paused, with its open replay ranges, or not at all while an unexpired
 * reset operation holds its lock. Then every [pollInterval] the documents are read again and applied: a pause or resume made
 * through another instance, a lock taken (the consumer leaves the group) or cleared (it joins again), a lease that passed
 * (the instance clears the operation it held and resumes), and the replay ranges this instance consumed past (removed).
 *
 * Requests run off the hub stream, one at a time per processor (a single thread each), except the renewal of the operation
 * this instance holds, which is answered at once. A reset executes here only for an operation this instance holds:
 * 1. the target becomes per-partition offsets (`Admin.listOffsets`: earliest, latest, for a timestamp; explicit offsets are
 *    checked against the partitions and watermarks), 2. the group's committed offsets are read, a range `[target, before)`
 *    per partition where the target is lower, 3. the group must be empty (`describeConsumerGroups`), 4. the processor's
 *    `@ResetHandler`s run (any range only), 5. the markers in the ranges are deleted (legacy ones too for `earliest`), the
 *    ranges written, then the offsets committed (`alterConsumerGroupOffsets`). While it runs the executor extends its own
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
    private val adminConfig: Map<String, Any>,
    private val pollInterval: Duration = POLL_INTERVAL,
    private val adminTimeout: Duration = Duration.ofSeconds(10),
    /**
     * How long the executor of a reset extends its own lease (the hub's default `KRAKEN_HUB_RESET_TIMEOUT`): an execute that
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
        val queue: ExecutorService = Executors.newSingleThreadExecutor { r -> Thread(r, "kraken-control-${consumer.processor}").apply { isDaemon = true } }
        @Volatile var held: Held? = null
        @Volatile var executing = false
        /** The operations that ended here, with the [System.nanoTime] until which they are remembered. */
        val finished = ConcurrentHashMap<String, Long>()
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
            if (consumers.isEmpty()) emptyMap() else store.read(consumers.map { it.processor })
        } catch (e: Exception) {
            log.warn("Could not read the processor control documents: no event processor starts until a poll reads them: {}", e.toString())
            null
        }
        val attached = consumers.associate { it.processor to Slot(it) }
        val now = clock.instant()
        leases = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "kraken-control-lease").apply { isDaemon = true } }
        slots = attached
        try {
            if (docs != null) for (slot in attached.values) converge(slot, docs[slot.consumer.processor], now, failFast = true)
        } catch (e: Exception) {
            detach()
            throw e
        }
        poller = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "kraken-control-poll").apply { isDaemon = true } }.also {
            it.scheduleWithFixedDelay(::poll, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    /** Stops polling and taking requests (the consumers are stopped by their owner). */
    @Synchronized
    fun detach() {
        val gone = slots.values
        slots = emptyMap()
        poller?.shutdownNow()
        leases?.shutdownNow()
        poller = null
        leases = null
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
        if (request.hasResetPrepare() && prepare.leaseMs > 0 && slot.held?.id == prepare.operationId) {
            // A renewal of the operation held here: answered at once, never behind an execute of it (ADR 0023).
            Thread.ofVirtual().name("kraken-control-renew").start { answer(guarded(request) { renew(slot, request) }) }
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

    private fun setPaused(slot: Slot, paused: Boolean, by: String) {
        store.setPaused(slot.consumer.processor, paused, by)
        synchronized(slot) { if (paused) slot.consumer.pause() else slot.consumer.resume() }
    }

    // --- reset ---

    /** The renewal of the operation held on [slot]. One whose lock is gone is refused, never turned into a prepare. */
    private fun renew(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetPrepare.operationId
        val lease = Duration.ofMillis(request.resetPrepare.leaseMs)
        val processor = slot.consumer.processor
        if (store.renew(processor, op, lease)) return ok(request, "lease renewed")
        // Its lease passed and was cleared, or it finished meanwhile: a late renewal must not take the free lock (ADR 0023).
        synchronized(slot) { if (slot.held?.id == op) slot.held = null }
        ended(slot, op, lease)
        return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "lock lost: operation $op no longer holds the lock of processor $processor")
    }

    private fun prepare(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetPrepare.operationId
        val leaseMs = request.resetPrepare.leaseMs
        if (op.isBlank() || leaseMs <= 0) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "a prepare needs an operation id and a positive lease")
        if (hasEnded(slot, op)) return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation $op already finished on this instance")
        val lease = Duration.ofMillis(leaseMs)
        val processor = slot.consumer.processor
        return when (val lock = store.lock(processor, op, lease)) {
            is ProcessorControlStore.Lock.Held -> refused(
                request, ControlRefusal.CONTROL_REFUSAL_LOCKED,
                lock.by?.let { "operation ${it.id} holds the lock of processor $processor until ${it.leaseUntil}" } ?: "another operation holds the lock of processor $processor",
            )
            ProcessorControlStore.Lock.Taken -> {
                synchronized(slot) {
                    slot.held = Held(op, lease)
                    slot.consumer.operationId = op
                    if (slot.consumer.isRunning) slot.consumer.stop() // finishes the record in hand, commits it, leaves the group
                }
                ok(request, "stopped, left consumer group ${slot.consumer.group}")
            }
        }
    }

    private fun execute(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetExecute.operationId
        val held = slot.held
        if (held == null || held.id != op) return notPrepared(slot, request, op)
        slot.executing = true
        val period = minOf(LEASE_EXTENSION, held.lease.dividedBy(3)).toMillis().coerceAtLeast(50)
        val until = System.nanoTime() + executeLeaseLimit.toNanos()
        val extension = leases?.scheduleAtFixedRate(
            {
                if (System.nanoTime() < until) {
                    runCatching { store.renew(slot.consumer.processor, op, held.lease) }
                        .onFailure { log.debug("Could not extend the lease of reset operation {}: {}", op, it.toString()) }
                }
            },
            period, period, TimeUnit.MILLISECONDS,
        )
        try {
            return reset(slot, request, held, until)
        } finally {
            extension?.cancel(false)
            if (System.nanoTime() < until) runCatching { store.renew(slot.consumer.processor, op, held.lease) } // a whole lease for the finish
            slot.executing = false
        }
    }

    /** Null when [held]'s lease was extended now and the execute is within [executeLeaseLimit] ([until]); else why not. */
    private fun leaseLost(slot: Slot, held: Held, until: Long): String? {
        if (System.nanoTime() - until >= 0) return "lease lost: the reset ran longer than ${executeLeaseLimit.toSeconds()} s"
        val renewed = try {
            store.renew(slot.consumer.processor, held.id, held.lease)
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
            val message = "${EventConsumer.utf8Cut(reply.refused.message, MAX_CAUSE_BYTES)}; reset handlers ran ($handlers), $deleted$written, offsets not moved: retry the reset"
            return reply.toBuilder().setRefused(reply.refused.toBuilder().setMessage(EventConsumer.utf8Cut(message, MAX_TEXT_BYTES))).build()
        }
    }

    private fun reset(slot: Slot, request: ControlRequest, held: Held, until: Long): ControlReply {
        val op = held.id
        val consumer = slot.consumer
        val processor = consumer.processor
        val topic = consumer.topic
        val group = consumer.group
        val wire = request.resetExecute.target
        val target = when (wire.toCase) {
            WireTarget.ToCase.EARLIEST -> ResetTarget.Earliest
            WireTarget.ToCase.LATEST -> ResetTarget.Latest
            WireTarget.ToCase.TIMESTAMP_MS -> ResetTarget.Timestamp(wire.timestampMs)
            WireTarget.ToCase.OFFSETS -> ResetTarget.Offsets(wire.offsets.offsetsList.associate { it.partition to it.offset })
            else -> return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the reset has no target")
        }
        val timeout = adminTimeout.toMillis()
        Admin.create(adminConfig.filterKeys { it in AdminClientConfig.configNames() }).use { admin ->
            val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get(timeout, TimeUnit.MILLISECONDS).getValue(topic)
                .partitions().map { TopicPartition(topic, it.partition()) }.sortedBy { it.partition() }
            fun offsets(spec: (TopicPartition) -> OffsetSpec): Map<TopicPartition, Long> =
                admin.listOffsets(partitions.associateWith(spec)).all().get(timeout, TimeUnit.MILLISECONDS).mapValues { it.value.offset() }
            // 1. The target per partition.
            val targets: Map<TopicPartition, Long> = when (target) {
                ResetTarget.Earliest -> offsets { OffsetSpec.earliest() }
                ResetTarget.Latest -> offsets { OffsetSpec.latest() }
                is ResetTarget.Timestamp -> {
                    val high = offsets { OffsetSpec.latest() }
                    val found = offsets { OffsetSpec.forTimestamp(target.timestampMs) }
                    partitions.associateWith { tp -> found[tp]?.takeIf { it >= 0 } ?: high.getValue(tp) } // no record that late: the end
                }
                is ResetTarget.Offsets -> {
                    if (wire.offsets.offsetsCount == 0) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the offsets target lists no partition")
                    if (target.offsets.size != wire.offsets.offsetsCount) {
                        return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "the offsets target names a partition twice")
                    }
                    val high = offsets { OffsetSpec.latest() }
                    val known = partitions.associateBy { it.partition() }
                    for ((p, o) in target.offsets) {
                        val tp = known[p] ?: return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "topic $topic has no partition $p")
                        if (o < 0) return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "offset $o of partition $p is negative")
                        if (o > high.getValue(tp)) {
                            return refused(request, ControlRefusal.CONTROL_REFUSAL_INVALID, "offset $o of partition $p is past its high watermark ${high.getValue(tp)}")
                        }
                    }
                    target.offsets.mapKeys { known.getValue(it.key) }
                }
            }
            // 2. Where the group is (a group that never existed has no offsets and no members).
            val before = orNoGroup(emptyMap()) {
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(timeout, TimeUnit.MILLISECONDS)
            }
            // 3. Nobody consumes.
            val members = orNoGroup(emptyList()) {
                admin.describeConsumerGroups(listOf(group)).all().get(timeout, TimeUnit.MILLISECONDS)[group]?.members().orEmpty().toList()
            }
            if (members.isNotEmpty()) {
                return refused(
                    request, ControlRefusal.CONTROL_REFUSAL_GROUP_NOT_EMPTY,
                    "consumer group $group has ${members.size} active member(s): ${members.joinToString { it.clientId() }}",
                )
            }
            val doc = store.get(processor)
            val open = doc?.replay.orEmpty()
            val ranges = if (target == ResetTarget.Latest) emptyList() else targets.mapNotNull { (tp, to) ->
                val from = before[tp]?.offset() ?: return@mapNotNull null // nothing known handled there
                if (to >= from) return@mapNotNull null
                // An open range of the partition is not replayed past `before` yet (its markers are gone): it is kept in.
                val still = open.filter { it.topic == topic && it.partition == tp.partition() }.maxOfOrNull { it.until } ?: 0L
                ReplayRange(topic, tp.partition(), to, maxOf(from, still))
            }.sortedBy { it.partition }
            // 4. The reset handlers, before any marker or offset changes.
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
            // 5. Markers and ranges first, then the offsets: a crash in between only re-runs what the next attempt re-runs. The
            //    lease is checked before each: lost, another instance may consume or reset already.
            val replay: List<ReplayRange>
            try {
                leaseLost(slot, held, until)?.let { return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, it)) }
                if (ranges.isNotEmpty()) done.markers = store.deleteMarkers(processor, ranges, legacy = target == ResetTarget.Earliest)
                replay = open.filter { o -> ranges.none { it.topic == o.topic && it.partition == o.partition } } + ranges
                if (ranges.isNotEmpty()) store.setReplay(processor, replay)
                done.ranges = ranges.isNotEmpty()
                leaseLost(slot, held, until)?.let { return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_FAILED, it)) }
                try {
                    admin.alterConsumerGroupOffsets(group, targets.mapValues { OffsetAndMetadata(it.value) }).all().get(timeout, TimeUnit.MILLISECONDS)
                } catch (e: ExecutionException) {
                    val cause = e.cause
                    if (cause is UnknownMemberIdException || cause is GroupNotEmptyException || cause is RebalanceInProgressException) {
                        return done.told(refused(request, ControlRefusal.CONTROL_REFUSAL_GROUP_NOT_EMPTY, "Kafka refused to move the offsets of group $group: ${cause.message}"))
                    }
                    throw e
                }
            } catch (e: Exception) {
                log.warn("Reset {} of processor {} failed after its reset handlers: {}", op, processor, e.toString())
                return done.told(failed(request, e))
            }
            val deleted = done.markers
            val handlers = done.handlers
            consumer.replay(replay)
            log.info(
                "Processor {} reset to {} by operation {}: offsets {}, replay {}, {} markers deleted, {} reset handlers",
                processor, target, op, targets.mapKeys { it.key.partition() }, ranges, deleted, handlers,
            )
            return reply(request).setReset(
                ResetOutcome.newBuilder()
                    .addAllRanges(ranges.map { WireRange.newBuilder().setPartition(it.partition).setFrom(it.from).setUntil(it.until).build() })
                    .setMarkersDeleted(deleted)
                    .setResetHandlers(handlers),
            ).build()
        }
    }

    private fun <T> orNoGroup(none: T, read: () -> T): T = try {
        read()
    } catch (e: ExecutionException) {
        if (e.cause !is GroupIdNotFoundException) throw e
        none
    }

    private fun finish(slot: Slot, request: ControlRequest): ControlReply {
        val op = request.resetFinish.operationId
        val processor = slot.consumer.processor
        ended(slot, op, slot.held?.takeIf { it.id == op }?.lease ?: Duration.ZERO)
        if (slot.held?.id == op) {
            store.release(processor, op)
            synchronized(slot) { slot.held = null }
            converge(slot)
            return ok(request, "released, consuming again")
        }
        val current = store.get(processor)?.operation
        return when {
            current == null || current.expired(clock.instant()) -> { converge(slot); ok(request, "no operation holds the lock") }
            current.id == op -> { store.release(processor, op); converge(slot); ok(request, "released, consuming again") }
            else -> refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation ${current.id} holds the lock of processor $processor, not $op")
        }
    }

    private fun notPrepared(slot: Slot, request: ControlRequest, op: String): ControlReply {
        val current = store.get(slot.consumer.processor)?.operation?.takeIf { !it.expired(clock.instant()) }
        val holder = current?.let { "; operation ${it.id} holds the lock" }.orEmpty()
        return refused(request, ControlRefusal.CONTROL_REFUSAL_NOT_PREPARED, "operation $op is not prepared on this instance$holder")
    }

    // --- convergence ---

    private fun poll() {
        val current = slots
        if (current.isEmpty()) return
        val docs = try {
            store.read(current.keys)
        } catch (e: Exception) {
            log.warn("Could not read the processor control documents, keeping the processors as they are: {}", e.toString())
            return
        }
        val now = clock.instant()
        for (slot in current.values) {
            try {
                converge(slot, docs[slot.consumer.processor], now)
                removePassed(slot)
            } catch (e: Exception) {
                log.warn("Could not apply the control document of processor {}: {}", slot.consumer.processor, e.toString())
            }
        }
    }

    private fun converge(slot: Slot) = converge(slot, store.get(slot.consumer.processor), clock.instant())

    /**
     * Applies [doc] to [slot]'s consumer: replay ranges, pause, and whether it runs (not while an unexpired operation holds the
     * lock, its own included). A lease of the operation held here that passed is cleared (conditional on its id) and the
     * consumer resumes. [failFast]: a consumer that cannot start throws (at start) instead of waiting for the next poll.
     */
    private fun converge(slot: Slot, doc: ProcessorControlStore.Control?, now: Instant, failFast: Boolean = false) = synchronized(slot) {
        val consumer = slot.consumer
        val processor = consumer.processor
        var operation = doc?.operation
        val held = slot.held
        if (held != null) {
            if (operation == null || operation.id != held.id) {
                slot.held = null // released (another instance's finish) or taken over after it expired
                ended(slot, held.id, held.lease)
            } else if (operation.expired(now) && !slot.executing) {
                if (!store.release(processor, held.id, expiredOnly = true)) return@synchronized // renewed meanwhile: next poll
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
                consumer.start()
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

    /** Removes the replay ranges this instance (or the group) consumed past from the document. */
    private fun removePassed(slot: Slot) {
        val consumer = slot.consumer
        for (range in consumer.passedRanges()) {
            if (store.removeReplay(consumer.processor, range)) {
                log.info("Processor {} replayed partition {} of {} up to offset {}", consumer.processor, range.partition, range.topic, range.until)
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
        .setTopic(letter.topic)
        .setPartition(letter.partition)
        .setOffset(letter.offset)
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
