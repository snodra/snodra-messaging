package io.damoreira.kraken.messaging.saga

import com.mongodb.MongoException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.TransientDataAccessException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.transaction.support.TransactionOperations
import java.time.Clock
import java.time.Instant
import java.util.UUID
import kotlin.reflect.KClass

/**
 * The saga stage of local event dispatch: applies each event to every interested saga type, once per instance.
 *
 * Per (event, saga type) (S1 plan R1) one transaction runs: the marker `processed_events` id `saga:<sagaType>:<eventId>`
 * is inserted first (an existing one skips the event); the live instances associated with the event are resolved per
 * handler (`keyName = value`); when none matches, a `@StartSaga` handler creates one with `startKey =
 * "<type>|<key>=<value>"` (a `forceNew` handler always creates one, without a start key); each instance's handlers run
 * in the scanner's order, a handler running only on instances associated with its own key and value. Handlers see
 * recording objects ([SagaLifecycle], [SagaCommands], [DeadlineManager]); once every handler returned, the state is
 * written with an optimistic version check (`version + 1`; a version of 0 means just created), the recorded dispatch
 * entries (`seq = version * 1000 + index`) and deadline changes are written, and an ended instance cancels its
 * scheduled deadlines. A conflict (version, start key, Mongo write conflict) or any handler exception rolls the whole
 * transaction back and the event is retried with [retry] (R4); after the last attempt it is parked in `saga_failures`
 * together with the marker and the stage moves on. The only exception [process] throws is when parking itself fails
 * (the store is down): the consumer then redelivers the record instead of losing it.
 *
 * Handlers run in the calling thread inside the transaction, with the MDC keys `sagaType`, `sagaId`, `correlationId`.
 * Persistence is behind [ports] (Mongo by default); [tx] is the transaction template of the Mongo manager, or
 * [TransactionOperations.withoutTransaction] over in-memory ports.
 */
class SagaRuntime(
    types: List<SagaType>,
    private val ports: SagaPorts,
    private val tx: TransactionOperations,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val beans: (KClass<*>) -> Any,
    private val clock: Clock = Clock.systemUTC(),
    private val retry: SagaRetry = SagaRetry(),
    private val metrics: SagaMetrics = NoopSagaMetrics,
) : SagaStage {
    constructor(
        types: List<SagaType>,
        mongo: MongoTemplate,
        tx: TransactionOperations,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        beans: (KClass<*>) -> Any,
        clock: Clock = Clock.systemUTC(),
        retry: SagaRetry = SagaRetry(),
        metrics: SagaMetrics = NoopSagaMetrics,
    ) : this(types, SagaPorts.mongo(mongo), tx, codec, typeResolver, beans, clock, retry, metrics)

    private val log = LoggerFactory.getLogger(SagaRuntime::class.java)
    private val byName: Map<String, SagaType> = types.associateBy { it.name }
    private val byEvent: Map<String, List<SagaType>> =
        types.flatMap { t -> t.eventHandlers.keys.map { it to t } }.groupBy({ it.first }, { it.second })

    override fun interested(type: String): Boolean = type in byEvent

    override fun process(type: String, payload: ByteArray, ctx: MessageContext) {
        for (sagaType in byEvent[type].orEmpty()) deliver(sagaType, type, payload, ctx, null)
    }

    /**
     * Fires [deadline] (claimed by the scheduler): in one transaction, a deadline no longer SCHEDULED is left alone; one
     * whose saga is gone or ended, or whose saga type has no handler of that name, is CANCELLED; otherwise the
     * `@DeadlineHandler` runs, its effects are written as for an event and the deadline becomes FIRED. Conflicts and
     * handler exceptions are retried like an event's; when they persist the exception is thrown to the scheduler (the
     * lease then lets the deadline fire again: at least once). An unknown saga type throws at once.
     */
    fun applyDeadline(deadline: DeadlineDocument) {
        val sagaType = byName[deadline.sagaType]
            ?: throw IllegalStateException("Deadline ${deadline.id} belongs to saga type '${deadline.sagaType}', which this service does not have")
        var last: Exception? = null
        for (attempt in 1..retry.maxAttempts) {
            if (attempt > 1) retry.pauseBefore(attempt)
            try {
                if (tx.execute { fireDeadline(sagaType, deadline) } == true) metrics.deadlineFired(deadline.name)
                return
            } catch (e: Exception) {
                last = e
                log.debug("Deadline {} of saga {} failed attempt {}: {}", deadline.id, deadline.sagaId, attempt, e.toString())
            }
        }
        throw last!!
    }

    /**
     * Re-delivers a parked event ([failure], from `SagaFailures`) to its saga type, ignoring the marker (it exists by
     * design): in one transaction the failure is deleted and the event applied, with the retries of R4. Returns true
     * when it was applied (or no longer matches anything), false when it failed again (the failure stays with `attempts`
     * increased and the new error) or was no longer parked (a concurrent retry or discard).
     */
    fun reapply(failure: SagaFailureDocument): Boolean {
        val sagaType = byName[failure.sagaType]
        if (sagaType == null) {
            parkAgain(failure, "Saga type '${failure.sagaType}' does not exist in this service", 0)
            return false
        }
        return deliver(sagaType, failure.eventType, failure.payload.toByteArray(Charsets.UTF_8), contextOf(failure), failure)
    }

    // ---- events ------------------------------------------------------------------------------------------------

    private enum class Kind { HANDLED, NO_MATCH, DUPLICATE, GONE }
    private class Result(val kind: Kind, val started: Int = 0, val ended: Int = 0)

    /** Runs the attempts for one saga type; true when applied (or skipped as a duplicate), false when parked or gone. */
    private fun deliver(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, retryOf: SagaFailureDocument?): Boolean {
        var last: Exception? = null
        for (attempt in 1..retry.maxAttempts) {
            if (attempt > 1) retry.pauseBefore(attempt)
            try {
                val result = tx.execute { applyEvent(sagaType, eventType, payload, ctx, retryOf) }
                report(sagaType, result)
                return result.kind != Kind.GONE
            } catch (e: Exception) {
                last = e
                if (isConflict(e)) metrics.event(sagaType.name, SagaMetrics.Outcome.CONFLICT)
                log.debug("Saga type {} failed attempt {} of event {}: {}", sagaType.name, attempt, ctx.messageId, e.toString())
            }
        }
        if (retryOf != null) {
            parkAgain(retryOf, describe(last!!), retry.maxAttempts)
            return false
        }
        park(sagaType, eventType, payload, ctx, describe(last!!))
        return false
    }

    private fun report(sagaType: SagaType, result: Result) {
        when (result.kind) {
            Kind.HANDLED -> {
                repeat(result.started) { metrics.sagaStarted(sagaType.name) }
                repeat(result.ended) { metrics.sagaEnded(sagaType.name) }
                metrics.event(sagaType.name, SagaMetrics.Outcome.HANDLED)
            }
            Kind.NO_MATCH -> metrics.event(sagaType.name, SagaMetrics.Outcome.NO_MATCH)
            Kind.DUPLICATE, Kind.GONE -> Unit
        }
    }

    /** One attempt, inside the transaction. */
    @Suppress("UNCHECKED_CAST")
    private fun applyEvent(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, retryOf: SagaFailureDocument?): Result {
        val now = clock.instant()
        if (retryOf != null) {
            if (!ports.failures.delete(retryOf.id)) return Result(Kind.GONE)
        } else {
            val marker = markerId(sagaType, ctx.messageId)
            if (ports.markers.exists(marker)) {
                log.debug("Event {} already applied by saga type {}, skipping", ctx.messageId, sagaType.name)
                return Result(Kind.DUPLICATE)
            }
            ports.markers.insert(marker, eventType, now)
        }

        val handlers = sagaType.eventHandlers.getValue(eventType)
        val event = codec.decode(payload, handlers.first().eventClass as KClass<Any>)
        val values = handlers.associateWith { it.associationValue(event, ctx) }

        // Live instances per handler: the handler runs only on the instances associated with its own key and value.
        val matched = LinkedHashMap<String, SagaDocument>()
        val handledBy = HashMap<String, MutableSet<SagaEventHandlerMethod>>()
        for ((handler, value) in values) {
            if (value == null) continue
            for (doc in ports.sagas.findLive(sagaType.name, handler.keyName, value)) {
                matched.putIfAbsent(doc.id, doc)
                handledBy.getOrPut(doc.id) { mutableSetOf() } += handler
            }
        }
        val planned = mutableListOf<Planned>()
        for (doc in matched.values) {
            planned += invoke(sagaType, doc.id, sagaType.decodeState(doc), doc, doc.associations, handlers.filter { it in handledBy.getValue(doc.id) }, event, values, ctx, now)
        }
        for ((handler, value) in values) {
            if (value == null || !handler.start || !handler.forceNew) continue
            planned += startInstance(sagaType, handler, value, handlers, event, values, ctx, now, startKey = null)
        }
        if (matched.isEmpty()) {
            val start = values.entries.firstOrNull { (h, v) -> h.start && !h.forceNew && v != null }
            if (start != null) {
                val value = start.value!!
                planned += startInstance(sagaType, start.key, value, handlers, event, values, ctx, now, startKey = "${sagaType.name}|${start.key.keyName}=$value")
            }
        }
        if (planned.isEmpty()) {
            log.debug("Event {} of type {} matched no instance of saga type {} and starts none", ctx.messageId, eventType, sagaType.name)
            return Result(Kind.NO_MATCH)
        }
        planned.forEach { write(it, now) }
        return Result(Kind.HANDLED, started = planned.count { it.created }, ended = planned.count { it.ended })
    }

    private fun startInstance(
        sagaType: SagaType, via: SagaEventHandlerMethod, value: String, handlers: List<SagaEventHandlerMethod>, event: Any,
        values: Map<SagaEventHandlerMethod, String?>, ctx: MessageContext, now: Instant, startKey: String?,
    ): Planned {
        val association = Association(via.keyName, value)
        val own = handlers.filter { it.keyName == via.keyName && values[it] == value }
        return invoke(sagaType, UUID.randomUUID().toString(), sagaType.newInstance(), null, listOf(association), own, event, values, ctx, now, startKey)
    }

    /** Runs [handlers] on one instance and plans its writes; [doc] is its stored document, null for a new instance. */
    private fun invoke(
        sagaType: SagaType, sagaId: String, saga: Any, doc: SagaDocument?, associations: List<Association>,
        handlers: List<SagaEventHandlerMethod>, event: Any, values: Map<SagaEventHandlerMethod, String?>, ctx: MessageContext, now: Instant,
        startKey: String? = null,
    ): Planned {
        val version = if (doc == null) 0L else doc.version + 1
        val run = Run(sagaType, sagaId, version, ctx, now, associations)
        withMdc(sagaType.name, sagaId, ctx.correlationId) {
            for (handler in handlers) {
                handler.invoke(saga, run.args(handler.params, event))
                if (handler.end) run.lifecycle.end()
                if (run.lifecycle.endRequested) break
            }
        }
        return run.plan(saga, doc, startKey)
    }

    // ---- deadlines ---------------------------------------------------------------------------------------------

    /** One attempt at firing [claimed], inside the transaction; true when its handler ran. */
    @Suppress("UNCHECKED_CAST")
    private fun fireDeadline(sagaType: SagaType, claimed: DeadlineDocument): Boolean {
        val now = clock.instant()
        ports.deadlines.findScheduled(claimed.id) ?: return false // cancelled or fired since the scheduler claimed it
        val doc = ports.sagas.findById(claimed.sagaId)
        if (doc == null || doc.ended) {
            ports.deadlines.cancel(claimed.sagaId, claimed.id, now)
            return false
        }
        val handler = sagaType.deadlineHandlers[claimed.name]
        if (handler == null) {
            log.error("Saga type {} has no @DeadlineHandler named '{}': cancelling deadline {} of saga {}", sagaType.name, claimed.name, claimed.id, claimed.sagaId)
            ports.deadlines.cancel(claimed.sagaId, claimed.id, now)
            return false
        }
        val ctx = MessageContext(claimed.id, "deadline:${claimed.name}", claimed.id, null, emptySet(), null, emptyMap())
        val payload = handler.payloadClass?.let { klass -> claimed.payload?.let { codec.decode(it.toByteArray(Charsets.UTF_8), klass as KClass<Any>) } }
        val run = Run(sagaType, doc.id, doc.version + 1, ctx, now, doc.associations)
        val saga = sagaType.decodeState(doc)
        withMdc(sagaType.name, doc.id, ctx.correlationId) {
            handler.invoke(saga, run.args(handler.params, payload))
            if (handler.end) run.lifecycle.end()
        }
        val planned = run.plan(saga, doc, null)
        if (!ports.deadlines.markFired(claimed.id, now)) throw VersionConflict("deadline ${claimed.id} changed while firing")
        write(planned, now)
        if (planned.ended) metrics.sagaEnded(sagaType.name)
        return true
    }

    // ---- shared ------------------------------------------------------------------------------------------------

    /** The recording objects of one instance's handling, and what to do with them afterwards. */
    private inner class Run(
        val sagaType: SagaType, val sagaId: String, val version: Long, val ctx: MessageContext, val now: Instant, associations: List<Association>,
    ) {
        val lifecycle = RecordingSagaLifecycle(sagaId, associations)
        val commands = RecordingSagaCommands(sagaId, version, ctx.correlationId, now, codec, typeResolver)
        val deadlines = RecordingDeadlineManager(sagaType.name, sagaId, now, codec)

        /** The handler's arguments: [first] is the event or the deadline payload. */
        fun args(params: List<ParamKind>, first: Any?): List<Any?> = params.map {
            when (it) {
                ParamKind.Event, ParamKind.DeadlinePayload -> first
                ParamKind.Lifecycle -> lifecycle
                ParamKind.Commands -> commands
                ParamKind.Deadlines -> deadlines
                ParamKind.Context -> ctx
                is ParamKind.Bean -> beans(it.type)
            }
        }

        fun plan(saga: Any, doc: SagaDocument?, startKey: String?): Planned {
            val ended = lifecycle.endRequested
            val state = String(codec.encode(saga), Charsets.UTF_8)
            val next = doc?.copy(
                state = state, associations = lifecycle.associations.toList(), ended = ended, endedAt = if (ended) now else null,
                version = version, updatedAt = now,
            ) ?: SagaDocument(
                id = sagaId, type = sagaType.name, state = state, associations = lifecycle.associations.toList(), startKey = startKey,
                ended = ended, endedAt = if (ended) now else null, version = 0, createdAt = now, updatedAt = now,
            )
            return Planned(next, doc?.version, commands.entries, deadlines.ops, created = doc == null)
        }
    }

    /** One instance's new document and the effects to write with it. [expectedVersion] is null for a new instance. */
    private class Planned(
        val doc: SagaDocument, val expectedVersion: Long?, val entries: List<SagaDispatchDocument>, val deadlineOps: List<DeadlineOp>,
        val created: Boolean,
    ) {
        val ended: Boolean get() = doc.ended
    }

    private fun write(planned: Planned, now: Instant) {
        val doc = planned.doc
        if (planned.expectedVersion == null) {
            ports.sagas.insert(doc)
        } else if (!ports.sagas.update(doc, planned.expectedVersion)) {
            throw VersionConflict("saga ${doc.id} is no longer at version ${planned.expectedVersion}")
        }
        for (op in planned.deadlineOps) {
            when (op) {
                is DeadlineOp.Schedule -> ports.deadlines.insert(op.deadline)
                is DeadlineOp.Cancel -> ports.deadlines.cancel(doc.id, op.deadlineId, now)
                is DeadlineOp.CancelAll -> ports.deadlines.cancelByName(doc.id, op.name, now)
            }
        }
        ports.dispatch.record(planned.entries)
        if (doc.ended) ports.deadlines.cancelAll(doc.id, now)
    }

    @Suppress("UNCHECKED_CAST")
    private fun SagaType.decodeState(doc: SagaDocument): Any = codec.decode(doc.state.toByteArray(Charsets.UTF_8), klass as KClass<Any>)

    private fun withMdc(sagaType: String, sagaId: String, correlationId: String, block: () -> Unit) {
        val keys = listOf("sagaType" to sagaType, "sagaId" to sagaId, "correlationId" to correlationId)
        val previous = keys.map { MDC.get(it.first) }
        keys.forEach { MDC.put(it.first, it.second) }
        try {
            block()
        } finally {
            keys.zip(previous).forEach { (key, old) -> if (old == null) MDC.remove(key.first) else MDC.put(key.first, old) }
        }
    }

    // ---- parking -----------------------------------------------------------------------------------------------

    /** Parks the event for [sagaType] with its marker; skipped when the marker exists (another instance finished it). */
    private fun park(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, error: String) {
        val now = clock.instant()
        val marker = markerId(sagaType, ctx.messageId)
        val parked = tx.execute {
            if (ports.markers.exists(marker)) {
                false
            } else {
                ports.markers.insert(marker, eventType, now)
                ports.failures.save(
                    SagaFailureDocument(
                        id = "${sagaType.name}:${ctx.messageId}", sagaType = sagaType.name, eventId = ctx.messageId, eventType = eventType,
                        payload = String(payload, Charsets.UTF_8),
                        headers = failureHeaders(ctx), error = error, attempts = retry.maxAttempts, firstFailedAt = now, lastFailedAt = now,
                    ),
                )
                true
            }
        }
        if (parked) {
            metrics.event(sagaType.name, SagaMetrics.Outcome.PARKED)
            log.error("Saga type {} parked event {} of type {} after {} attempts: {}", sagaType.name, ctx.messageId, eventType, retry.maxAttempts, error)
        } else {
            log.warn("Saga type {} gave up on event {} but it was applied or parked meanwhile", sagaType.name, ctx.messageId)
        }
    }

    /** A reapplied failure failed again: it stays, with the attempts of this try added and the new error. */
    private fun parkAgain(failure: SagaFailureDocument, error: String, attempts: Int) {
        val now = clock.instant()
        tx.executeWithoutResult {
            ports.failures.save(failure.copy(attempts = failure.attempts + attempts, error = error, lastFailedAt = now))
        }
        log.error("Saga type {} failed again on parked event {}: {}", failure.sagaType, failure.eventId, error)
    }

    /** The headers kept with a parked event: enough to rebuild its [MessageContext] ([contextOf]). */
    private fun failureHeaders(ctx: MessageContext): Map<String, String> {
        val headers = LinkedHashMap(ctx.headers)
        headers.putIfAbsent("correlationId", ctx.correlationId)
        if (ctx.userId != null) headers.putIfAbsent("userId", ctx.userId)
        return headers
    }

    private fun contextOf(failure: SagaFailureDocument) = MessageContext(
        messageId = failure.eventId,
        type = failure.eventType,
        correlationId = failure.headers["correlationId"] ?: failure.eventId,
        userId = failure.headers["userId"]?.takeIf { it.isNotBlank() },
        roles = emptySet(),
        authorization = null,
        headers = failure.headers,
    )

    private fun markerId(sagaType: SagaType, eventId: String) = "saga:${sagaType.name}:$eventId"

    private fun describe(e: Throwable): String {
        val root = generateSequence(e) { t -> t.cause?.takeIf { it !== t } }.last()
        return "${root.javaClass.name}: ${root.message}".take(MAX_ERROR_LENGTH)
    }

    /** Version, start-key and Mongo write conflicts: the transient kind a retry resolves. */
    private fun isConflict(e: Throwable): Boolean = generateSequence(e) { t -> t.cause?.takeIf { it !== t } }.any {
        it is VersionConflict || it is DuplicateKeyException || it is TransientDataAccessException ||
            (it is MongoException && (it.hasErrorLabel("TransientTransactionError") || it.code == WRITE_CONFLICT))
    }

    /** The stored version of an instance (or the state of a deadline) is not the one this attempt read. */
    private class VersionConflict(message: String) : RuntimeException(message)

    private companion object {
        const val MAX_ERROR_LENGTH = 2000
        const val WRITE_CONFLICT = 112
    }
}
