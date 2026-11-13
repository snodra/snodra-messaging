package io.damoreira.kraken.messaging.saga

import com.mongodb.MongoException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.store.SagaStores
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
 * "<type>|<key>=<value>"` ([startKey]: the parts escaped) (a `forceNew` handler always creates one, without a start key; the key is kept only while the
 * instance keeps that association, from its start handler on); each instance's handlers run
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
    private val ports: SagaStores,
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
    ) : this(types, SagaStores.mongo(mongo), tx, codec, typeResolver, beans, clock, retry, metrics)

    private val log = LoggerFactory.getLogger(SagaRuntime::class.java)
    private val commandFailedType: String = typeResolver.typeNameOf(SagaCommandFailed::class)
    private val byName: Map<String, SagaType> = types.associateBy { it.name }
    private val byEvent: Map<String, List<SagaType>> =
        types.flatMap { t -> t.eventHandlers.keys.map { it to t } }.groupBy({ it.first }, { it.second })

    override fun interested(type: String): Boolean = type in byEvent

    override fun eventTypes(): Set<String> = byEvent.keys

    override fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
        var applied = false
        for (sagaType in byEvent[type].orEmpty()) {
            // Parked (null) counts too: the event reached a saga, whose park is logged and kept for a person.
            val kind = deliverKind(sagaType, type, payload, ctx, null)
            if (kind != Kind.NO_MATCH && kind != Kind.GONE) applied = true
        }
        return applied
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
        try {
            withRetries("Deadline ${deadline.id} of saga ${deadline.sagaId}", onConflict = {}) {
                if (tx.execute { fireDeadline(sagaType, deadline) } == true) metrics.deadlineFired(deadline.name)
            }
        } catch (e: GaveUp) {
            throw e.last
        }
    }

    /**
     * Re-delivers a parked event ([failure], from `SagaFailures`) to its saga type, ignoring the marker (it exists by
     * design): in one transaction the failure is deleted and the event applied, with the retries of R4. Returns true
     * when it was applied (or no longer matches anything), false when it failed again (the failure stays with `attempts`
     * increased and the new error) or was no longer parked (a concurrent retry or discard: it is then not brought back).
     *
     * @throws IllegalStateException when the failure's saga type does not run in this service; the parked event is left as
     *   it was (its error, attempts and `lastFailedAt`), see [checkRuns]
     */
    fun reapply(failure: SagaFailureDocument): Boolean {
        checkRuns(failure.sagaType) { "parked event ${failure.id} is left as it was" }
        return deliver(byName.getValue(failure.sagaType), failure.eventType, failure.payload.toByteArray(Charsets.UTF_8), contextOf(failure), failure)
    }

    /** Refuses, without writing anything, a retry of parked events whose saga type [sagaType] does not run in this service; [left] says which. */
    fun checkRuns(sagaType: String, left: () -> String) {
        check(sagaType in byName) { "Saga type '$sagaType' does not run in this service: ${left()}" }
    }

    // ---- events ------------------------------------------------------------------------------------------------

    private enum class Kind { HANDLED, NO_MATCH, DUPLICATE, GONE }
    private class Result(val kind: Kind, val started: Int = 0, val ended: Int = 0)

    /** Runs the attempts for one saga type; true when applied (or skipped as a duplicate), false when parked or gone. */
    private fun deliver(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, retryOf: SagaFailureDocument?): Boolean =
        deliverKind(sagaType, eventType, payload, ctx, retryOf).let { it != null && it != Kind.GONE }

    /** As [deliver], but says what happened: null when the event was parked (or parked again). */
    private fun deliverKind(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, retryOf: SagaFailureDocument?): Kind? {
        try {
            return withRetries("Saga type ${sagaType.name} event ${ctx.messageId}", onConflict = { metrics.event(sagaType.name, SagaMetrics.Outcome.CONFLICT) }) {
                val result = tx.execute { applyEvent(sagaType, eventType, payload, ctx, retryOf) }
                report(sagaType, result)
                result.kind
            }
        } catch (e: GaveUp) {
            if (retryOf != null) {
                parkAgain(retryOf, describe(e.last), e.attempts)
            } else {
                park(sagaType, eventType, payload, ctx, describe(e.last), e.attempts)
            }
            return null
        }
    }

    /** An attempt loop gave up after [attempts] attempts; [last] is the failure of the final one. */
    private class GaveUp(val last: Exception, val attempts: Int) : RuntimeException(last.message, last, false, false)

    /**
     * Runs [block] (one attempt, in its own transaction) until it returns, with the retry rules of ruling 33: a conflict is
     * retried up to `maxAttempts` attempts in all with the backoff of [SagaRetry.pauseBefore]; any other exception gets at
     * most [SagaRetry.HANDLER_ATTEMPTS] attempts with only a quick pause between them (waiting does not cure a handler that
     * throws). Throws [GaveUp] with the last failure and the attempts made.
     */
    private fun <T> withRetries(what: String, onConflict: () -> Unit, block: () -> T): T {
        var conflicts = 0
        var failures = 0
        while (true) {
            try {
                return block()
            } catch (e: Exception) {
                val made = conflicts + failures + 1
                log.debug("{} failed attempt {}: {}", what, made, e.toString())
                if (isConflict(e)) {
                    onConflict()
                    conflicts++
                    if (made >= retry.maxAttempts) throw GaveUp(e, made)
                    retry.pauseBefore(conflicts + 1)
                } else {
                    failures++
                    if (failures >= minOf(SagaRetry.HANDLER_ATTEMPTS, retry.maxAttempts)) throw GaveUp(e, made)
                    retry.pauseBeforeHandlerRetry()
                }
            }
        }
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
        // A SagaCommandFailed also reaches the saga that sent the command, by its id: no association needed (ruling 27).
        if (eventType == commandFailedType) {
            ports.sagas.findById((event as SagaCommandFailed).sagaId)?.takeIf { it.type == sagaType.name && !it.ended }?.let { owner ->
                matched.putIfAbsent(owner.id, owner)
                handledBy.getOrPut(owner.id) { mutableSetOf() } += handlers
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
                planned += startInstance(sagaType, start.key, value, handlers, event, values, ctx, now, startKey = startKey(sagaType.name, start.key.keyName, value))
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
        // The correlation id of the message that scheduled it.
        val ctx = MessageContext(claimed.id, "deadline:${claimed.name}", claimed.correlationId, null, emptySet(), null, emptyMap())
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
        val deadlines = RecordingDeadlineManager(sagaType.name, sagaId, now, codec, ctx.correlationId)

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
            // The start key lives and dies with its association: remove that one and the key is free for a new instance.
            // That holds from the first handler on: a start handler that removes its own start association reserves no key.
            val keptStartKey = (if (doc == null) startKey else doc.startKey)?.takeIf { key -> lifecycle.associations.any { startKey(sagaType.name, it.k, it.v) == key } }
            val next = doc?.copy(
                state = state, associations = lifecycle.associations.toList(), startKey = keptStartKey, ended = ended,
                endedAt = if (ended) now else null, version = version, updatedAt = now,
            ) ?: SagaDocument(
                id = sagaId, type = sagaType.name, state = state, associations = lifecycle.associations.toList(), startKey = keptStartKey,
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
    private fun park(sagaType: SagaType, eventType: String, payload: ByteArray, ctx: MessageContext, error: String, attempts: Int) {
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
                        headers = failureHeaders(ctx), error = error, attempts = attempts, firstFailedAt = now, lastFailedAt = now,
                    ),
                )
                true
            }
        }
        if (parked) {
            metrics.event(sagaType.name, SagaMetrics.Outcome.PARKED)
            log.error("Saga type {} parked event {} of type {} after {} attempts: {}", sagaType.name, ctx.messageId, eventType, attempts, error)
        } else {
            log.warn("Saga type {} gave up on event {} but it was applied or parked meanwhile", sagaType.name, ctx.messageId)
        }
    }

    /** A reapplied failure failed again: it stays, with the attempts of this try added and the new error. */
    private fun parkAgain(failure: SagaFailureDocument, error: String, attempts: Int) {
        val now = clock.instant()
        val stillParked = tx.execute { ports.failures.failedAgain(failure.id, error, attempts, now) } == true
        if (stillParked) {
            log.error("Saga type {} failed again on parked event {}: {}", failure.sagaType, failure.eventId, error)
        } else {
            log.warn("Saga type {} failed again on parked event {}, which was discarded or retried meanwhile: {}", failure.sagaType, failure.eventId, error)
        }
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

    companion object {
        private const val MAX_ERROR_LENGTH = 2000
        private const val WRITE_CONFLICT = 112

        /**
         * The start key of the association [key] = [value] of saga type [type]: `<type>|<key>=<value>` with `%`, `|` and `=`
         * percent-escaped in each part (`%25`, `%7C`, `%3D`), so two different pairs never build the same key.
         */
        fun startKey(type: String, key: String, value: String): String = "${escape(type)}|${escape(key)}=${escape(value)}"

        private fun escape(part: String): String = part.replace("%", "%25").replace("|", "%7C").replace("=", "%3D")
    }
}
