package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.Processors
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics
import com.damoreira.snodra.messaging.saga.SagaStage
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.ProcessedMarker
import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.tracing.CorrelationContext
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant

/**
 * Runs one event inside this service: first its ordinary `@EventHandler`s (stage 1), then the saga runtime ([SagaStage],
 * stage 2). Shared by the event processors ([EventConsumer], one per processor, [dispatchIn]), the external sources
 * and the webhook ingress ([dispatch]).
 *
 * Stage 1 runs per event processor: the processor's idempotency marker ([ProcessedMarker], id
 * [Processors.markerId]: `processor:<name>:<dedupeId>`, `default` included) is recorded and the processor's handlers run
 * inside one transaction of the message [store]; a duplicate marker (from the marker's record only) skips the processor, a
 * handler failure rolls the marker back. Handlers run via `runBlocking` on the calling thread so the thread-bound
 * transaction wraps them. Stage 2 runs only for the `default` processor, after stage 1 committed, with markers
 * and transactions of its own, so a redelivered record whose stage 1 is done goes straight to it. **Stage 2 runs even when
 * stage 1 failed**: the failure is held, stage 2 runs, and then the failure is
 * thrown, so the caller redelivers, parks or skips the record by its own rules; a redelivery applies nothing twice because
 * the markers decide. Both stages finish before the caller acknowledges the record.
 *
 * Each stage-1 run of [dispatchIn] (a delivered event) is timed into [metrics], whether its handlers ran, failed
 * or found the marker; the saga stage and the other entry points are not.
 */
class LocalEventDispatcher(
    private val registry: HandlerRegistry,
    private val invoker: HandlerInvoker,
    private val store: MessageStore,
    private val sagaStage: SagaStage = SagaStage.NONE,
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
) {
    private val log = LoggerFactory.getLogger(LocalEventDispatcher::class.java)
    private val tx = TransactionTemplate(store.transactions)

    /** What [dispatch] did with an event. */
    enum class Outcome {
        /** An ordinary handler ran now, or the saga stage ran. */
        HANDLED,

        /** The ordinary handlers had already run for this id and no saga is interested: nothing was done. */
        DUPLICATE,

        /** No ordinary handler and no saga wants the type: nothing was done and no marker written. */
        NO_INTEREST,

        /** No ordinary handler, and the saga stage is interested in the type but applied the event to no saga instance. */
        UNCLAIMED,
    }

    /** What [runStage1] did. */
    enum class Stage1 { HANDLED, DUPLICATE, NO_HANDLER }

    /** Whether any ordinary handler (of any processor) or saga type wants events of [type]. */
    fun interested(type: String): Boolean = registry.eventHandlers(type).isNotEmpty() || sagaStage.interested(type)

    /** Whether [processor] wants events of [type]: a stage-1 handler of it, or (only `default`) a saga type. */
    fun interested(processor: String, type: String): Boolean =
        hasStage1(processor, type) || (processor == Processors.DEFAULT && sagaStage.interested(type))

    /** Whether [processor] has a stage-1 handler for [type]. */
    fun hasStage1(processor: String, type: String): Boolean = registry.eventHandlers(type, processor).isNotEmpty()

    /** Whether [processor] runs in this service: it has a stage-1 handler, or it is `default` and sagas run. */
    fun runs(processor: String): Boolean =
        processor in registry.processors() || (processor == Processors.DEFAULT && sagaStage.eventTypes().isNotEmpty())

    /** The processors that run in this service, `default` first, then by name. */
    fun processors(): List<String> {
        val named = registry.processors() - Processors.DEFAULT
        return (if (runs(Processors.DEFAULT)) listOf(Processors.DEFAULT) else emptyList()) + named.sorted()
    }

    /** The event types [processor] handles, sorted; `default` also lists the saga event types. */
    fun eventTypes(processor: String): Set<String> =
        (registry.eventTypes(processor) + if (processor == Processors.DEFAULT) sagaStage.eventTypes() else emptySet()).toSortedSet()

    /**
     * Dispatches the event of [type] with its JSON [payload] to the stage 1 of **every** processor (each in its own
     * transaction under its own marker; `default` first, then by name) and then to the sagas: the local dispatch of the
     * external sources and the webhook ingress. [dedupeId] keys the markers and [ctx] is what the handlers see. Throws the
     * first stage-1 failure (with the others and a stage-2 failure suppressed in it) or stage 2's failure, after everything
     * ran; see the class comment.
     */
    fun dispatch(dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext): Outcome {
        val processors = processors().filter { hasStage1(it, type) }
        return run(processors, sagaStage.interested(type), dedupeId, type, payload, ctx, wrapStage2 = false, timed = false)
    }

    /**
     * Dispatches the event to [processor] only: its stage 1 (its marker carrying [position], the delivered event's), then (only
     * for `default`) the sagas. In a [replay] (the record is in a replay range of a reset) the `@DisallowReplay`
     * handlers and the sagas are skipped. Throws stage 1's failure (stage 2's suppressed in it), or a [SagaStageException]
     * when only stage 2 failed.
     */
    fun dispatchIn(
        processor: String, dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext,
        position: RecordPosition? = null, replay: Boolean = false,
    ): Outcome {
        val stage1 = if (hasStage1(processor, type)) listOf(processor) else emptyList()
        val saga = !replay && processor == Processors.DEFAULT && sagaStage.interested(type)
        return run(stage1, saga, dedupeId, type, payload, ctx, wrapStage2 = true, timed = true, position, replay)
    }

    /** Runs only stage 2 (the sagas) for the event, as for a record parked SEQUENCED in `default`; false when no saga is interested. */
    fun runStage2(type: String, payload: ByteArray, ctx: MessageContext): Boolean {
        if (!sagaStage.interested(type)) return false
        sagaStage.process(type, payload, ctx)
        return true
    }

    /**
     * Runs [processor]'s stage 1 for the event and then [inTransaction], all in the marker transaction: a dead letter's
     * retry deletes the letter there, so handling and deletion commit together or not at all. Throws what the handlers or
     * [inTransaction] throw (everything rolled back). [replay]: the record was parked while its processor replayed it, so its
     * `@DisallowReplay` handlers are left out.
     */
    fun runStage1(
        processor: String, dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext, position: RecordPosition? = null,
        replay: Boolean = false, inTransaction: () -> Unit,
    ): Stage1 {
        if (!hasStage1(processor, type)) return Stage1.NO_HANDLER
        return if (stage1(processor, dedupeId, type, payload, ctx, position, replay, inTransaction)) Stage1.HANDLED else Stage1.DUPLICATE
    }

    private fun run(
        stage1: List<String>, saga: Boolean, dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext, wrapStage2: Boolean,
        timed: Boolean, position: RecordPosition? = null, replay: Boolean = false,
    ): Outcome {
        if (stage1.isEmpty() && !saga) return Outcome.NO_INTEREST
        var handled = false
        var failure: Exception? = null
        for (processor in stage1) {
            val started = System.nanoTime()
            try {
                if (stage1(processor, dedupeId, type, payload, ctx, position, replay) {}) handled = true
            } catch (e: Exception) {
                // Held until the other processors and stage 2 ran: one failing handler group hides the event from none of them.
                val first = failure
                if (first == null) failure = e else first.addSuppressed(e)
            } finally {
                if (timed) metrics.eventHandled(processor, type, System.nanoTime() - started)
            }
        }
        var applied = false
        if (saga) {
            try {
                applied = sagaStage.process(type, payload, ctx)
            } catch (e: Exception) {
                val first = failure
                if (first == null) throw if (wrapStage2) SagaStageException(e) else e
                first.addSuppressed(e) // both failed: the caller acts on stage 1's, stage 2's is kept with it
            }
        }
        failure?.let { throw it }
        return when {
            stage1.isNotEmpty() && !handled && !saga -> Outcome.DUPLICATE
            stage1.isEmpty() && !applied -> Outcome.UNCLAIMED
            else -> Outcome.HANDLED
        }
    }

    /** One processor's stage 1 in one transaction; true when its handlers ran, false for a duplicate marker. */
    private fun stage1(
        processor: String, dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext, position: RecordPosition?, replay: Boolean,
        inTransaction: () -> Unit,
    ): Boolean {
        val marker = Processors.markerId(processor, dedupeId)
        return try {
            tx.executeWithoutResult {
                runBlocking(CorrelationContext(ctx.correlationId, ctx.headers[EventHeaders.TRACEPARENT])) {
                    val recorded = store.processedEvents.record(
                        ProcessedMarker(
                            marker, type, Instant.now(),
                            processor.takeIf { position != null }, position?.source, position?.segment, position?.position,
                        ),
                    )
                    if (!recorded) throw AlreadyProcessed // only the marker's duplicate means "seen before"; a handler's is a failure
                    invoker.invokeEvent(type, payload, ctx, processor, replay)
                }
                inTransaction()
            }
            true
        } catch (_: AlreadyProcessed) {
            log.debug("Event {} of type {} already processed in processor {}, skipping", dedupeId, type, processor)
            false
        }
    }

    /** Thrown inside the transaction when the idempotency marker already exists; rolls it back and skips the stage. */
    private object AlreadyProcessed : RuntimeException("event already processed", null, false, false) {
        private fun readResolve(): Any = AlreadyProcessed
    }
}

/** Stage 2 (the sagas) failed for an event whose stage 1 succeeded or had nothing to do ([LocalEventDispatcher.dispatchIn]). */
class SagaStageException(cause: Exception) : RuntimeException("saga stage failed: ${cause.message}", cause)
