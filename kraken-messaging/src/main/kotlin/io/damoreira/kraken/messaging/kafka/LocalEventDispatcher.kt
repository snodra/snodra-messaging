package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant

/**
 * Runs one event inside this service: first its ordinary `@EventHandler`s, then the saga runtime ([SagaStage]). Shared
 * by the Kafka [EventConsumer] and, later, the external sources and the webhook ingress.
 *
 * Stage 1 is the consumer's own behaviour: the idempotency marker ([ProcessedEventDocument], id [dedupeId]) is inserted
 * and the handlers run inside one Mongo transaction; a duplicate marker (from the marker insert only) skips the stage, a
 * handler failure rolls the marker back and is thrown, so the caller redelivers the record (and stage 2 does not run
 * for it). Handlers run via `runBlocking` on the calling thread so the thread-bound transaction wraps them. Stage 2 runs
 * after stage 1 committed, with markers and transactions of its own, so a redelivered record whose stage 1 is done goes
 * straight to it. Both stages finish before the caller acknowledges the record.
 */
class LocalEventDispatcher(
    private val registry: HandlerRegistry,
    private val invoker: HandlerInvoker,
    private val tx: TransactionTemplate,
    private val mongo: MongoTemplate,
    private val sagaStage: SagaStage = SagaStage.NONE,
) {
    private val log = LoggerFactory.getLogger(LocalEventDispatcher::class.java)

    /** What [dispatch] did with an event. */
    enum class Outcome {
        /** An ordinary handler ran now, or the saga stage ran. */
        HANDLED,

        /** The ordinary handlers had already run for this id and no saga is interested: nothing was done. */
        DUPLICATE,

        /** No ordinary handler and no saga wants the type: nothing was done and no marker written. */
        NO_INTEREST,
    }

    /** Whether any ordinary handler or saga type wants events of [type]. */
    fun interested(type: String): Boolean = registry.eventHandlers(type).isNotEmpty() || sagaStage.interested(type)

    /**
     * Dispatches the event of [type] with its JSON [payload]; [dedupeId] keys stage 1's marker and [ctx] is what the
     * handlers see. Throws what stage 1's handlers throw (the caller retries); see the class comment.
     */
    fun dispatch(dedupeId: String, type: String, payload: ByteArray, ctx: MessageContext): Outcome {
        val ordinary = registry.eventHandlers(type).isNotEmpty()
        val saga = sagaStage.interested(type)
        if (!ordinary && !saga) return Outcome.NO_INTEREST

        var duplicate = false
        if (ordinary) {
            try {
                tx.executeWithoutResult {
                    try {
                        mongo.insert(ProcessedEventDocument(dedupeId, type, Instant.now()))
                    } catch (_: DuplicateKeyException) {
                        throw AlreadyProcessed // only the marker's duplicate means "seen before"; a handler's is a failure
                    }
                    runBlocking(CorrelationContext(ctx.correlationId)) { invoker.invokeEvent(type, payload, ctx) }
                }
            } catch (_: AlreadyProcessed) {
                log.debug("Event {} of type {} already processed, skipping", dedupeId, type)
                duplicate = true
            }
        }
        if (saga) sagaStage.process(type, payload, ctx)
        return if (duplicate && !saga) Outcome.DUPLICATE else Outcome.HANDLED
    }

    /** Thrown inside the transaction when the idempotency marker already exists; rolls it back and skips the stage. */
    private object AlreadyProcessed : RuntimeException("event already processed", null, false, false) {
        private fun readResolve(): Any = AlreadyProcessed
    }
}
