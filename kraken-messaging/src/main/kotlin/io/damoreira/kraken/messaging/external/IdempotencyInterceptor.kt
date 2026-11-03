package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.handler.HandlerMethod
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics.ExternalOutcome
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.cancellation.CancellationException

/** Settings of `external_calls` (`kraken.messaging.external.calls.*`): how long an `IN_PROGRESS` call may take before its outcome counts as unknown. */
data class ExternalCallSettings(val lease: Duration = Duration.ofMinutes(5)) {
    init {
        require(!lease.isZero && !lease.isNegative) { "lease must be positive, was $lease" }
    }
}

/**
 * Runs the handlers annotated with [IdempotentHandler] (the invoker hands each call to [execute]): the call is recorded in
 * `external_calls` under its key, so a repeat of the key never runs the body twice.
 *
 * 1. The record is inserted `IN_PROGRESS` with `leaseUntil = now + lease`; a duplicate key means an earlier call exists.
 * 2. An earlier `SUCCEEDED` call returns its stored reply, `FAILED` throws [ExternalCallFailedException] (permanent), `UNKNOWN`
 *    throws [ExternalCallUnknownOutcomeException]. An `IN_PROGRESS` one within its lease is another call in flight: a
 *    [RetryableExternalException] (come back later). Past its lease the outcome is unknown: without `externalIdempotent`
 *    it becomes `UNKNOWN` (never called again automatically) and the call throws [ExternalCallUnknownOutcomeException],
 *    with it the call takes the record over (`attempts + 1`) and runs the body.
 * 3. After the body: `SUCCEEDED` with the reply as JSON (when that write fails the call is an unknown outcome, never a
 *    failed one: see [unrecorded]); a [RetryableExternalException] deletes the record (the next call starts afresh, the relay
 *    owns the backoff); an [ExternalCallUnknownOutcomeException] thrown by the adapter itself leaves the record `UNKNOWN`
 *    and is thrown as it is; any other failure is `FAILED` with `{"code","message"}` and is thrown as it is.
 *
 * Every write of the record is its own short transaction (REQUIRES_NEW when [txManager] is given), so a rollback of the
 * caller's transaction (an event handler runs inside one) does not forget a call that happened. The body itself runs in
 * whatever transaction the caller has: an `@EventHandler`'s inside the dispatcher's stage-1 transaction, a `@CommandHandler`'s
 * (what the saga relay calls) in none.
 */
class IdempotencyInterceptor(
    private val mongo: MongoTemplate,
    private val codec: JsonCodec,
    txManager: PlatformTransactionManager? = null,
    private val clock: Clock = Clock.systemUTC(),
    private val settings: ExternalCallSettings = ExternalCallSettings(),
    private val metrics: SagaMetrics = NoopSagaMetrics,
) {
    private val log = LoggerFactory.getLogger(IdempotencyInterceptor::class.java)
    private val own: TransactionTemplate? = txManager?.let {
        TransactionTemplate(it).apply { propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW }
    }

    /** What [begin] decided: run the body, or the call is answered from the record. */
    private sealed interface Begin {
        data object Run : Begin
        class Replay(val reply: String?) : Begin
    }

    /** Runs [proceed] (the handler's body) under the idempotency rules; returns its result, or the stored reply (a JSON tree). */
    suspend fun execute(handler: HandlerMethod, message: Any, ctx: MessageContext, proceed: suspend () -> Any?): Any? {
        val idempotency = handler.idempotency ?: return proceed()
        val label = handler.label
        val key = idempotency.keyOf(message, ctx, label)
        when (val begin = begin(key, idempotency.externalIdempotent, label)) {
            is Begin.Replay -> {
                metrics.externalCall(label, ExternalOutcome.REPLAYED)
                return begin.reply?.let { codec.decodeTree(it.toByteArray(Charsets.UTF_8)) } ?: Unit
            }
            Begin.Run -> Unit
        }
        val result = try {
            proceed()
        } catch (e: CancellationException) {
            throw e // whether the external system was reached is unknown: the record stays IN_PROGRESS and its lease decides
        } catch (e: Throwable) {
            settleFailure(key, e, label)
            throw e
        }
        try {
            short { mongo.updateFirst(byId(key), Update().set("status", ExternalCallStatus.SUCCEEDED).set("reply", String(codec.encode(result ?: Unit), Charsets.UTF_8)).set("updatedAt", clock.instant()).unset("leaseUntil").unset("error"), ExternalCallDocument::class.java) }
        } catch (e: Exception) {
            throw unrecorded(key, label, idempotency.externalIdempotent, e)
        }
        metrics.externalCall(label, ExternalOutcome.SUCCEEDED)
        return result
    }

    private fun begin(key: String, externalIdempotent: Boolean, label: String): Begin {
        repeat(MAX_BEGIN_ROUNDS) {
            val now = clock.instant()
            try {
                short { mongo.insert(ExternalCallDocument(key, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.plus(settings.lease))) }
                return Begin.Run
            } catch (_: DuplicateKeyException) {
                // an earlier call of this key exists
            }
            val existing = mongo.findById(key, ExternalCallDocument::class.java) ?: return@repeat // reset or expired meanwhile: insert again
            when (existing.status) {
                ExternalCallStatus.SUCCEEDED -> return Begin.Replay(existing.reply)
                ExternalCallStatus.FAILED -> {
                    metrics.externalCall(label, ExternalOutcome.FAILED)
                    throw ExternalCallFailedException(storedMessage(existing))
                }
                ExternalCallStatus.UNKNOWN -> throw unknown(key, label)
                ExternalCallStatus.IN_PROGRESS -> {
                    val lease = existing.leaseUntil
                    if (lease != null && lease.isAfter(now)) {
                        metrics.externalCall(label, ExternalOutcome.RETRYABLE)
                        throw RetryableExternalException("The call with key '$key' is already in progress (handler $label)")
                    }
                    val sameRecord = Criteria.where("_id").`is`(key).and("status").`is`(ExternalCallStatus.IN_PROGRESS)
                        .and("leaseUntil").`is`(lease)
                    if (externalIdempotent) {
                        val taken = short {
                            mongo.findAndModify(
                                Query(sameRecord),
                                Update().set("leaseUntil", now.plus(settings.lease)).set("updatedAt", now).inc("attempts", 1),
                                FindAndModifyOptions.options().returnNew(true), ExternalCallDocument::class.java,
                            )
                        }
                        if (taken != null) return Begin.Run
                    } else {
                        short {
                            mongo.updateFirst(Query(sameRecord), Update().set("status", ExternalCallStatus.UNKNOWN).set("updatedAt", now).unset("leaseUntil"), ExternalCallDocument::class.java)
                        }
                        throw unknown(key, label)
                    }
                }
            }
        }
        throw RetryableExternalException("The call with key '$key' kept changing under this call (handler $label)")
    }

    private fun unknown(key: String, label: String): ExternalCallUnknownOutcomeException {
        metrics.externalCall(label, ExternalOutcome.UNKNOWN)
        return ExternalCallUnknownOutcomeException("The outcome of the call with key '$key' is unknown: an earlier call of handler $label did not finish and the external system may have acted")
    }

    /**
     * The body returned but its `SUCCEEDED` could not be written (the store is unreachable): the external system acted and the
     * record cannot say so, which is an unknown outcome and never a failed call. With `externalIdempotent` the partner
     * de-duplicates a repeat, so it is retryable (the record stays `IN_PROGRESS`, its lease settles it); otherwise the record
     * is moved to `UNKNOWN` (best effort: if even that fails the lease does it later) and the call reports the unknown outcome.
     */
    private fun unrecorded(key: String, label: String, externalIdempotent: Boolean, cause: Exception): RuntimeException {
        log.error("The call with key '{}' (handler {}) ran but its outcome could not be recorded: {}", key, label, cause.toString())
        if (externalIdempotent) {
            metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            return RetryableExternalException("The call with key '$key' (handler $label) ran but its outcome could not be recorded; a repeat is safe because the partner de-duplicates by the key: $cause")
        }
        markUnknown(key)
        return unknown(key, label)
    }

    private fun markUnknown(key: String) {
        try {
            short { mongo.updateFirst(byId(key), Update().set("status", ExternalCallStatus.UNKNOWN).set("updatedAt", clock.instant()).unset("leaseUntil"), ExternalCallDocument::class.java) }
        } catch (bookkeeping: Exception) {
            log.warn("Could not mark the call with key '{}' UNKNOWN (its lease will): {}", key, bookkeeping.toString())
        }
    }

    /** The record after the body failed: reset for a retryable failure, FAILED with the error for any other. Never masks the failure. */
    private fun settleFailure(key: String, e: Throwable, label: String) {
        try {
            if (e is RetryableExternalException) {
                short { mongo.remove(byId(key), ExternalCallDocument::class.java) }
                metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            } else if (e is ExternalCallUnknownOutcomeException) {
                // The adapter itself cannot tell whether the partner acted (a connection broke after the request went out):
                // not FAILED (a redelivery would report a plain failure and a person would think it safe to call again).
                markUnknown(key)
                metrics.externalCall(label, ExternalOutcome.UNKNOWN)
            } else {
                val error = String(codec.encode(mapOf("code" to "HANDLER_ERROR", "message" to describe(e))), Charsets.UTF_8)
                short {
                    mongo.updateFirst(byId(key), Update().set("status", ExternalCallStatus.FAILED).set("error", error).set("updatedAt", clock.instant()).unset("leaseUntil"), ExternalCallDocument::class.java)
                }
                metrics.externalCall(label, ExternalOutcome.FAILED)
            }
        } catch (bookkeeping: Exception) {
            log.warn("Could not record the failure of the call with key '{}' (handler {}): {}", key, label, bookkeeping.toString())
        }
    }

    private fun storedMessage(doc: ExternalCallDocument): String =
        doc.error?.let { runCatching { codec.decodeTree(it.toByteArray(Charsets.UTF_8)).get("message")?.asString() }.getOrNull() }
            ?: "The call with key '${doc.id}' failed earlier"

    private fun describe(e: Throwable) = "${e::class.simpleName}: ${e.message}"

    private fun byId(key: String) = Query(Criteria.where("_id").`is`(key))

    /** [block] in a transaction of its own (when there is a transaction manager), apart from any transaction the caller is in. */
    private fun <T> short(block: () -> T): T {
        val template = own ?: return block()
        return template.execute { block() } // T may be null (a findAndModify that found nothing): no !!
    }

    private companion object {
        const val MAX_BEGIN_ROUNDS = 3
    }
}
