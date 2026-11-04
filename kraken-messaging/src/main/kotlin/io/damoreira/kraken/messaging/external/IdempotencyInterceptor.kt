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
 * A call owns the record while it is `IN_PROGRESS` with the `attempts` value the call set in step 1 (1 on insert, the
 * incremented value on a take-over): every write after step 1 filters on `{_id, status: IN_PROGRESS, attempts}`, so a call
 * whose lease ran out never deletes, settles or overwrites the record of the call that took over or gave it up. A write
 * that matches nothing is logged at warn; a body that returned is then not reported as a success (see [lost]).
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
        /** Run the body; [attempts] is the record's `attempts` as this call set it, its ownership token for every later write. */
        class Run(val attempts: Int) : Begin
        class Replay(val reply: String?) : Begin
    }

    /** Runs [proceed] (the handler's body) under the idempotency rules; returns its result, or the stored reply (a JSON tree). */
    suspend fun execute(handler: HandlerMethod, message: Any, ctx: MessageContext, proceed: suspend () -> Any?): Any? {
        val idempotency = handler.idempotency ?: return proceed()
        val label = handler.label
        val key = idempotency.keyOf(message, ctx, label)
        val call = when (val begin = begin(key, idempotency.externalIdempotent, label)) {
            is Begin.Replay -> {
                metrics.externalCall(label, ExternalOutcome.REPLAYED)
                return begin.reply?.let { codec.decodeTree(it.toByteArray(Charsets.UTF_8)) } ?: Unit
            }
            is Begin.Run -> Owned(key, begin.attempts)
        }
        val result = try {
            proceed()
        } catch (e: CancellationException) {
            throw e // whether the external system was reached is unknown: the record stays IN_PROGRESS and its lease decides
        } catch (e: Throwable) {
            settleFailure(call, e, label)
            throw e
        }
        val recorded = try {
            short { mongo.updateFirst(call.query(), Update().set("status", ExternalCallStatus.SUCCEEDED).set("reply", String(codec.encode(result ?: Unit), Charsets.UTF_8)).set("updatedAt", clock.instant()).unset("leaseUntil").unset("error"), ExternalCallDocument::class.java) }.matchedCount > 0
        } catch (e: Exception) {
            throw unrecorded(call, label, idempotency.externalIdempotent, e)
        }
        if (!recorded) throw lost(call, label, idempotency.externalIdempotent)
        metrics.externalCall(label, ExternalOutcome.SUCCEEDED)
        return result
    }

    /** The record as this call owns it: `IN_PROGRESS` with the [attempts] the call set. */
    private class Owned(val key: String, val attempts: Int) {
        fun query() = Query(Criteria.where("_id").`is`(key).and("status").`is`(ExternalCallStatus.IN_PROGRESS).and("attempts").`is`(attempts))
    }

    private fun begin(key: String, externalIdempotent: Boolean, label: String): Begin {
        repeat(MAX_BEGIN_ROUNDS) {
            val now = clock.instant()
            try {
                short { mongo.insert(ExternalCallDocument(key, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.plus(settings.lease))) }
                return Begin.Run(attempts = 1)
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
                        if (taken != null) return Begin.Run(taken.attempts)
                    } else {
                        val givenUp = short {
                            mongo.updateFirst(Query(sameRecord), Update().set("status", ExternalCallStatus.UNKNOWN).set("updatedAt", now).unset("leaseUntil"), ExternalCallDocument::class.java)
                        }.matchedCount > 0
                        if (givenUp) throw unknown(key, label)
                        // The record changed since it was read (the slow call settled it, or another call gave it up or took it
                        // over): read it again rather than report an unknown outcome over what it says now.
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
    private fun unrecorded(call: Owned, label: String, externalIdempotent: Boolean, cause: Exception): RuntimeException {
        log.error("The call with key '{}' (handler {}) ran but its outcome could not be recorded: {}", call.key, label, cause.toString())
        if (externalIdempotent) {
            metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            return RetryableExternalException("The call with key '${call.key}' (handler $label) ran but its outcome could not be recorded; a repeat is safe because the partner de-duplicates by the key: $cause")
        }
        markUnknown(call, label)
        return unknown(call.key, label)
    }

    /**
     * The body returned but the record is no longer this call's (its lease ran out and another call took it over or gave
     * it up as `UNKNOWN`): the success cannot be recorded, so it is not reported. With `externalIdempotent` the repeat is
     * safe (the partner de-duplicates) and finds the other call's outcome; otherwise the outcome is unknown, as the record says.
     */
    private fun lost(call: Owned, label: String, externalIdempotent: Boolean): RuntimeException {
        log.warn("The call with key '{}' (handler {}) ran but no longer owns its record (attempt {}): its lease ran out and another call took it over or gave it up", call.key, label, call.attempts)
        if (externalIdempotent) {
            metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            return RetryableExternalException("The call with key '${call.key}' (handler $label) ran after its lease ran out and another call owns its record now; a repeat is safe because the partner de-duplicates by the key")
        }
        return unknown(call.key, label)
    }

    private fun markUnknown(call: Owned, label: String) {
        try {
            val marked = short { mongo.updateFirst(call.query(), Update().set("status", ExternalCallStatus.UNKNOWN).set("updatedAt", clock.instant()).unset("leaseUntil"), ExternalCallDocument::class.java) }.matchedCount > 0
            if (!marked) notOwned(call, label, "UNKNOWN")
        } catch (bookkeeping: Exception) {
            log.warn("Could not mark the call with key '{}' UNKNOWN (its lease will): {}", call.key, bookkeeping.toString())
        }
    }

    /** The record after the body failed: reset for a retryable failure, FAILED with the error for any other. Never masks the failure. */
    private fun settleFailure(call: Owned, e: Throwable, label: String) {
        try {
            if (e is RetryableExternalException) {
                val reset = short { mongo.remove(call.query(), ExternalCallDocument::class.java) }.deletedCount > 0
                if (!reset) notOwned(call, label, "reset")
                metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            } else if (e is ExternalCallUnknownOutcomeException) {
                // The adapter itself cannot tell whether the partner acted (a connection broke after the request went out):
                // not FAILED (a redelivery would report a plain failure and a person would think it safe to call again).
                markUnknown(call, label)
                metrics.externalCall(label, ExternalOutcome.UNKNOWN)
            } else {
                val error = String(codec.encode(mapOf("code" to "HANDLER_ERROR", "message" to describe(e))), Charsets.UTF_8)
                val failed = short {
                    mongo.updateFirst(call.query(), Update().set("status", ExternalCallStatus.FAILED).set("error", error).set("updatedAt", clock.instant()).unset("leaseUntil"), ExternalCallDocument::class.java)
                }.matchedCount > 0
                if (!failed) notOwned(call, label, "FAILED")
                metrics.externalCall(label, ExternalOutcome.FAILED)
            }
        } catch (bookkeeping: Exception) {
            log.warn("Could not record the failure of the call with key '{}' (handler {}): {}", call.key, label, bookkeeping.toString())
        }
    }

    private fun notOwned(call: Owned, label: String, write: String) =
        log.warn("The call with key '{}' (handler {}) no longer owns its record (attempt {}), so it is not {}: its lease ran out and another call took it over or gave it up", call.key, label, call.attempts, write)

    private fun storedMessage(doc: ExternalCallDocument): String =
        doc.error?.let { runCatching { codec.decodeTree(it.toByteArray(Charsets.UTF_8)).get("message")?.asString() }.getOrNull() }
            ?: "The call with key '${doc.id}' failed earlier"

    private fun describe(e: Throwable) = "${e::class.simpleName}: ${e.message}"

    /** [block] in a transaction of its own (when there is a transaction manager), apart from any transaction the caller is in. */
    private fun <T> short(block: () -> T): T {
        val template = own ?: return block()
        return template.execute { block() } // T may be null (a findAndModify that found nothing): no !!
    }

    private companion object {
        const val MAX_BEGIN_ROUNDS = 3
    }
}
