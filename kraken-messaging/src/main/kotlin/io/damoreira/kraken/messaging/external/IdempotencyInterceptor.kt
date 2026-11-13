package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.handler.HandlerMethod
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics.ExternalOutcome
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.MessageStore
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException

/** Settings of `external_calls` (`kraken.messaging.external.calls.*`): how long an `IN_PROGRESS` call may take before its outcome counts as unknown. */
data class ExternalCallSettings(val lease: Duration = Duration.ofMinutes(5)) {
    init {
        require(!lease.isZero && !lease.isNegative) { "lease must be positive, was $lease" }
    }
}

/**
 * Runs the handlers annotated with [IdempotentHandler] (the invoker hands each call to [execute]): the call is recorded in
 * the message store's external calls ([MessageStore.externalCalls], `external_calls`) under its key, so a repeat of the key
 * never runs the body twice.
 *
 * 1. The record is inserted `IN_PROGRESS` with `leaseUntil = now + lease`; a duplicate key means an earlier call exists.
 * 2. An earlier `SUCCEEDED` call returns its stored reply, `FAILED` throws [ExternalCallFailedException] (permanent), `UNKNOWN`
 *    throws [ExternalCallUnknownOutcomeException]. An `IN_PROGRESS` one within its lease is another call in flight: a
 *    [RetryableExternalException] (come back later). Past its lease the outcome is unknown: without `externalIdempotent`
 *    it becomes `UNKNOWN` (never called again automatically) and the call throws [ExternalCallUnknownOutcomeException],
 *    with it the call takes the record over (`attempts + 1`, its own `owner`) and runs the body.
 * 3. After the body: `SUCCEEDED` with the reply as JSON (when that write fails the call is an unknown outcome, never a
 *    failed one: see [unrecorded]); a [RetryableExternalException] deletes the record (the next call starts afresh, the relay
 *    owns the backoff); an [ExternalCallUnknownOutcomeException] thrown by the adapter itself leaves the record `UNKNOWN`
 *    and is thrown as it is; any other failure is `FAILED` with `{"code","message"}` and is thrown as it is.
 *
 * A call owns the record while it is `IN_PROGRESS` with the `owner` token the call wrote in step 1 (a random one per call,
 * on insert and on a take-over; `attempts` would repeat once a record was deleted and inserted afresh): every write after step 1 filters on `{_id, status: IN_PROGRESS, owner}` and a settling write removes
 * the token, so a call whose lease ran out never deletes, settles or overwrites the record of the call that took over, gave
 * it up, or inserted it again after a reset. A write that matches nothing is logged at warn; a body that returned is then
 * not reported as a success (see [lost]).
 *
 * Every write of the record is its own short transaction (REQUIRES_NEW over [MessageStore.transactions]), so a rollback of
 * the caller's transaction (an event handler runs inside one) does not forget a call that happened. The body itself runs in
 * whatever transaction the caller has: an `@EventHandler`'s inside the dispatcher's stage-1 transaction, a `@CommandHandler`'s
 * (what the saga relay calls) in none.
 */
class IdempotencyInterceptor(
    store: MessageStore,
    private val codec: JsonCodec,
    private val clock: Clock = Clock.systemUTC(),
    private val settings: ExternalCallSettings = ExternalCallSettings(),
    private val metrics: SagaMetrics = NoopSagaMetrics,
) {
    private val log = LoggerFactory.getLogger(IdempotencyInterceptor::class.java)
    private val calls = store.externalCalls
    private val own = TransactionTemplate(store.transactions).apply { propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW }

    /** What [begin] decided: run the body, or the call is answered from the record. */
    private sealed interface Begin {
        /** Run the body; [owner] is the token this call wrote, its ownership token for every later write; [attempts] the record's count. */
        class Run(val owner: String, val attempts: Int) : Begin
        class Replay(val reply: ByteArray?) : Begin
    }

    /** Runs [proceed] (the handler's body) under the idempotency rules; returns its result, or the stored reply (a JSON tree). */
    suspend fun execute(handler: HandlerMethod, message: Any, ctx: MessageContext, proceed: suspend () -> Any?): Any? {
        val idempotency = handler.idempotency ?: return proceed()
        val label = handler.label
        val key = idempotency.keyOf(message, ctx, label)
        val call = when (val begin = begin(key, idempotency.externalIdempotent, label)) {
            is Begin.Replay -> {
                metrics.externalCall(label, ExternalOutcome.REPLAYED)
                return begin.reply?.let { codec.decodeTree(it) } ?: Unit
            }
            is Begin.Run -> Owned(key, begin.owner, begin.attempts)
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
            short { calls.succeed(call.key, call.owner, codec.encode(result ?: Unit), clock.instant()) }
        } catch (e: Exception) {
            throw unrecorded(call, label, idempotency.externalIdempotent, e)
        }
        if (!recorded) throw lost(call, label, idempotency.externalIdempotent)
        metrics.externalCall(label, ExternalOutcome.SUCCEEDED)
        return result
    }

    /** The record as this call owns it: `IN_PROGRESS` with the [owner] token the call wrote ([attempts] is for the logs). */
    private class Owned(val key: String, val owner: String, val attempts: Int)

    private suspend fun begin(key: String, externalIdempotent: Boolean, label: String): Begin {
        repeat(MAX_BEGIN_ROUNDS) {
            val now = clock.instant()
            val owner = UUID.randomUUID().toString()
            try {
                short {
                    val record = ExternalCallRecord(key, ExternalCallStatus.IN_PROGRESS, attempts = 1, updatedAt = now, leaseUntil = now.plus(settings.lease), owner = owner)
                    // A duplicate may have aborted the store's transaction: roll it back rather than commit it.
                    if (!calls.insert(record)) throw KeyTaken
                }
                return Begin.Run(owner, attempts = 1)
            } catch (_: KeyTaken) {
                // an earlier call of this key exists
            }
            val existing = calls.find(key) ?: return@repeat // reset or expired meanwhile: insert again
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
                    if (externalIdempotent) {
                        val taken = short { calls.takeOver(key, existing.owner, lease, owner, now.plus(settings.lease), now) }
                        if (taken != null) return Begin.Run(owner, taken.attempts)
                    } else {
                        val givenUp = short { calls.giveUp(key, existing.owner, lease, now) }
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
    private suspend fun unrecorded(call: Owned, label: String, externalIdempotent: Boolean, cause: Exception): RuntimeException {
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
        log.warn("The call with key '{}' (handler {}) ran but no longer owns its record (attempt {}): its lease ran out and another call took it over, gave it up or reset it", call.key, label, call.attempts)
        if (externalIdempotent) {
            metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            return RetryableExternalException("The call with key '${call.key}' (handler $label) ran after its lease ran out and another call owns its record now; a repeat is safe because the partner de-duplicates by the key")
        }
        return unknown(call.key, label)
    }

    private suspend fun markUnknown(call: Owned, label: String) {
        try {
            val marked = short { calls.markUnknown(call.key, call.owner, clock.instant()) }
            if (!marked) notOwned(call, label, "UNKNOWN")
        } catch (bookkeeping: Exception) {
            log.warn("Could not mark the call with key '{}' UNKNOWN (its lease will): {}", call.key, bookkeeping.toString())
        }
    }

    /** The record after the body failed: reset for a retryable failure, FAILED with the error for any other. Never masks the failure. */
    private suspend fun settleFailure(call: Owned, e: Throwable, label: String) {
        try {
            if (e is RetryableExternalException) {
                val reset = short { calls.reset(call.key, call.owner) }
                if (!reset) notOwned(call, label, "reset")
                metrics.externalCall(label, ExternalOutcome.RETRYABLE)
            } else if (e is ExternalCallUnknownOutcomeException) {
                // The adapter itself cannot tell whether the partner acted (a connection broke after the request went out):
                // not FAILED (a redelivery would report a plain failure and a person would think it safe to call again).
                markUnknown(call, label)
                metrics.externalCall(label, ExternalOutcome.UNKNOWN)
            } else {
                val error = codec.encode(mapOf("code" to "HANDLER_ERROR", "message" to describe(e)))
                val failed = short { calls.fail(call.key, call.owner, error, clock.instant()) }
                if (!failed) notOwned(call, label, "FAILED")
                metrics.externalCall(label, ExternalOutcome.FAILED)
            }
        } catch (bookkeeping: Exception) {
            log.warn("Could not record the failure of the call with key '{}' (handler {}): {}", call.key, label, bookkeeping.toString())
        }
    }

    private fun notOwned(call: Owned, label: String, write: String) =
        log.warn("The call with key '{}' (handler {}) no longer owns its record (attempt {}), so it is not {}: its lease ran out and another call took it over, gave it up or reset it", call.key, label, call.attempts, write)

    private fun storedMessage(call: ExternalCallRecord): String =
        call.error?.let { runCatching { codec.decodeTree(it).get("message")?.asString() }.getOrNull() }
            ?: "The call with key '${call.id}' failed earlier"

    private fun describe(e: Throwable) = "${e::class.simpleName}: ${e.message}"

    /**
     * [block] in a transaction of its own, apart from any transaction the caller is in, on the calling thread (where Spring
     * binds the transaction the store joins).
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T> short(block: suspend () -> T): T = own.execute { runBlocking { block() } } as T // T may be null (a take-over that matched nothing)

    /** Thrown inside the insert's transaction when the key has a record: rolls that transaction back. */
    private object KeyTaken : RuntimeException("an earlier call of this key exists", null, false, false) {
        private fun readResolve(): Any = KeyTaken
    }

    private companion object {
        const val MAX_BEGIN_ROUNDS = 3
    }
}
