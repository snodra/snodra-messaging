package io.damoreira.kraken.messaging

import java.time.Duration
import kotlin.random.Random

/**
 * How one command or query is sent over the hub. Every field is optional; `null` keeps the configured default.
 *
 * @property priority `Request.priority`: higher first when the hub queues requests for a busy, flow-controlled handler
 *   instance (equal priorities keep arrival order). Default: `kraken.messaging.hub.priorities.<type>`, else 0.
 * @property timeout the caller's deadline, sent as `timeout_ms` (the hub still clamps it). Default:
 *   `kraken.messaging.hub.request-timeout`, else the hub decides (its default or per-type override). With retry it is the
 *   total budget of all attempts. Zero counts as unset.
 * @property routingKey commands only: same key, same owning instance (consistent hashing on the hub).
 * @property retry the [RetryPolicy] for this call; [RetryPolicy.NONE] disables a configured one.
 *   Default: `kraken.messaging.hub.retry.*` when enabled, else no retry.
 */
data class SendOptions(
    val priority: Int? = null,
    val timeout: Duration? = null,
    val routingKey: String? = null,
    val retry: RetryPolicy? = null,
)

/** Whether a message is a command (may have side effects) or a query (read only); decides what may be retried. */
enum class MessageKind { COMMAND, QUERY }

/**
 * Retries a failed send with exponential backoff, opt-in (`kraken.messaging.hub.retry.enabled`, or
 * [SendOptions.retry]). Only errors [RetryClassification] calls retryable are retried; every attempt is a new hub
 * request with a fresh id; all attempts together stay within the caller's time budget (see [SendOptions.timeout]), and
 * a retry is started only while at least `max(initialBackoff, MIN_ATTEMPT_BUDGET)` of it is left after the backoff.
 *
 * Any `RetryPolicy` object is in force as given: there is no separate on/off switch on the policy, and
 * `maxAttempts = 1` ([NONE]) is a single attempt, i.e. no retry. Invalid values are rejected on construction (and a
 * misconfigured `kraken.messaging.hub.retry.*` fails the startup, even while retry is disabled).
 *
 * @property maxAttempts total attempts, the first one included (1 = no retry)
 * @property initialBackoff wait before the second attempt
 * @property multiplier growth of the wait per further attempt (>= 1)
 * @property maxBackoff upper bound of any single wait
 */
data class RetryPolicy(
    val maxAttempts: Int = 3,
    val initialBackoff: Duration = Duration.ofMillis(100),
    val multiplier: Double = 2.0,
    val maxBackoff: Duration = Duration.ofSeconds(2),
) {
    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, was $maxAttempts" }
        require(multiplier >= 1.0) { "multiplier must be >= 1, was $multiplier" }
        require(!initialBackoff.isNegative && !maxBackoff.isNegative) { "backoffs must not be negative" }
    }

    /**
     * The base wait before attempt number [attempt] (2 for the first retry), without jitter:
     * `initialBackoff * multiplier^(attempt - 2)`, capped at [maxBackoff].
     */
    fun backoffBefore(attempt: Int): Duration {
        require(attempt >= 2) { "attempt 1 has no backoff" }
        val ms = initialBackoff.toMillis() * Math.pow(multiplier, (attempt - 2).toDouble())
        return Duration.ofMillis(minOf(ms, maxBackoff.toMillis().toDouble()).toLong())
    }

    /**
     * The wait the bus actually sleeps before attempt number [attempt]: `min(maxBackoff, base * U(0.8, 1.2))` with the
     * base from [backoffBefore]. Below the cap it is the base +-20 %; at the cap it is 80-100 % of [maxBackoff], so
     * callers that fail together do not retry in lockstep (like the reconnect backoff).
     */
    fun jitteredBackoffBefore(attempt: Int, random: Random = Random.Default): Duration {
        val base = backoffBefore(attempt)
        val jittered = (base.toNanos() * random.nextDouble(0.8, 1.2)).toLong()
        return Duration.ofNanos(minOf(jittered, maxBackoff.toNanos()))
    }

    companion object {
        /** One attempt, no retry: disables a configured policy for a single call. */
        val NONE = RetryPolicy(maxAttempts = 1)

        /**
         * A retry is started only if at least `max(initialBackoff, MIN_ATTEMPT_BUDGET)` of the budget is left after its
         * backoff, so no attempt is sent with a near-zero `timeout_ms`.
         */
        val MIN_ATTEMPT_BUDGET: Duration = Duration.ofMillis(250)
    }
}

/**
 * Which failures may be retried. Shared by the bus retry and, later, the saga relay, so both agree.
 *
 * - Both kinds: [HubUnavailableException] (never reached the hub), [NoHandlerException] (nobody ran it),
 *   [HandlerBusyException] (rejected before any handler ran).
 * - Queries only: [HandlerDisconnectedException], [MessageTimeoutException], [HubConnectionLostException]. A command in
 *   those states may have run, so retrying it could apply it twice.
 * - Nothing else: handler errors, authentication and authorization failures, invalid messages, other exceptions.
 */
object RetryClassification {
    fun isRetryable(kind: MessageKind, error: Throwable): Boolean = when (error) {
        is HubUnavailableException, is NoHandlerException, is HandlerBusyException -> true
        is HandlerDisconnectedException, is MessageTimeoutException, is HubConnectionLostException -> kind == MessageKind.QUERY
        else -> false
    }
}
