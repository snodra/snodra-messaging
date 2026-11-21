package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.RetryPolicy
import java.time.Duration

/**
 * The bounded retry of one saga event inside stage 2. Only a **conflict** (a
 * version check, a start-key duplicate, a write conflict or transient error of the store) backs off: it is retried up
 * to [policy].maxAttempts attempts in all, with the policy's jittered exponential backoff (default 5 attempts, 200 ms
 * doubling up to 5 s). Any other exception, a handler's above all, is not cured by waiting: it gets at most
 * [HANDLER_ATTEMPTS] attempts, [pauseBeforeHandlerRetry] (at most [QUICK_PAUSE_MS]) apart, and the event is then parked.
 * [sleeper] is replaceable for tests.
 */
class SagaRetry(
    val policy: RetryPolicy = RetryPolicy(maxAttempts = 5, initialBackoff = Duration.ofMillis(200), multiplier = 2.0, maxBackoff = Duration.ofSeconds(5)),
    private val sleeper: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    val maxAttempts: Int get() = policy.maxAttempts

    /** Waits before attempt number [attempt] (2 or more) after a conflict. */
    fun pauseBefore(attempt: Int) = sleeper(policy.jitteredBackoffBefore(attempt))

    /** The short wait before the one quick repeat of an attempt that failed for a reason other than a conflict. */
    fun pauseBeforeHandlerRetry() = sleeper(Duration.ofMillis(minOf(policy.initialBackoff.toMillis(), QUICK_PAUSE_MS)))

    companion object {
        /** Attempts in all for an exception that is not a conflict: the original and one repeat, in case it was a blip. */
        const val HANDLER_ATTEMPTS = 2
        const val QUICK_PAUSE_MS = 50L
    }
}
