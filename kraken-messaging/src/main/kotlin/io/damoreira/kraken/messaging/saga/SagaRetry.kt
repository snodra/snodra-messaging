package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.RetryPolicy
import java.time.Duration

/**
 * The bounded retry of one saga event inside stage 2 (S1 plan R4): version conflicts, start-key duplicates and handler
 * exceptions are retried up to [policy].maxAttempts attempts in all, with the policy's jittered exponential backoff
 * (default 5 attempts, 200 ms doubling up to 5 s); the event is then parked. [sleeper] is replaceable for tests.
 */
class SagaRetry(
    val policy: RetryPolicy = RetryPolicy(maxAttempts = 5, initialBackoff = Duration.ofMillis(200), multiplier = 2.0, maxBackoff = Duration.ofSeconds(5)),
    private val sleeper: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    val maxAttempts: Int get() = policy.maxAttempts

    /** Waits before attempt number [attempt] (2 or more). */
    fun pauseBefore(attempt: Int) = sleeper(policy.jitteredBackoffBefore(attempt))
}
