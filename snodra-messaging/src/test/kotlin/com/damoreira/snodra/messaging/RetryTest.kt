package com.damoreira.snodra.messaging

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RetryTest {
    private val retryableForBoth = listOf(
        HubUnavailableException("down"), NoHandlerException("t"), HandlerBusyException("t"),
    )
    private val retryableForQueriesOnly = listOf(
        HandlerDisconnectedException("t"), MessageTimeoutException("t"), HubConnectionLostException("lost"),
    )
    private val neverRetryable = listOf(
        RemoteHandlerException("t", "boom", ByteArray(0)), UnauthenticatedException("no"), AccessDeniedException("no"),
        InvalidMessageException("bad"), HubRegistrationRejectedException("no"), IllegalStateException("not ours"),
    )

    @Test fun `classification table`() {
        for (e in retryableForBoth) {
            assertTrue(RetryClassification.isRetryable(MessageKind.COMMAND, e), "command ${e::class.simpleName}")
            assertTrue(RetryClassification.isRetryable(MessageKind.QUERY, e), "query ${e::class.simpleName}")
        }
        for (e in retryableForQueriesOnly) {
            // The command may have run: retrying it could apply it twice.
            assertFalse(RetryClassification.isRetryable(MessageKind.COMMAND, e), "command ${e::class.simpleName}")
            assertTrue(RetryClassification.isRetryable(MessageKind.QUERY, e), "query ${e::class.simpleName}")
        }
        for (e in neverRetryable) {
            assertFalse(RetryClassification.isRetryable(MessageKind.COMMAND, e), "command ${e::class.simpleName}")
            assertFalse(RetryClassification.isRetryable(MessageKind.QUERY, e), "query ${e::class.simpleName}")
        }
    }

    @Test fun `backoff grows by the multiplier up to the maximum`() {
        val policy = RetryPolicy(maxAttempts = 6, initialBackoff = Duration.ofMillis(100), multiplier = 2.0, maxBackoff = Duration.ofMillis(500))
        assertEquals(listOf(100L, 200L, 400L, 500L, 500L), (1..5).map { policy.backoffBefore(it + 1).toMillis() })
    }

    @Test fun `jittered backoff lies within 20 percent of the base and never above the maximum`() {
        val policy = RetryPolicy(maxAttempts = 10, initialBackoff = Duration.ofMillis(100), multiplier = 2.0, maxBackoff = Duration.ofMillis(450))
        val random = Random(11)
        for (attempt in 2..7) { // bases 100, 200, 400, 450 (capped), 450, 450
            val base = policy.backoffBefore(attempt)
            val low = base.multipliedBy(8).dividedBy(10)
            val high = minOf(base.multipliedBy(12).dividedBy(10), policy.maxBackoff)
            val samples = (1..2_000).map { policy.jitteredBackoffBefore(attempt, random) }
            assertTrue(samples.all { it in low..high }, "attempt $attempt: ${samples.min()}..${samples.max()} not in $low..$high")
            assertTrue(samples.toSet().size > 1, "attempt $attempt: every wait is $base")
        }
    }

    @Test fun `defaults and the no-retry policy`() {
        val defaults = RetryPolicy()
        assertEquals(3, defaults.maxAttempts)
        assertEquals(Duration.ofMillis(100), defaults.initialBackoff)
        assertEquals(2.0, defaults.multiplier)
        assertEquals(Duration.ofSeconds(2), defaults.maxBackoff)
        assertEquals(1, RetryPolicy.NONE.maxAttempts)
        assertFailsWith<IllegalArgumentException> { RetryPolicy(maxAttempts = 0) }
        assertFailsWith<IllegalArgumentException> { RetryPolicy(multiplier = 0.5) }
    }
}
