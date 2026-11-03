package io.damoreira.kraken.messaging.external

/**
 * How an adapter's failure kind crosses the wire. A handler error travels as `HANDLER_ERROR` with a message and opaque
 * `details`; the invoker puts one of these two small JSON objects in `details` for [RetryableExternalException] and
 * [ExternalCallUnknownOutcomeException], and the saga relay reads them to tell a transient failure and an unknown outcome
 * from an ordinary handler failure, whether the adapter ran locally or in another service.
 */
object ExternalFailureMarkers {
    private const val RETRYABLE_JSON = """{"kraken":"retryable"}"""
    private const val UNKNOWN_OUTCOME_JSON = """{"kraken":"unknown-outcome"}"""

    val RETRYABLE: ByteArray get() = RETRYABLE_JSON.toByteArray(Charsets.UTF_8)
    val UNKNOWN_OUTCOME: ByteArray get() = UNKNOWN_OUTCOME_JSON.toByteArray(Charsets.UTF_8)

    fun isRetryable(details: ByteArray): Boolean = String(details, Charsets.UTF_8).trim() == RETRYABLE_JSON
    fun isUnknownOutcome(details: ByteArray): Boolean = String(details, Charsets.UTF_8).trim() == UNKNOWN_OUTCOME_JSON
}
