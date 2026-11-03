package io.damoreira.kraken.messaging.external

/**
 * Thrown by an adapter handler when the external system failed in a way that a later attempt may cure (a timeout, a 503,
 * a rate limit). Adapters do not retry internally: the saga dispatch relay owns the backoff, so retries never stack and no
 * thread sleeps inside an adapter. Under `@IdempotentHandler` the call's record is reset, so the next attempt runs the body.
 */
open class RetryableExternalException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
