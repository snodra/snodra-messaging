package com.damoreira.snodra.messaging.external

/** A later call of an `@IdempotentHandler` key whose first call failed for good: the stored error again (permanent). */
class ExternalCallFailedException(message: String) : RuntimeException(message)

/**
 * An `@IdempotentHandler` call crashed in the middle and its outcome is unknown (the external system may or may not have
 * acted): the body is not called again. The saga relay reports it as `SagaCommandFailed(reason = UNKNOWN_OUTCOME)`.
 */
class ExternalCallUnknownOutcomeException(message: String) : RuntimeException(message)
