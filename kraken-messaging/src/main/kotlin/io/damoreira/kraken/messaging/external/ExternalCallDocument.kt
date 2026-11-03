package io.damoreira.kraken.messaging.external

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

/** Where an external call stands in `external_calls`; see [ExternalCallDocument]. */
enum class ExternalCallStatus { IN_PROGRESS, SUCCEEDED, FAILED, UNKNOWN }

/**
 * The record of one idempotent external call (`@IdempotentHandler`), keyed by its idempotency key ([id]). [reply] is the
 * JSON text of the handler's reply (any JSON value) and [error] the JSON text of a `FAILED` call's error (`{"code", "message"}`,
 * added by the S1 plan). An `IN_PROGRESS` record whose [leaseUntil] (added by the S1 plan) has passed means the outcome is
 * unknown. Expires through the TTL index on [updatedAt].
 */
@Document("external_calls")
data class ExternalCallDocument(
    @Id val id: String,
    val status: ExternalCallStatus,
    val reply: String? = null,
    val error: String? = null,
    val attempts: Int = 0,
    val updatedAt: Instant,
    val leaseUntil: Instant? = null,
)
