package io.damoreira.kraken.messaging.kafka

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

/** One event waiting in (or already relayed from) the `outbox` collection. */
@Document("outbox")
data class OutboxDocument(
    @Id val id: String,
    val type: String,
    val key: String?,
    val headers: Map<String, String>,
    val payload: org.bson.Document,
    val createdAt: Instant,
    /** Tie-breaker for events created in the same millisecond; see [OutboxEventBus]. */
    val seq: Long,
    val sentAt: Instant? = null,
    val attempts: Int = 0,
    /** Set when the document was given up on (non-retriable failure or `outbox.max-attempts`); excluded from relaying. */
    val failedAt: Instant? = null,
)
