package io.damoreira.kraken.messaging.kafka

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

/** Idempotency marker of a consumed event; [id] is the event id. Expires via the TTL index on `processedAt`. */
@Document("processed_events")
data class ProcessedEventDocument(
    @Id val id: String,
    val type: String,
    val processedAt: Instant,
)
