package io.damoreira.kraken.messaging.kafka

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

/**
 * Idempotency marker of a consumed event; [id] is the marker id ([io.damoreira.kraken.messaging.handler.Processors.markerId]
 * for stage 1). Expires via the TTL index on `processedAt`. A stage-1 marker of a Kafka record (and of a dead letter's retry)
 * also carries the record's [processor], [topic], [partition] and [offset] (P6b, ADR 0023), so a reset deletes exactly the
 * markers of its replay range; every other marker leaves them out (null is not written).
 */
@Document("processed_events")
data class ProcessedEventDocument(
    @Id val id: String,
    val type: String,
    val processedAt: Instant,
    val processor: String? = null,
    val topic: String? = null,
    val partition: Int? = null,
    val offset: Long? = null,
)

/** Where a Kafka record is: its stage-1 marker carries it (P6b). */
data class RecordPosition(val topic: String, val partition: Int, val offset: Long)
