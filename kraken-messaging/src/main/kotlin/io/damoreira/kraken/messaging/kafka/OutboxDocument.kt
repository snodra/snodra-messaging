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
    val sentAt: Instant? = null,
    val attempts: Int = 0,
)
