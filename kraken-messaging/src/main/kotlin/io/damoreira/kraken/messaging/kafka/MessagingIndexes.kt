package io.damoreira.kraken.messaging.kafka

import org.springframework.beans.factory.InitializingBean
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import java.time.Duration

/** Ensures the outbox relay index and the TTL indexes of `outbox` and `processed_events` exist. Idempotent. */
class MessagingIndexes(
    private val mongo: MongoTemplate,
    private val outboxRetention: Duration,
    private val idempotencyRetention: Duration,
) : InitializingBean {
    override fun afterPropertiesSet() {
        mongo.indexOps("outbox").createIndex(
            Index().on("sentAt", Sort.Direction.ASC).on("createdAt", Sort.Direction.ASC).named("sentAt_1_createdAt_1"),
        )
        // Documents with sentAt == null are never expired by the TTL monitor.
        mongo.indexOps("outbox").createIndex(
            Index().on("sentAt", Sort.Direction.ASC).expire(outboxRetention).named("sentAt_ttl"),
        )
        mongo.indexOps("processed_events").createIndex(
            Index().on("processedAt", Sort.Direction.ASC).expire(idempotencyRetention).named("processedAt_ttl"),
        )
    }
}
