package com.damoreira.snodra.messaging.mongo

import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.PartialIndexFilter
import org.springframework.data.mongodb.core.query.Criteria
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * The indexes of every collection the store uses (idempotent, the names the library has always given them): the outbox relay
 * index, the TTL indexes of `outbox`, `processed_events`, `external_calls` and the saga collections, the three of
 * `dead_letters`, and the partial association and start-key indexes of `sagas` (on `ended: false`, so ended instances neither
 * route events nor hold a start key). A document whose TTL date is still null is never expired.
 */
internal object MongoIndexes {
    fun ensure(mongo: MongoTemplate, settings: MongoStoreSettings) {
        mongo.indexOps("outbox").createIndex(
            Index().on("sentAt", Sort.Direction.ASC).on("createdAt", Sort.Direction.ASC)
                .on("seq", Sort.Direction.ASC).named("sentAt_1_createdAt_1_seq_1"),
        )
        // Documents with sentAt == null are never expired by the TTL monitor.
        mongo.indexOps("outbox").createIndex(
            Index().on("sentAt", Sort.Direction.ASC).expire(settings.outboxRetention).named("sentAt_ttl"),
        )
        mongo.indexOps("processed_events").createIndex(
            Index().on("processedAt", Sort.Direction.ASC).expire(settings.idempotencyRetention).named("processedAt_ttl"),
        )
        mongo.indexOps("external_calls").createIndex(
            Index().on("updatedAt", Sort.Direction.ASC).expire(settings.externalCallRetention).named("updatedAt_ttl"),
        )

        val deadLetters = mongo.independent(MongoDeadLetterStore.COLLECTION)
        deadLetters.createIndex(Indexes.ascending("processor", "key", "offset"), IndexOptions().name("dead_letters_processor_key_offset"))
        deadLetters.createIndex(Indexes.ascending("processor", "parkedAt"), IndexOptions().name("dead_letters_processor_parked"))
        deadLetters.createIndex(Indexes.ascending("expireAt"), IndexOptions().name("dead_letters_expire").expireAfter(0L, TimeUnit.SECONDS))

        val live = PartialIndexFilter.of(Criteria.where("ended").`is`(false))
        mongo.indexOps("sagas").createIndex(
            Index().on("type", Sort.Direction.ASC).on("associations.k", Sort.Direction.ASC).on("associations.v", Sort.Direction.ASC)
                .partial(live).named("type_associations_live"),
        )
        mongo.indexOps("sagas").createIndex(
            Index().on("startKey", Sort.Direction.ASC).unique()
                .partial(PartialIndexFilter.of(Criteria.where("ended").`is`(false).and("startKey").exists(true))).named("startKey_live_unique"),
        )
        mongo.indexOps("sagas").createIndex(Index().on("endedAt", Sort.Direction.ASC).expire(settings.sagaEndedRetention).named("endedAt_ttl"))

        mongo.indexOps("saga_dispatch").createIndex(
            Index().on("sagaId", Sort.Direction.ASC).on("seq", Sort.Direction.ASC).named("sagaId_1_seq_1"),
        )
        // What the relay's due-saga aggregation matches (dispatchedAt and failedAt null) and sorts by (sagaId, seq).
        mongo.indexOps("saga_dispatch").createIndex(
            Index().on("dispatchedAt", Sort.Direction.ASC).on("failedAt", Sort.Direction.ASC).on("sagaId", Sort.Direction.ASC).on("seq", Sort.Direction.ASC)
                .named("dispatchedAt_1_failedAt_1_sagaId_1_seq_1"),
        )
        mongo.indexOps("saga_dispatch").createIndex(Index().on("dispatchedAt", Sort.Direction.ASC).expire(settings.sagaDispatchRetention).named("dispatchedAt_ttl"))
        mongo.indexOps("saga_dispatch").createIndex(Index().on("failedAt", Sort.Direction.ASC).expire(settings.failedRetention).named("failedAt_ttl"))
        mongo.indexOps("saga_dispatch").createIndex(
            Index().on("noticeNextAt", Sort.Direction.ASC)
                .partial(PartialIndexFilter.of(Criteria.where("noticePending").`is`(true))).named("noticePending_noticeNextAt"),
        )
        mongo.indexOps("saga_dispatch_leases").createIndex(Index().on("until", Sort.Direction.ASC).expire(Duration.ZERO).named("until_ttl"))

        mongo.indexOps("deadlines").createIndex(
            Index().on("status", Sort.Direction.ASC).on("dueAt", Sort.Direction.ASC).named("status_1_dueAt_1"),
        )
        mongo.indexOps("deadlines").createIndex(
            Index().on("sagaId", Sort.Direction.ASC).on("status", Sort.Direction.ASC).named("sagaId_1_status_1"),
        )
        mongo.indexOps("deadlines").createIndex(Index().on("finishedAt", Sort.Direction.ASC).expire(settings.deadlineRetention).named("finishedAt_ttl"))

        mongo.indexOps("saga_failures").createIndex(
            Index().on("sagaType", Sort.Direction.ASC).on("lastFailedAt", Sort.Direction.ASC).named("sagaType_1_lastFailedAt_1"),
        )
        mongo.indexOps("saga_failures").createIndex(Index().on("lastFailedAt", Sort.Direction.ASC).expire(settings.failedRetention).named("lastFailedAt_ttl"))
    }
}
