package io.damoreira.kraken.messaging.saga

import org.springframework.beans.factory.InitializingBean
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.PartialIndexFilter
import org.springframework.data.mongodb.core.query.Criteria
import java.time.Duration

/**
 * Ensures the indexes of `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines` and `saga_failures` exist
 * (the spec's storage table). Idempotent. The association and start-key indexes are partial on `ended: false`, so ended
 * instances neither route events nor hold a start key; the TTL indexes expire ended sagas ([endedRetention]), dispatched
 * entries ([dispatchRetention]) and fired or cancelled deadlines ([deadlineRetention]); a document whose date is still
 * null is never expired.
 */
class SagaIndexes(
    private val mongo: MongoTemplate,
    private val endedRetention: Duration,
    private val dispatchRetention: Duration,
    private val deadlineRetention: Duration,
) : InitializingBean {
    override fun afterPropertiesSet() {
        val live = PartialIndexFilter.of(Criteria.where("ended").`is`(false))
        mongo.indexOps("sagas").createIndex(
            Index().on("type", Sort.Direction.ASC).on("associations.k", Sort.Direction.ASC).on("associations.v", Sort.Direction.ASC)
                .partial(live).named("type_associations_live"),
        )
        mongo.indexOps("sagas").createIndex(
            Index().on("startKey", Sort.Direction.ASC).unique()
                .partial(PartialIndexFilter.of(Criteria.where("ended").`is`(false).and("startKey").exists(true))).named("startKey_live_unique"),
        )
        mongo.indexOps("sagas").createIndex(Index().on("endedAt", Sort.Direction.ASC).expire(endedRetention).named("endedAt_ttl"))

        mongo.indexOps("saga_dispatch").createIndex(
            Index().on("sagaId", Sort.Direction.ASC).on("seq", Sort.Direction.ASC).named("sagaId_1_seq_1"),
        )
        // What the relay's due-saga aggregation matches (dispatchedAt and failedAt null) and sorts by (sagaId, seq).
        mongo.indexOps("saga_dispatch").createIndex(
            Index().on("dispatchedAt", Sort.Direction.ASC).on("failedAt", Sort.Direction.ASC).on("sagaId", Sort.Direction.ASC).on("seq", Sort.Direction.ASC)
                .named("dispatchedAt_1_failedAt_1_sagaId_1_seq_1"),
        )
        mongo.indexOps("saga_dispatch").createIndex(Index().on("dispatchedAt", Sort.Direction.ASC).expire(dispatchRetention).named("dispatchedAt_ttl"))
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
        mongo.indexOps("deadlines").createIndex(Index().on("finishedAt", Sort.Direction.ASC).expire(deadlineRetention).named("finishedAt_ttl"))

        mongo.indexOps("saga_failures").createIndex(
            Index().on("sagaType", Sort.Direction.ASC).on("lastFailedAt", Sort.Direction.ASC).named("sagaType_1_lastFailedAt_1"),
        )
    }
}
