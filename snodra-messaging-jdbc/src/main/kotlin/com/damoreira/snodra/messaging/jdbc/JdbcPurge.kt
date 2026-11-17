package com.damoreira.snodra.messaging.jdbc

/**
 * The retention of the SQL message store (what the MongoDB add-on's TTL indexes do): deletes, in batches, the sent outbox
 * entries, the processed-event markers, the external calls, the ended sagas (and their associations), the dispatched and the
 * failed saga-dispatch entries, the fired or cancelled deadlines and the parked saga failures older than their retention, the
 * expired dead letters and the saga-dispatch leases that passed. Unsent outbox entries, live sagas and scheduled deadlines are
 * never deleted.
 */
internal object JdbcPurge {
    fun run(db: Db, s: JdbcStoreSettings): Long {
        val now = db.now()
        fun before(retention: java.time.Duration) = mapOf("before" to db.ts(now.minus(retention)))
        return db.purge("snodra_outbox", "id", "sent_at < :before", before(s.outboxRetention)) +
            db.purge("snodra_processed_events", "id_hash", "processed_at < :before", before(s.idempotencyRetention)) +
            db.purge("snodra_external_calls", "id_hash", "updated_at < :before", before(s.externalCallRetention)) +
            db.purge("snodra_sagas", "id", "ended = TRUE AND ended_at < :before", before(s.sagaEndedRetention)) +
            db.purge("snodra_saga_dispatch", "id", "dispatched_at < :before", before(s.sagaDispatchRetention)) +
            db.purge("snodra_saga_dispatch", "id", "failed_at < :before", before(s.failedRetention)) +
            db.purge("snodra_saga_dispatch_leases", "saga_id", "lease_until < :before", mapOf("before" to db.ts(now))) +
            db.purge("snodra_deadlines", "id", "finished_at < :before", before(s.deadlineRetention)) +
            db.purge("snodra_saga_failures", "id_hash", "last_failed_at < :before", before(s.failedRetention)) +
            db.purge("snodra_dead_letters", "id", "expire_at <= :before", mapOf("before" to db.ts(now)))
    }
}
