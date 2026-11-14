package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DeadlineStatus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.DeadlineStore
import io.damoreira.kraken.messaging.store.SagaDispatchStore
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaMarkerStore
import io.damoreira.kraken.messaging.store.SagaRepository
import io.damoreira.kraken.messaging.store.SagaStores
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet
import java.time.Instant

/*
 * The saga ports ([SagaStores]) over `kraken_sagas` (+ `kraken_saga_associations`), `kraken_saga_dispatch`,
 * `kraken_saga_dispatch_leases`, `kraken_deadlines`, `kraken_saga_failures` and `kraken_processed_events`. What the runtime calls
 * joins its transaction; the relays' and the scheduler's claims (notices, leases, deadlines) commit on their own.
 *
 * Two transactions writing one saga never wait for each other: the loser fails at once with a `TransientDataAccessException`
 * the saga runtime retries (a database would otherwise make it wait for the winner's commit). An update locks the saga's row
 * with `FOR UPDATE NOWAIT`; a start (an insert with a live start key) takes, on PostgreSQL, the transaction-scoped advisory
 * lock `pg_try_advisory_xact_lock(1263684430, hashtext(start_key))` (another transaction's insert is invisible until it
 * commits), and on MySQL locks the live row with that key `FOR UPDATE NOWAIT` (an uncommitted one included).
 */

internal fun jdbcSagaStores(db: Db) = SagaStores(
    JdbcSagaRepository(db), JdbcSagaDispatchStore(db), JdbcDeadlineStore(db), JdbcSagaFailureStore(db), JdbcSagaMarkerStore(db),
)

internal class JdbcSagaRepository(private val db: Db) : SagaRepository {
    override fun findById(id: String): SagaDocument? {
        val jdbc = db.joined
        val saga = jdbc.sql("SELECT * FROM kraken_sagas WHERE id = :id").param("id", id).query(::readSaga).optional().orElse(null) ?: return null
        return withAssociations(jdbc, listOf(saga)).single()
    }

    override fun findLive(type: String, key: String, value: String): List<SagaDocument> {
        val jdbc = db.joined
        val sagas = jdbc.sql(
            "SELECT s.* FROM kraken_sagas s WHERE s.type = :type AND s.ended = FALSE AND EXISTS (SELECT 1 FROM kraken_saga_associations a " +
                "WHERE a.saga_id = s.id AND a.assoc_key = :key AND a.assoc_value = :value) ORDER BY s.created_at, s.id",
        ).param("type", type).param("key", key).param("value", value).query(::readSaga).list()
        return withAssociations(jdbc, sagas)
    }

    override fun insert(saga: SagaDocument) {
        db.atomic { jdbc ->
            if (saga.startKey != null && !saga.ended) holdStartKey(jdbc, saga.startKey!!)
            jdbc.sql(
                "INSERT INTO kraken_sagas (id, type, state, start_key, ended, ended_at, version, created_at, updated_at) " +
                    "VALUES (:id, :type, :state, :startKey, :ended, :endedAt, :version, :createdAt, :updatedAt)",
            ).params(sagaParams(saga)).update()
            insertAssociations(saga)
        }
    }

    override fun update(saga: SagaDocument, expectedVersion: Long): Boolean = db.atomic { jdbc ->
        val stored = jdbc.sql("SELECT version FROM kraken_sagas WHERE id = :id FOR UPDATE NOWAIT").param("id", saga.id)
            .query(Long::class.javaObjectType).optional().orElse(null)
        if (stored != expectedVersion) return@atomic false
        jdbc.sql(
            "UPDATE kraken_sagas SET type = :type, state = :state, start_key = :startKey, ended = :ended, ended_at = :endedAt, version = :version, " +
                "created_at = :createdAt, updated_at = :updatedAt WHERE id = :id AND version = :expected",
        ).params(sagaParams(saga)).param("expected", expectedVersion).update()
        if (associations(jdbc, listOf(saga.id))[saga.id].orEmpty() != saga.associations) {
            jdbc.sql("DELETE FROM kraken_saga_associations WHERE saga_id = :id").param("id", saga.id).update()
            insertAssociations(saga)
        }
        true
    }

    override fun countLiveByType(): Map<String, Long> =
        db.joined.sql("SELECT type, COUNT(*) AS n FROM kraken_sagas WHERE ended = FALSE GROUP BY type")
            .query { rs, _ -> rs.getString("type") to rs.getLong("n") }.list().toMap()

    /** Fails at once, retryably, while another live transaction starts a saga with [startKey]; a committed holder is a duplicate. */
    private fun holdStartKey(jdbc: JdbcClient, startKey: String) {
        when (db.dialect) {
            PostgresDialect -> {
                val got = jdbc.sql("SELECT pg_try_advisory_xact_lock(:class, hashtext(:key))")
                    .param("class", PostgresDialect.SAGA_START_LOCK).param("key", startKey).query(Boolean::class.javaObjectType).single()
                if (!got) throw org.springframework.dao.CannotAcquireLockException("sagas: another transaction is starting a saga with the start key $startKey")
            }
            MySqlDialect -> {
                val holder = jdbc.sql("SELECT id FROM kraken_sagas WHERE live_start_key = :key FOR UPDATE NOWAIT").param("key", startKey)
                    .query(String::class.java).optional().orElse(null)
                if (holder != null) throw DuplicateKeyException("sagas: the start key $startKey is held by the live saga $holder")
            }
        }
    }

    private fun insertAssociations(saga: SagaDocument) {
        if (saga.associations.isEmpty()) return
        db.joinedBatch.batchUpdate(
            "INSERT INTO kraken_saga_associations (saga_id, idx, assoc_key, assoc_value) VALUES (?, ?, ?, ?)",
            saga.associations.mapIndexed { i, a -> arrayOf<Any>(saga.id, i, a.k, a.v) },
        )
    }

    private fun associations(jdbc: JdbcClient, ids: List<String>): Map<String, List<Association>> {
        if (ids.isEmpty()) return emptyMap()
        return jdbc.sql("SELECT saga_id, assoc_key, assoc_value FROM kraken_saga_associations WHERE saga_id IN (:ids) ORDER BY saga_id, idx")
            .param("ids", ids).query { rs, _ -> rs.getString("saga_id") to Association(rs.getString("assoc_key"), rs.getString("assoc_value")) }
            .list().groupBy({ it.first }, { it.second })
    }

    private fun withAssociations(jdbc: JdbcClient, sagas: List<SagaDocument>): List<SagaDocument> {
        val byId = associations(jdbc, sagas.map { it.id })
        return sagas.map { it.copy(associations = byId[it.id].orEmpty()) }
    }

    private fun sagaParams(s: SagaDocument) = mapOf(
        "id" to s.id, "type" to s.type, "state" to s.state, "startKey" to s.startKey, "ended" to s.ended, "endedAt" to db.ts(s.endedAt),
        "version" to s.version, "createdAt" to db.ts(s.createdAt), "updatedAt" to db.ts(s.updatedAt),
    )

    private fun readSaga(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = SagaDocument(
        id = rs.getString("id"),
        type = rs.getString("type"),
        state = rs.getString("state"),
        startKey = rs.getString("start_key"),
        ended = rs.getBoolean("ended"),
        endedAt = db.instant(rs, "ended_at"),
        version = rs.getLong("version"),
        createdAt = db.instant(rs, "created_at")!!,
        updatedAt = db.instant(rs, "updated_at")!!,
    )
}

internal class JdbcSagaDispatchStore(private val db: Db) : SagaDispatchStore {
    override fun record(entries: List<SagaDispatchDocument>) {
        if (entries.isEmpty()) return
        // An event the outbox would refuse is refused here, inside the saga's transaction: never committed, then stuck.
        for (e in entries) if (e.kind == DispatchKind.EVENT) Json.requireObject(e.payload.toByteArray(Charsets.UTF_8), "saga event ${e.id} (${e.type})")
        db.atomic {
            db.joinedBatch.batchUpdate(
                "INSERT INTO kraken_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, routing_key, created_at, dispatched_at, attempts, " +
                    "next_attempt_at, failed_at, failure_reason, failure_message, notice_pending, notice_attempts, notice_next_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ${db.dialect.json("?")}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                entries.map { e ->
                    arrayOf(
                        e.id, e.sagaId, e.seq, e.kind.name, e.type, e.payload, Json.write(e.headers), e.routingKey, db.ts(e.createdAt),
                        db.ts(e.dispatchedAt), e.attempts, db.ts(e.nextAttemptAt), db.ts(e.failedAt), e.failureReason?.name, e.failureMessage,
                        e.noticePending, e.noticeAttempts, db.ts(e.noticeNextAt),
                    )
                },
            )
        }
    }

    override fun dueSagas(now: Instant, exclude: Collection<String>, limit: Int): List<String> {
        val excluded = if (exclude.isEmpty()) "" else "AND d.saga_id NOT IN (:exclude) "
        return db.joined.sql(
            "SELECT d.saga_id FROM kraken_saga_dispatch d WHERE d.dispatched_at IS NULL AND d.failed_at IS NULL $excluded" +
                "AND d.seq = (SELECT MIN(x.seq) FROM kraken_saga_dispatch x WHERE x.saga_id = d.saga_id AND x.dispatched_at IS NULL AND x.failed_at IS NULL) " +
                "AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= :now) ORDER BY d.created_at, d.saga_id LIMIT :limit",
        ).param("now", db.ts(now)).param("limit", limit).also { if (exclude.isNotEmpty()) it.param("exclude", exclude.toList()) }
            .query { rs, _ -> rs.getString("saga_id") }.list()
    }

    override fun nextPending(sagaId: String): SagaDispatchDocument? =
        db.joined.sql("SELECT * FROM kraken_saga_dispatch WHERE saga_id = :sagaId AND dispatched_at IS NULL AND failed_at IS NULL ORDER BY seq LIMIT 1")
            .param("sagaId", sagaId).query(::readDispatch).optional().orElse(null)

    override fun markDispatched(id: String, now: Instant) {
        update(id, "dispatched_at = :now", mapOf("now" to db.ts(now)))
    }

    override fun scheduleRetry(id: String, attempts: Int, nextAttemptAt: Instant) {
        update(id, "attempts = :attempts, next_attempt_at = :next", mapOf("attempts" to attempts, "next" to db.ts(nextAttemptAt)))
    }

    override fun markFailed(id: String, attempts: Int, reason: SagaCommandFailed.Reason, message: String, now: Instant, noticeNextAt: Instant) {
        update(
            id,
            "attempts = :attempts, failed_at = :now, failure_reason = :reason, failure_message = :message, notice_pending = TRUE, " +
                "notice_attempts = 0, notice_next_at = :noticeNextAt",
            mapOf("attempts" to attempts, "now" to db.ts(now), "reason" to reason.name, "message" to message, "noticeNextAt" to db.ts(noticeNextAt)),
        )
    }

    override fun claimDueNotices(now: Instant, reserveUntil: Instant, limit: Int): List<SagaDispatchDocument> = db.own { jdbc ->
        val due = jdbc.sql(
            "SELECT * FROM kraken_saga_dispatch WHERE notice_pending = TRUE AND (notice_next_at IS NULL OR notice_next_at <= :now) " +
                "ORDER BY CASE WHEN notice_next_at IS NULL THEN 0 ELSE 1 END, notice_next_at, id LIMIT :limit FOR UPDATE SKIP LOCKED",
        ).param("now", db.ts(now)).param("limit", limit).query(::readDispatch).list()
        if (due.isNotEmpty()) {
            jdbc.sql("UPDATE kraken_saga_dispatch SET notice_next_at = :reserve WHERE id IN (:ids)")
                .param("reserve", db.ts(reserveUntil)).param("ids", due.map { it.id }).update()
        }
        due.map { it.copy(noticeNextAt = SqlDialect.millis(reserveUntil)) }
    }

    override fun scheduleNoticeRetry(id: String, noticeAttempts: Int, noticeNextAt: Instant) {
        update(id, "notice_attempts = :attempts, notice_next_at = :next", mapOf("attempts" to noticeAttempts, "next" to db.ts(noticeNextAt)))
    }

    override fun clearNotice(id: String) {
        update(id, "notice_pending = NULL, notice_attempts = NULL, notice_next_at = NULL", emptyMap())
    }

    override fun claimLease(sagaId: String, owner: String, until: Instant, now: Instant): Boolean = db.own { jdbc ->
        val params = mapOf("sagaId" to sagaId, "owner" to owner, "until" to db.ts(until), "now" to db.ts(now))
        when (db.dialect) {
            // One statement: inserted, or taken over when the lease passed; 0 rows while it holds.
            PostgresDialect -> jdbc.sql(
                "INSERT INTO kraken_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (:sagaId, :owner, :until) " +
                    "ON CONFLICT (saga_id) DO UPDATE SET owner = EXCLUDED.owner, lease_until = EXCLUDED.lease_until " +
                    "WHERE kraken_saga_dispatch_leases.lease_until < :now",
            ).params(params).update() == 1
            // Inserted, else taken over when the lease passed. No locking read first: on a missing row two of those would take
            // gap locks and deadlock on their inserts; a concurrent insert of the same key waits for the other's commit instead.
            MySqlDialect ->
                db.insertOnce(jdbc, "kraken_saga_dispatch_leases", "saga_id, owner, lease_until", ":sagaId, :owner, :until", "saga_id", params) ||
                    jdbc.sql("UPDATE kraken_saga_dispatch_leases SET owner = :owner, lease_until = :until WHERE saga_id = :sagaId AND lease_until < :now")
                        .params(params).update() == 1
        }
    }

    override fun renewLease(sagaId: String, owner: String, until: Instant): Boolean =
        db.joined.sql("UPDATE kraken_saga_dispatch_leases SET lease_until = :until WHERE saga_id = :sagaId AND owner = :owner")
            .param("until", db.ts(until)).param("sagaId", sagaId).param("owner", owner).update() == 1

    override fun releaseLease(sagaId: String, owner: String) {
        db.joined.sql("DELETE FROM kraken_saga_dispatch_leases WHERE saga_id = :sagaId AND owner = :owner").param("sagaId", sagaId).param("owner", owner).update()
    }

    override fun pending(): Long =
        db.joined.sql("SELECT COUNT(*) FROM kraken_saga_dispatch WHERE dispatched_at IS NULL AND failed_at IS NULL").query(Long::class.javaObjectType).single()

    private fun update(id: String, set: String, params: Map<String, Any?>) {
        db.joined.sql("UPDATE kraken_saga_dispatch SET $set WHERE id = :id").params(params).param("id", id).update()
    }

    private fun readDispatch(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = SagaDispatchDocument(
        id = rs.getString("id"),
        sagaId = rs.getString("saga_id"),
        seq = rs.getLong("seq"),
        kind = DispatchKind.valueOf(rs.getString("kind")),
        type = rs.getString("type"),
        payload = rs.getString("payload"),
        headers = Json.strings(rs.getString("headers")),
        routingKey = rs.getString("routing_key"),
        createdAt = db.instant(rs, "created_at")!!,
        dispatchedAt = db.instant(rs, "dispatched_at"),
        attempts = rs.getInt("attempts"),
        nextAttemptAt = db.instant(rs, "next_attempt_at"),
        failedAt = db.instant(rs, "failed_at"),
        failureReason = rs.getString("failure_reason")?.let(SagaCommandFailed.Reason::valueOf),
        failureMessage = rs.getString("failure_message"),
        noticePending = rs.getBoolean("notice_pending").takeUnless { rs.wasNull() },
        noticeAttempts = rs.getInt("notice_attempts").takeUnless { rs.wasNull() },
        noticeNextAt = db.instant(rs, "notice_next_at"),
    )
}

internal class JdbcDeadlineStore(private val db: Db) : DeadlineStore {
    override fun insert(deadline: DeadlineDocument) {
        db.joined.sql(
            "INSERT INTO kraken_deadlines (id, saga_type, saga_id, name, payload, payload_type, due_at, correlation_id, status, claimed_by, claimed_until, finished_at) " +
                "VALUES (:id, :sagaType, :sagaId, :name, :payload, :payloadType, :dueAt, :correlationId, :status, :claimedBy, :claimedUntil, :finishedAt)",
        )
            .param("id", deadline.id).param("sagaType", deadline.sagaType).param("sagaId", deadline.sagaId).param("name", deadline.name)
            .param("payload", deadline.payload).param("payloadType", deadline.payloadType).param("dueAt", db.ts(deadline.dueAt))
            .param("correlationId", deadline.correlationId).param("status", deadline.status.name).param("claimedBy", deadline.claimedBy)
            .param("claimedUntil", db.ts(deadline.claimedUntil)).param("finishedAt", db.ts(deadline.finishedAt))
            .update()
    }

    override fun findScheduled(id: String): DeadlineDocument? =
        db.joined.sql("SELECT * FROM kraken_deadlines WHERE id = :id AND status = 'SCHEDULED'").param("id", id).query(::readDeadline).optional().orElse(null)

    override fun cancel(sagaId: String, deadlineId: String, now: Instant) =
        cancelWhere("id = :id AND saga_id = :sagaId", mapOf("id" to deadlineId, "sagaId" to sagaId), now)

    override fun cancelByName(sagaId: String, name: String, now: Instant) =
        cancelWhere("saga_id = :sagaId AND name = :name", mapOf("sagaId" to sagaId, "name" to name), now)

    override fun cancelAll(sagaId: String, now: Instant) = cancelWhere("saga_id = :sagaId", mapOf("sagaId" to sagaId), now)

    private fun cancelWhere(condition: String, params: Map<String, Any?>, now: Instant) {
        db.joined.sql("UPDATE kraken_deadlines SET status = 'CANCELLED', finished_at = :now WHERE $condition AND status = 'SCHEDULED'")
            .params(params).param("now", db.ts(now)).update()
    }

    override fun markFired(id: String, now: Instant): Boolean =
        db.joined.sql("UPDATE kraken_deadlines SET status = 'FIRED', finished_at = :now WHERE id = :id AND status = 'SCHEDULED'")
            .param("now", db.ts(now)).param("id", id).update() == 1

    override fun claimDue(owner: String, now: Instant, leaseUntil: Instant): DeadlineDocument? = db.own { jdbc ->
        val due = jdbc.sql(
            "SELECT * FROM kraken_deadlines WHERE status = 'SCHEDULED' AND due_at <= :now AND (claimed_until IS NULL OR claimed_until < :now) " +
                "ORDER BY due_at, id LIMIT 1 FOR UPDATE SKIP LOCKED",
        ).param("now", db.ts(now)).query(::readDeadline).optional().orElse(null) ?: return@own null
        jdbc.sql("UPDATE kraken_deadlines SET claimed_by = :owner, claimed_until = :until WHERE id = :id")
            .param("owner", owner).param("until", db.ts(leaseUntil)).param("id", due.id).update()
        due.copy(claimedBy = owner, claimedUntil = SqlDialect.millis(leaseUntil))
    }

    override fun countOverdueByName(now: Instant): Map<String, Long> =
        db.joined.sql("SELECT name, COUNT(*) AS n FROM kraken_deadlines WHERE status = 'SCHEDULED' AND due_at <= :now GROUP BY name")
            .param("now", db.ts(now)).query { rs, _ -> rs.getString("name") to rs.getLong("n") }.list().toMap()

    private fun readDeadline(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = DeadlineDocument(
        id = rs.getString("id"),
        sagaType = rs.getString("saga_type"),
        sagaId = rs.getString("saga_id"),
        name = rs.getString("name"),
        payload = rs.getString("payload"),
        payloadType = rs.getString("payload_type"),
        dueAt = db.instant(rs, "due_at")!!,
        correlationId = rs.getString("correlation_id"),
        status = DeadlineStatus.valueOf(rs.getString("status")),
        claimedBy = rs.getString("claimed_by"),
        claimedUntil = db.instant(rs, "claimed_until"),
        finishedAt = db.instant(rs, "finished_at"),
    )
}

internal class JdbcSagaFailureStore(private val db: Db) : SagaFailureStore {
    override fun find(id: String): SagaFailureDocument? =
        db.joined.sql("SELECT * FROM kraken_saga_failures WHERE id = :id").param("id", id).query(::readFailure).optional().orElse(null)

    /** Replaces the failure with that id (deleted, then inserted, in one transaction). */
    override fun save(failure: SagaFailureDocument) {
        db.atomic { jdbc ->
            jdbc.sql("DELETE FROM kraken_saga_failures WHERE id = :id").param("id", failure.id).update()
            jdbc.sql(
                "INSERT INTO kraken_saga_failures (id, saga_type, event_id, event_type, payload, headers, error, attempts, first_failed_at, last_failed_at) " +
                    "VALUES (:id, :sagaType, :eventId, :eventType, :payload, ${db.dialect.json(":headers")}, :error, :attempts, :first, :last)",
            )
                .param("id", failure.id).param("sagaType", failure.sagaType).param("eventId", failure.eventId).param("eventType", failure.eventType)
                .param("payload", failure.payload).param("headers", Json.write(failure.headers)).param("error", failure.error)
                .param("attempts", failure.attempts).param("first", db.ts(failure.firstFailedAt)).param("last", db.ts(failure.lastFailedAt))
                .update()
        }
    }

    override fun delete(id: String): Boolean = db.joined.sql("DELETE FROM kraken_saga_failures WHERE id = :id").param("id", id).update() == 1

    override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean =
        db.joined.sql("UPDATE kraken_saga_failures SET attempts = attempts + :attempts, error = :error, last_failed_at = :now WHERE id = :id")
            .param("attempts", attempts).param("error", error).param("now", db.ts(now)).param("id", id).update() == 1

    override fun list(sagaType: String?): List<SagaFailureDocument> =
        db.joined.sql("SELECT * FROM kraken_saga_failures" + (if (sagaType != null) " WHERE saga_type = :sagaType" else "") + " ORDER BY last_failed_at, id")
            .also { if (sagaType != null) it.param("sagaType", sagaType) }.query(::readFailure).list()

    private fun readFailure(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = SagaFailureDocument(
        id = rs.getString("id"),
        sagaType = rs.getString("saga_type"),
        eventId = rs.getString("event_id"),
        eventType = rs.getString("event_type"),
        payload = rs.getString("payload"),
        headers = Json.strings(rs.getString("headers")),
        error = rs.getString("error"),
        attempts = rs.getInt("attempts"),
        firstFailedAt = db.instant(rs, "first_failed_at")!!,
        lastFailedAt = db.instant(rs, "last_failed_at")!!,
    )
}

/** The saga stage's markers, in `kraken_processed_events`. */
internal class JdbcSagaMarkerStore(private val db: Db) : SagaMarkerStore {
    override fun exists(id: String): Boolean = markerExists(db, id)

    override fun insert(id: String, type: String, now: Instant) {
        db.joined.sql("INSERT INTO kraken_processed_events (id, type, processed_at) VALUES (:id, :type, :now)")
            .param("id", id).param("type", type).param("now", db.ts(now)).update()
    }
}
