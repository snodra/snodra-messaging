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
import org.springframework.dao.CannotAcquireLockException
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.PessimisticLockingFailureException
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet
import java.time.Instant

/*
 * The saga ports ([SagaStores]) over `kraken_sagas` (+ `kraken_saga_associations`), `kraken_saga_dispatch`,
 * `kraken_saga_dispatch_leases`, `kraken_deadlines`, `kraken_saga_failures` and `kraken_processed_events`. What the runtime calls
 * joins its transaction; the relays' and the scheduler's claims (notices, leases, deadlines) commit on their own.
 *
 * Two transactions writing one saga never wait long for each other: the loser fails with a `TransientDataAccessException`
 * the saga runtime retries (a database would otherwise make it wait for the winner's commit). An update locks the saga's row
 * with `FOR UPDATE NOWAIT`. A start (an insert with a live start key) takes, on PostgreSQL, the transaction-scoped advisory
 * lock `pg_try_advisory_xact_lock(1263684430, hashtext(start_key_hash))` (another transaction's insert is invisible until it
 * commits); on MySQL the insert itself meets the other transaction's uncommitted row in the unique index on
 * `live_start_key_hash` and waits for it at most [MYSQL_START_WAIT_SECONDS] (`innodb_lock_wait_timeout`, for that statement):
 * no locking read on a missing row, so no gap lock, so no deadlock between starts of different keys. Messages name a saga's
 * type and a key's hash, never a start key or an association (personal data).
 */

internal fun jdbcSagaStores(db: Db) = SagaStores(
    JdbcSagaRepository(db), JdbcSagaDispatchStore(db), JdbcDeadlineStore(db), JdbcSagaFailureStore(db), JdbcSagaMarkerStore(db),
)

/** How long a saga start waits on MySQL for another transaction's uncommitted start with the same key (then retried). */
internal const val MYSQL_START_WAIT_SECONDS = 1

internal class JdbcSagaRepository(private val db: Db) : SagaRepository {
    override fun findById(id: String): SagaDocument? = db.tx { jdbc ->
        jdbc.sql("SELECT * FROM kraken_sagas WHERE id = :id").param("id", id).query(::readSaga).optional().orElse(null)
            ?.let { withAssociations(jdbc, listOf(it)).single() }
    }

    override fun findLive(type: String, key: String, value: String): List<SagaDocument> = db.tx { jdbc ->
        val sagas = jdbc.sql(
            "SELECT s.* FROM kraken_sagas s WHERE s.type = :type AND s.ended = FALSE AND EXISTS (SELECT 1 FROM kraken_saga_associations a " +
                "WHERE a.saga_id = s.id AND a.assoc_hash = :hash AND a.assoc_key = :key AND a.assoc_value = :value) ORDER BY s.created_at, s.id",
        ).param("type", type).param("hash", Limits.associationHash(key, value)).param("key", key).param("value", value).query(::readSaga).list()
        withAssociations(jdbc, sagas)
    }

    override fun insert(saga: SagaDocument) {
        check(saga)
        db.tx { jdbc ->
            val live = saga.startKey != null && !saga.ended
            if (live && db.dialect == PostgresDialect) holdStartKey(jdbc, saga)
            val insert: () -> Unit = {
                jdbc.sql(
                    "INSERT INTO kraken_sagas (id, type, state, start_key, start_key_hash, ended, ended_at, version, created_at, updated_at) " +
                        "VALUES (:id, :type, :state, :startKey, :startKeyHash, :ended, :endedAt, :version, :createdAt, :updatedAt)",
                ).params(sagaParams(saga)).update()
            }
            try {
                if (live && db.dialect == MySqlDialect) waitingAtMost(jdbc, MYSQL_START_WAIT_SECONDS, insert) else insert()
            } catch (e: DuplicateKeyException) {
                if (!live) throw e
                throw DuplicateKeyException("sagas: a live saga of type ${saga.type} holds the start key ${saga.startKey?.let(Limits::sha256)} (hash)", e)
            }
            insertAssociations(saga)
        }
    }

    override fun update(saga: SagaDocument, expectedVersion: Long): Boolean = db.tx { jdbc ->
        check(saga)
        val stored = jdbc.sql("SELECT version FROM kraken_sagas WHERE id = :id FOR UPDATE NOWAIT").param("id", saga.id)
            .query(Long::class.javaObjectType).optional().orElse(null)
        if (stored != expectedVersion) return@tx false
        jdbc.sql(
            "UPDATE kraken_sagas SET type = :type, state = :state, start_key = :startKey, start_key_hash = :startKeyHash, ended = :ended, ended_at = :endedAt, version = :version, " +
                "created_at = :createdAt, updated_at = :updatedAt WHERE id = :id AND version = :expected",
        ).params(sagaParams(saga)).param("expected", expectedVersion).update()
        if (associations(jdbc, listOf(saga.id))[saga.id].orEmpty() != saga.associations) {
            jdbc.sql("DELETE FROM kraken_saga_associations WHERE saga_id = :id").param("id", saga.id).update()
            insertAssociations(saga)
        }
        true
    }

    override fun countLiveByType(): Map<String, Long> = db.tx {
        it.sql("SELECT type, COUNT(*) AS n FROM kraken_sagas WHERE ended = FALSE GROUP BY type")
            .query { rs, _ -> rs.getString("type") to rs.getLong("n") }.list().toMap()
    }

    private fun check(saga: SagaDocument) {
        Limits.check("a saga's id", saga.id, Limits.ID)
        Limits.check("a saga's type", saga.type, Limits.NAME)
    }

    /** PostgreSQL: fails at once, retryably, while another live transaction starts a saga with the same start key. */
    private fun holdStartKey(jdbc: JdbcClient, saga: SagaDocument) {
        val hash = Limits.sha256(saga.startKey!!)
        val got = jdbc.sql("SELECT pg_try_advisory_xact_lock(:class, hashtext(:hash))")
            .param("class", PostgresDialect.SAGA_START_LOCK).param("hash", hash).query(Boolean::class.javaObjectType).single()
        if (!got) throw CannotAcquireLockException("sagas: another transaction is starting a saga of type ${saga.type} with the start key $hash (hash)")
    }

    /** MySQL: runs [statement] with `innodb_lock_wait_timeout` at [seconds], then puts the session's value back. */
    private fun waitingAtMost(jdbc: JdbcClient, seconds: Int, statement: () -> Unit) {
        val previous = jdbc.sql("SELECT @@SESSION.innodb_lock_wait_timeout").query(Long::class.javaObjectType).single()
        jdbc.sql("SET SESSION innodb_lock_wait_timeout = $seconds").update()
        try {
            statement()
        } finally {
            jdbc.sql("SET SESSION innodb_lock_wait_timeout = $previous").update()
        }
    }

    private fun insertAssociations(saga: SagaDocument) {
        if (saga.associations.isEmpty()) return
        db.batch.batchUpdate(
            "INSERT INTO kraken_saga_associations (saga_id, idx, assoc_key, assoc_value, assoc_hash) VALUES (?, ?, ?, ?, ?)",
            saga.associations.mapIndexed { i, a -> arrayOf<Any>(saga.id, i, a.k, a.v, Limits.associationHash(a.k, a.v)) },
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
        "id" to s.id, "type" to s.type, "state" to s.state, "startKey" to s.startKey, "startKeyHash" to s.startKey?.let(Limits::sha256), "ended" to s.ended, "endedAt" to db.ts(s.endedAt),
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
        for (e in entries) {
            Limits.check("a saga-dispatch entry's id", e.id, Limits.ID)
            Limits.check("a saga-dispatch entry's saga id", e.sagaId, Limits.ID)
            Limits.check("a saga-dispatch entry's type", e.type, Limits.NAME)
            // An event the outbox would refuse is refused here, inside the saga's transaction: never committed, then stuck.
            if (e.kind == DispatchKind.EVENT) Json.requireObject(e.payload.toByteArray(Charsets.UTF_8), "saga event ${e.id} (${e.type})")
        }
        db.tx {
            db.batch.batchUpdate(
                "INSERT INTO kraken_saga_dispatch (id, saga_id, seq, kind, type, payload, headers, routing_key, created_at, dispatched_at, attempts, " +
                    "next_attempt_at, failed_at, failure_reason, failure_message, notice_pending, notice_attempts, notice_next_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ${db.dialect.json("?")}, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                entries.map { e ->
                    arrayOf(
                        e.id, e.sagaId, e.seq, e.kind.name, e.type, e.payload, Json.write(e.headers), e.routingKey, db.ts(e.createdAt),
                        db.ts(e.dispatchedAt), e.attempts, db.ts(e.nextAttemptAt), db.ts(e.failedAt), e.failureReason?.name,
                        e.failureMessage?.let(Limits::cut), e.noticePending, e.noticeAttempts, db.ts(e.noticeNextAt),
                    )
                },
            )
        }
    }

    override fun dueSagas(now: Instant, exclude: Collection<String>, limit: Int): List<String> = db.tx { jdbc ->
        val excluded = if (exclude.isEmpty()) "" else "AND d.saga_id NOT IN (:exclude) "
        jdbc.sql(
            "SELECT d.saga_id FROM kraken_saga_dispatch d WHERE d.dispatched_at IS NULL AND d.failed_at IS NULL $excluded" +
                "AND d.seq = (SELECT MIN(x.seq) FROM kraken_saga_dispatch x WHERE x.saga_id = d.saga_id AND x.dispatched_at IS NULL AND x.failed_at IS NULL) " +
                "AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= :now) ORDER BY d.created_at, d.saga_id LIMIT :limit",
        ).param("now", db.ts(now)).param("limit", limit).also { if (exclude.isNotEmpty()) it.param("exclude", exclude.toList()) }
            .query { rs, _ -> rs.getString("saga_id") }.list()
    }

    override fun nextPending(sagaId: String): SagaDispatchDocument? = db.tx {
        it.sql("SELECT * FROM kraken_saga_dispatch WHERE saga_id = :sagaId AND dispatched_at IS NULL AND failed_at IS NULL ORDER BY seq LIMIT 1")
            .param("sagaId", sagaId).query(::readDispatch).optional().orElse(null)
    }

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
            mapOf(
                "attempts" to attempts, "now" to db.ts(now), "reason" to reason.name, "message" to Limits.cut(message),
                "noticeNextAt" to db.ts(noticeNextAt),
            ),
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

    /**
     * Takes [sagaId]'s lease for [owner] when there is none or it passed. PostgreSQL: one upsert, conditional on the lease having
     * passed. MySQL: the takeover first (an UPDATE conditional on the lease having passed), else an insert; a duplicate means
     * another claim got it. A lock conflict with another claim (a deadlock) is that claim winning: false.
     */
    override fun claimLease(sagaId: String, owner: String, until: Instant, now: Instant): Boolean {
        Limits.check("a saga id", sagaId, Limits.ID)
        val params = mapOf("sagaId" to sagaId, "owner" to owner, "until" to db.ts(until), "now" to db.ts(now))
        return try {
            db.own { jdbc ->
                when (db.dialect) {
                    PostgresDialect -> jdbc.sql(
                        "INSERT INTO kraken_saga_dispatch_leases (saga_id, owner, lease_until) VALUES (:sagaId, :owner, :until) " +
                            "ON CONFLICT (saga_id) DO UPDATE SET owner = EXCLUDED.owner, lease_until = EXCLUDED.lease_until " +
                            "WHERE kraken_saga_dispatch_leases.lease_until < :now",
                    ).params(params).update() == 1
                    MySqlDialect ->
                        jdbc.sql("UPDATE kraken_saga_dispatch_leases SET owner = :owner, lease_until = :until WHERE saga_id = :sagaId AND lease_until < :now")
                            .params(params).update() == 1 ||
                            db.insertOnce(jdbc, "kraken_saga_dispatch_leases", "saga_id, owner, lease_until", ":sagaId, :owner, :until", "saga_id", params)
                }
            }
        } catch (_: PessimisticLockingFailureException) {
            false
        }
    }

    override fun renewLease(sagaId: String, owner: String, until: Instant): Boolean = db.tx {
        it.sql("UPDATE kraken_saga_dispatch_leases SET lease_until = :until WHERE saga_id = :sagaId AND owner = :owner")
            .param("until", db.ts(until)).param("sagaId", sagaId).param("owner", owner).update() == 1
    }

    override fun releaseLease(sagaId: String, owner: String) {
        db.tx { it.sql("DELETE FROM kraken_saga_dispatch_leases WHERE saga_id = :sagaId AND owner = :owner").param("sagaId", sagaId).param("owner", owner).update() }
    }

    override fun pending(): Long = db.tx {
        it.sql("SELECT COUNT(*) FROM kraken_saga_dispatch WHERE dispatched_at IS NULL AND failed_at IS NULL").query(Long::class.javaObjectType).single()
    }

    private fun update(id: String, set: String, params: Map<String, Any?>) {
        db.tx { it.sql("UPDATE kraken_saga_dispatch SET $set WHERE id = :id").params(params).param("id", id).update() }
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
        Limits.check("a deadline's id", deadline.id, Limits.ID)
        Limits.check("a deadline's saga id", deadline.sagaId, Limits.ID)
        Limits.check("a deadline's saga type", deadline.sagaType, Limits.NAME)
        Limits.check("a deadline's name", deadline.name, Limits.NAME)
        db.tx {
            it.sql(
                "INSERT INTO kraken_deadlines (id, saga_type, saga_id, name, payload, payload_type, due_at, correlation_id, status, claimed_by, claimed_until, finished_at) " +
                    "VALUES (:id, :sagaType, :sagaId, :name, :payload, :payloadType, :dueAt, :correlationId, :status, :claimedBy, :claimedUntil, :finishedAt)",
            )
                .param("id", deadline.id).param("sagaType", deadline.sagaType).param("sagaId", deadline.sagaId).param("name", deadline.name)
                .param("payload", deadline.payload).param("payloadType", deadline.payloadType).param("dueAt", db.ts(deadline.dueAt))
                .param("correlationId", deadline.correlationId).param("status", deadline.status.name).param("claimedBy", deadline.claimedBy)
                .param("claimedUntil", db.ts(deadline.claimedUntil)).param("finishedAt", db.ts(deadline.finishedAt))
                .update()
        }
    }

    override fun findScheduled(id: String): DeadlineDocument? = db.tx {
        it.sql("SELECT * FROM kraken_deadlines WHERE id = :id AND status = 'SCHEDULED'").param("id", id).query(::readDeadline).optional().orElse(null)
    }

    override fun cancel(sagaId: String, deadlineId: String, now: Instant) =
        cancelWhere("id = :id AND saga_id = :sagaId", mapOf("id" to deadlineId, "sagaId" to sagaId), now)

    override fun cancelByName(sagaId: String, name: String, now: Instant) =
        cancelWhere("saga_id = :sagaId AND name = :name", mapOf("sagaId" to sagaId, "name" to name), now)

    override fun cancelAll(sagaId: String, now: Instant) = cancelWhere("saga_id = :sagaId", mapOf("sagaId" to sagaId), now)

    private fun cancelWhere(condition: String, params: Map<String, Any?>, now: Instant) {
        db.tx {
            it.sql("UPDATE kraken_deadlines SET status = 'CANCELLED', finished_at = :now WHERE $condition AND status = 'SCHEDULED'")
                .params(params).param("now", db.ts(now)).update()
        }
    }

    override fun markFired(id: String, now: Instant): Boolean = db.tx {
        it.sql("UPDATE kraken_deadlines SET status = 'FIRED', finished_at = :now WHERE id = :id AND status = 'SCHEDULED'")
            .param("now", db.ts(now)).param("id", id).update() == 1
    }

    override fun claimDue(owner: String, now: Instant, leaseUntil: Instant): DeadlineDocument? = db.own { jdbc ->
        val due = jdbc.sql(CLAIM).param("now", db.ts(now)).query(::readDeadline).optional().orElse(null) ?: return@own null
        jdbc.sql("UPDATE kraken_deadlines SET claimed_by = :owner, claimed_until = :until WHERE id = :id")
            .param("owner", owner).param("until", db.ts(leaseUntil)).param("id", due.id).update()
        due.copy(claimedBy = owner, claimedUntil = SqlDialect.millis(leaseUntil))
    }

    override fun countOverdueByName(now: Instant): Map<String, Long> = db.tx {
        it.sql("SELECT name, COUNT(*) AS n FROM kraken_deadlines WHERE status = 'SCHEDULED' AND due_at <= :now GROUP BY name")
            .param("now", db.ts(now)).query { rs, _ -> rs.getString("name") to rs.getLong("n") }.list().toMap()
    }

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

    companion object {
        /** The schedulers' claim: the scheduled deadline due first that nobody holds, skipping one another claim locked. */
        const val CLAIM = "SELECT * FROM kraken_deadlines WHERE status = 'SCHEDULED' AND due_at <= :now " +
            "AND (claimed_until IS NULL OR claimed_until < :now) ORDER BY due_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    }
}

/** `kraken_saga_failures`, keyed by the SHA-256 of `<sagaType>:<eventId>` (`id_hash`; the id itself in `id`). */
internal class JdbcSagaFailureStore(private val db: Db) : SagaFailureStore {
    override fun find(id: String): SagaFailureDocument? = db.tx {
        it.sql("SELECT * FROM kraken_saga_failures WHERE id_hash = :idHash").param("idHash", Limits.sha256(id)).query(::readFailure).optional().orElse(null)
    }

    /** Replaces the failure with that id (deleted, then inserted, in one transaction). */
    override fun save(failure: SagaFailureDocument) {
        Limits.check("a saga failure's saga type", failure.sagaType, Limits.NAME)
        db.tx { jdbc ->
            jdbc.sql("DELETE FROM kraken_saga_failures WHERE id_hash = :idHash").param("idHash", Limits.sha256(failure.id)).update()
            jdbc.sql(
                "INSERT INTO kraken_saga_failures (id_hash, id, saga_type, event_id, event_type, payload, headers, error, attempts, first_failed_at, last_failed_at) " +
                    "VALUES (:idHash, :id, :sagaType, :eventId, :eventType, :payload, ${db.dialect.json(":headers")}, :error, :attempts, :first, :last)",
            )
                .param("idHash", Limits.sha256(failure.id)).param("id", failure.id).param("sagaType", failure.sagaType)
                .param("eventId", failure.eventId).param("eventType", failure.eventType).param("payload", failure.payload)
                .param("headers", Json.write(failure.headers)).param("error", Limits.cut(failure.error)).param("attempts", failure.attempts)
                .param("first", db.ts(failure.firstFailedAt)).param("last", db.ts(failure.lastFailedAt))
                .update()
        }
    }

    override fun delete(id: String): Boolean = db.tx {
        it.sql("DELETE FROM kraken_saga_failures WHERE id_hash = :idHash").param("idHash", Limits.sha256(id)).update() == 1
    }

    override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean = db.tx {
        it.sql("UPDATE kraken_saga_failures SET attempts = attempts + :attempts, error = :error, last_failed_at = :now WHERE id_hash = :idHash")
            .param("attempts", attempts).param("error", Limits.cut(error)).param("now", db.ts(now)).param("idHash", Limits.sha256(id)).update() == 1
    }

    override fun list(sagaType: String?): List<SagaFailureDocument> = db.tx { jdbc ->
        jdbc.sql("SELECT * FROM kraken_saga_failures" + (if (sagaType != null) " WHERE saga_type = :sagaType" else "") + " ORDER BY last_failed_at, id_hash")
            .also { if (sagaType != null) it.param("sagaType", sagaType) }.query(::readFailure).list()
    }

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
        db.tx {
            it.sql("INSERT INTO kraken_processed_events (id_hash, id, type, processed_at) VALUES (:idHash, :id, :type, :now)")
                .param("idHash", Limits.sha256(id)).param("id", id).param("type", type).param("now", db.ts(now)).update()
        }
    }
}
