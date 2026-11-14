package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.external.ExternalCallStatus
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.ExternalCallStore
import java.sql.ResultSet
import java.time.Instant

/**
 * `kraken_external_calls`: `reply` and `error` as the JSON text written. Every write joins the caller's transaction (the core
 * runs each in a short one of its own) and every write after [insert] is conditional on the record the caller read or owns.
 */
internal class JdbcExternalCallStore(private val db: Db) : ExternalCallStore {
    override suspend fun insert(call: ExternalCallRecord): Boolean = db.insertOnce(
        db.joined, "kraken_external_calls", "id, status, reply, error, attempts, updated_at, lease_until, owner",
        ":id, :status, :reply, :error, :attempts, :updatedAt, :leaseUntil, :owner", "id",
        mapOf(
            "id" to call.id, "status" to call.status.name, "reply" to call.reply?.let { String(it, Charsets.UTF_8) },
            "error" to call.error?.let { String(it, Charsets.UTF_8) }, "attempts" to call.attempts, "updatedAt" to db.ts(call.updatedAt),
            "leaseUntil" to db.ts(call.leaseUntil), "owner" to call.owner,
        ),
    )

    override suspend fun find(id: String): ExternalCallRecord? =
        db.joined.sql("SELECT * FROM kraken_external_calls WHERE id = :id").param("id", id).query(::read).optional().orElse(null)

    override suspend fun takeOver(
        id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, owner: String, leaseUntil: Instant, now: Instant,
    ): ExternalCallRecord? = db.atomic { jdbc ->
        val params = mutableMapOf<String, Any?>("leaseUntil" to db.ts(leaseUntil), "now" to db.ts(now), "owner" to owner)
        val updated = jdbc.sql(
            "UPDATE kraken_external_calls SET lease_until = :leaseUntil, updated_at = :now, owner = :owner, attempts = attempts + 1 " +
                "WHERE ${asRead(id, expectedOwner, expectedLeaseUntil, params)}",
        ).params(params).update() == 1
        // The row is this transaction's until it ends: the read sees the update.
        if (updated) jdbc.sql("SELECT * FROM kraken_external_calls WHERE id = :id").param("id", id).query(::read).single() else null
    }

    override suspend fun giveUp(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, now: Instant): Boolean {
        val params = mutableMapOf<String, Any?>("now" to db.ts(now))
        return db.joined.sql("UPDATE kraken_external_calls SET $SETTLE, status = 'UNKNOWN' WHERE ${asRead(id, expectedOwner, expectedLeaseUntil, params)}")
            .params(params).update() == 1
    }

    override suspend fun succeed(id: String, owner: String, reply: ByteArray, now: Instant): Boolean =
        db.joined.sql("UPDATE kraken_external_calls SET $SETTLE, status = 'SUCCEEDED', reply = :reply, error = NULL WHERE $OWNED")
            .param("reply", String(reply, Charsets.UTF_8)).param("now", db.ts(now)).param("id", id).param("expectedOwner", owner).update() == 1

    override suspend fun fail(id: String, owner: String, error: ByteArray, now: Instant): Boolean =
        db.joined.sql("UPDATE kraken_external_calls SET $SETTLE, status = 'FAILED', error = :error WHERE $OWNED")
            .param("error", String(error, Charsets.UTF_8)).param("now", db.ts(now)).param("id", id).param("expectedOwner", owner).update() == 1

    override suspend fun markUnknown(id: String, owner: String, now: Instant): Boolean =
        db.joined.sql("UPDATE kraken_external_calls SET $SETTLE, status = 'UNKNOWN' WHERE $OWNED")
            .param("now", db.ts(now)).param("id", id).param("expectedOwner", owner).update() == 1

    override suspend fun reset(id: String, owner: String): Boolean =
        db.joined.sql("DELETE FROM kraken_external_calls WHERE $OWNED").param("id", id).param("expectedOwner", owner).update() == 1

    /** `{id, status IN_PROGRESS, owner, lease_until}` as read (a null one as null), adding its parameters to [params]. */
    private fun asRead(id: String, owner: String?, leaseUntil: Instant?, params: MutableMap<String, Any?>): String {
        params["id"] = id
        params["expectedOwner"] = owner
        params["expectedLease"] = db.ts(leaseUntil)
        return "id = :id AND status = 'IN_PROGRESS' AND " +
            (if (owner == null) "owner IS NULL" else "owner = :expectedOwner") + " AND " +
            (if (leaseUntil == null) "lease_until IS NULL" else "lease_until = :expectedLease")
    }

    private fun read(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = ExternalCallRecord(
        id = rs.getString("id"),
        status = ExternalCallStatus.valueOf(rs.getString("status")),
        reply = rs.getString("reply")?.toByteArray(Charsets.UTF_8),
        error = rs.getString("error")?.toByteArray(Charsets.UTF_8),
        attempts = rs.getInt("attempts"),
        updatedAt = db.instant(rs, "updated_at")!!,
        leaseUntil = db.instant(rs, "lease_until"),
        owner = rs.getString("owner"),
    )

    private companion object {
        /** A settled record: no owner, no lease, updated now. */
        const val SETTLE = "updated_at = :now, lease_until = NULL, owner = NULL"

        /** The record [id] in progress, owned by the caller. */
        const val OWNED = "id = :id AND status = 'IN_PROGRESS' AND owner = :expectedOwner"
    }
}
