package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.OutboxStore
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant

/**
 * `kraken_outbox`. An append joins the caller's transaction (and, on PostgreSQL, notifies `kraken_outbox` in it: delivered at
 * the commit, dropped by a rollback). A claim runs in a transaction of its own: `SELECT ... FOR UPDATE SKIP LOCKED` in
 * `(created_at, seq)` order picks the entries free, already the owner's or whose lease passed, then `owner` and `lease_until`
 * mark them the owner's; a concurrent claim skips the rows this one locked and, once it committed, no longer matches them.
 */
internal class JdbcOutboxStore(
    private val db: Db,
    internal val notifications: PostgresNotifications?,
    /** Whether an append notifies the relays (PostgreSQL with `kraken.messaging.store.jdbc.notify` on). */
    private val notifies: Boolean,
) : OutboxStore {
    override suspend fun append(entry: OutboxEntry) {
        Limits.check("an outbox entry's id", entry.id, Limits.ID)
        Limits.check("an outbox entry's type", entry.type, Limits.NAME)
        Json.requireObject(entry.payload, "outbox entry ${entry.id} (${entry.type})")
        db.tx { jdbc ->
            jdbc.sql(
                "INSERT INTO kraken_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) " +
                    "VALUES (:id, :type, :key, ${db.dialect.json(":headers")}, ${db.dialect.json(":payload")}, :createdAt, :seq, :attempts)",
            )
                .param("id", entry.id).param("type", entry.type).param("key", entry.key)
                .param("headers", Json.write("an outbox entry's headers", entry.headers)).param("payload", String(entry.payload, Charsets.UTF_8))
                .param("createdAt", db.ts(entry.createdAt)).param("seq", entry.seq).param("attempts", entry.attempts)
                .update()
            if (notifies) jdbc.sql(NOTIFY).query { _, _ -> }.list()
        }
    }

    override suspend fun claim(owner: String, lease: Duration, limit: Int): List<OutboxEntry> = db.own { jdbc ->
        val now = db.now()
        val rows = jdbc.sql(CLAIM).param("owner", owner).param("now", db.ts(now)).param("limit", limit).query(::read).list()
        if (rows.isNotEmpty()) {
            jdbc.sql("UPDATE kraken_outbox SET owner = :owner, lease_until = :until WHERE id IN (:ids)")
                .param("owner", owner).param("until", db.ts(now.plus(lease))).param("ids", rows.map { it.id }).update()
        }
        rows
    }

    override suspend fun markSent(ids: Collection<String>) {
        if (ids.isEmpty()) return
        db.tx { it.sql("UPDATE kraken_outbox SET sent_at = :now WHERE id IN (:ids)").param("now", db.ts(db.now())).param("ids", ids.toList()).update() }
    }

    override suspend fun markAttemptFailed(id: String, error: String) {
        db.tx { it.sql("UPDATE kraken_outbox SET attempts = attempts + 1, last_error = :error WHERE id = :id").param("error", Limits.cut(error)).param("id", id).update() }
    }

    override suspend fun markFailed(id: String, error: String) {
        db.tx {
            it.sql("UPDATE kraken_outbox SET attempts = attempts + 1, last_error = :error, failed_at = :now WHERE id = :id")
                .param("error", Limits.cut(error)).param("now", db.ts(db.now())).param("id", id).update()
        }
    }

    override suspend fun pending(): Long =
        db.tx { it.sql("SELECT COUNT(*) FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL").query(Long::class.javaObjectType).single() }

    override suspend fun oldestPending(): Instant? = db.tx {
        it.sql("SELECT created_at FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL ORDER BY created_at, seq LIMIT 1")
            .query { rs, _ -> db.instant(rs, "created_at") }.optional().orElse(null)
    }

    override suspend fun purge(sentBefore: Instant): Long =
        db.purge("kraken_outbox", "id", "sent_at < :before", mapOf("before" to db.ts(sentBefore)))

    /** Registers [wake] with the PostgreSQL notifications; a no-op on MySQL (the relay polls). */
    override fun onAppend(wake: () -> Unit): AutoCloseable = notifications?.register(wake) ?: AutoCloseable {}

    private fun read(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = OutboxEntry(
        id = rs.getString("id"),
        type = rs.getString("type"),
        key = rs.getString("event_key"),
        headers = Json.strings(rs.getString("headers")),
        payload = rs.getString("payload").toByteArray(Charsets.UTF_8),
        createdAt = db.instant(rs, "created_at")!!,
        seq = rs.getLong("seq"),
        attempts = rs.getInt("attempts"),
    )

    companion object {
        /**
         * The relays' claim: the entries free, already the owner's or whose lease passed, in order, skipping those another claim
         * locked. At READ COMMITTED a locking read on MySQL still locks each row it scans until it has evaluated the row's filter
         * (then releases the ones that do not match): for that instant a concurrent claim can skip a row it could have taken, which
         * it then takes at its next claim. Sub-millisecond, and only between two relays claiming at the same moment.
         */
        const val CLAIM = "SELECT id, type, event_key, headers, payload, created_at, seq, attempts FROM kraken_outbox " +
            "WHERE sent_at IS NULL AND failed_at IS NULL AND (owner IS NULL OR owner = :owner OR lease_until <= :now) " +
            "ORDER BY created_at, seq, id LIMIT :limit FOR UPDATE SKIP LOCKED"

        /** The notification of an append, in its transaction (PostgreSQL). */
        const val NOTIFY = "SELECT pg_notify('${PostgresDialect.CHANNEL}', '')"
    }
}

/**
 * `kraken_processed_events`: one row per marker id, written in the caller's transaction. A stage-1 marker also keeps its
 * processor, source, segment and position (and the position as a number when it is a decimal one, for resets by range).
 */
internal class JdbcProcessedEventStore(private val db: Db) : ProcessedEventStore {
    override suspend fun record(marker: ProcessedMarker): Boolean {
        Limits.check("a marker's processor", marker.processor, Limits.PROCESSOR)
        Limits.check("a marker's source", marker.source, Limits.SOURCE)
        Limits.check("a marker's segment", marker.segment, Limits.SEGMENT)
        Limits.check("a marker's position", marker.position, Limits.POSITION)
        return db.tx { jdbc ->
            db.insertOnce(
                jdbc, "kraken_processed_events", "id_hash, id, type, processed_at, processor, source, segment, position, position_num",
                ":idHash, :id, :type, :processedAt, :processor, :source, :segment, :position, :positionNum", "id_hash",
                mapOf(
                    "idHash" to Limits.sha256(marker.id), "id" to marker.id, "type" to marker.type, "processedAt" to db.ts(marker.processedAt),
                    "processor" to marker.processor, "source" to marker.source, "segment" to marker.segment, "position" to marker.position,
                    "positionNum" to marker.position?.let(::positionNumber),
                ),
            )
        }
    }

    override suspend fun exists(id: String): Boolean = markerExists(db, id)

    override suspend fun purge(before: Instant): Long =
        db.purge("kraken_processed_events", "id_hash", "processed_at < :before", mapOf("before" to db.ts(before)))
}

/** Whether the marker [id] is recorded (by its hash). */
internal fun markerExists(db: Db, id: String): Boolean = db.tx {
    it.sql("SELECT COUNT(*) FROM kraken_processed_events WHERE id_hash = :idHash").param("idHash", Limits.sha256(id)).query(Long::class.javaObjectType).single() > 0
}
