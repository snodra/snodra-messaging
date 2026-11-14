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
internal class JdbcOutboxStore(private val db: Db, internal val notifications: PostgresNotifications?) : OutboxStore {
    override suspend fun append(entry: OutboxEntry) {
        Json.requireObject(entry.payload, "outbox entry ${entry.id} (${entry.type})")
        val jdbc = db.joined
        jdbc.sql(
            "INSERT INTO kraken_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) " +
                "VALUES (:id, :type, :key, ${db.dialect.json(":headers")}, ${db.dialect.json(":payload")}, :createdAt, :seq, :attempts)",
        )
            .param("id", entry.id).param("type", entry.type).param("key", entry.key)
            .param("headers", Json.write(entry.headers)).param("payload", String(entry.payload, Charsets.UTF_8))
            .param("createdAt", db.ts(entry.createdAt)).param("seq", entry.seq).param("attempts", entry.attempts)
            .update()
        if (db.dialect.notifies) jdbc.sql("SELECT pg_notify('${PostgresDialect.CHANNEL}', '')").query { _, _ -> }.list()
    }

    override suspend fun claim(owner: String, lease: Duration, limit: Int): List<OutboxEntry> = db.own { jdbc ->
        val now = db.now()
        val rows = jdbc.sql(
            "SELECT id, type, event_key, headers, payload, created_at, seq, attempts FROM kraken_outbox " +
                "WHERE sent_at IS NULL AND failed_at IS NULL AND (owner IS NULL OR owner = :owner OR lease_until <= :now) " +
                "ORDER BY created_at, seq, id LIMIT :limit FOR UPDATE SKIP LOCKED",
        ).param("owner", owner).param("now", db.ts(now)).param("limit", limit).query(::read).list()
        if (rows.isNotEmpty()) {
            jdbc.sql("UPDATE kraken_outbox SET owner = :owner, lease_until = :until WHERE id IN (:ids)")
                .param("owner", owner).param("until", db.ts(now.plus(lease))).param("ids", rows.map { it.id }).update()
        }
        rows
    }

    override suspend fun markSent(ids: Collection<String>) {
        if (ids.isEmpty()) return
        db.joined.sql("UPDATE kraken_outbox SET sent_at = :now WHERE id IN (:ids)").param("now", db.ts(db.now())).param("ids", ids.toList()).update()
    }

    override suspend fun markAttemptFailed(id: String, error: String) {
        db.joined.sql("UPDATE kraken_outbox SET attempts = attempts + 1, last_error = :error WHERE id = :id").param("error", error).param("id", id).update()
    }

    override suspend fun markFailed(id: String, error: String) {
        db.joined.sql("UPDATE kraken_outbox SET attempts = attempts + 1, last_error = :error, failed_at = :now WHERE id = :id")
            .param("error", error).param("now", db.ts(db.now())).param("id", id).update()
    }

    override suspend fun pending(): Long =
        db.joined.sql("SELECT COUNT(*) FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL").query(Long::class.javaObjectType).single()

    override suspend fun oldestPending(): Instant? =
        db.joined.sql("SELECT created_at FROM kraken_outbox WHERE sent_at IS NULL AND failed_at IS NULL ORDER BY created_at, seq LIMIT 1")
            .query { rs, _ -> db.instant(rs, "created_at") }.optional().orElse(null)

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
}

/**
 * `kraken_processed_events`: one row per marker id, written in the caller's transaction. A stage-1 marker also keeps its
 * processor, source, segment and position (and the position as a number when it is a decimal one, for resets by range).
 */
internal class JdbcProcessedEventStore(private val db: Db) : ProcessedEventStore {
    override suspend fun record(marker: ProcessedMarker): Boolean = db.insertOnce(
        db.joined, "kraken_processed_events", "id, type, processed_at, processor, source, segment, position, position_num",
        ":id, :type, :processedAt, :processor, :source, :segment, :position, :positionNum", "id",
        mapOf(
            "id" to marker.id, "type" to marker.type, "processedAt" to db.ts(marker.processedAt), "processor" to marker.processor,
            "source" to marker.source, "segment" to marker.segment, "position" to marker.position,
            "positionNum" to marker.position?.let(::positionNumber),
        ),
    )

    override suspend fun exists(id: String): Boolean = markerExists(db, id)

    override suspend fun purge(before: Instant): Long =
        db.purge("kraken_processed_events", "id", "processed_at < :before", mapOf("before" to db.ts(before)))
}

internal fun markerExists(db: Db, id: String): Boolean =
    db.joined.sql("SELECT COUNT(*) FROM kraken_processed_events WHERE id = :id").param("id", id).query(Long::class.javaObjectType).single() > 0
