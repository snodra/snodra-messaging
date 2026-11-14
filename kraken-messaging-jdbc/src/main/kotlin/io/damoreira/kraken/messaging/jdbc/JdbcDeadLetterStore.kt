package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.store.DeadLetterStore
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet

/**
 * `kraken_dead_letters` (ADR 0022). Every write but [deleteInTransaction] commits on its own. A letter whose `expire_at`
 * passed is gone at once for every read and write here (it no longer blocks its key), and the purge deletes it.
 * Positions are ordered as [POSITION_ORDER] says: decimal ones by value, then the others by their bytes; a bound compares only
 * positions of its own kind.
 */
internal class JdbcDeadLetterStore(private val db: Db) : DeadLetterStore {
    private val live = "expire_at > :now"

    override suspend fun park(letter: DeadLetter): Boolean = db.own { jdbc ->
        Limits.check("a dead letter's id", letter.id, Limits.DEAD_LETTER_ID)
        Limits.check("a dead letter's processor", letter.processor, Limits.PROCESSOR)
        Limits.check("a dead letter's source", letter.source, Limits.SOURCE)
        Limits.check("a dead letter's segment", letter.segment, Limits.SEGMENT)
        Limits.check("a dead letter's position", letter.position, Limits.POSITION)
        val now = db.ts(db.now())
        jdbc.sql("DELETE FROM kraken_dead_letters WHERE id = :id AND expire_at <= :now").param("id", letter.id).param("now", now).update()
        db.insertOnce(
            jdbc, "kraken_dead_letters",
            "id, processor, source, segment, position, position_num, event_key, event_key_hash, event_id, event_type, headers, payload, payload_binary, " +
                "reason, error_type, error_message, blocked_by, attempts, parked_at, last_attempt_at, expire_at, replay",
            ":id, :processor, :source, :segment, :position, :positionNum, :key, :keyHash, :eventId, :eventType, ${db.dialect.json(":headers")}, " +
                ":payload, :payloadBinary, :reason, :errorType, :errorMessage, :blockedBy, :attempts, :parkedAt, :lastAttemptAt, :expireAt, :replay",
            "id",
            mapOf(
                "id" to letter.id, "processor" to letter.processor, "source" to letter.source, "segment" to letter.segment,
                "position" to letter.position, "positionNum" to positionNumber(letter.position), "key" to letter.key,
                "keyHash" to letter.key?.let(Limits::sha256),
                "eventId" to letter.eventId, "eventType" to letter.eventType, "headers" to Json.write("a dead letter's headers", letter.headers),
                "payload" to letter.payload, "payloadBinary" to letter.payloadBinary, "reason" to letter.reason.name,
                "errorType" to Limits.cut(letter.error.type), "errorMessage" to Limits.cut(letter.error.message), "blockedBy" to letter.blockedBy,
                "attempts" to letter.attempts, "parkedAt" to db.ts(letter.parkedAt), "lastAttemptAt" to db.ts(letter.lastAttemptAt),
                "expireAt" to db.ts(letter.expireAt), "replay" to letter.replay,
            ),
        )
    }

    override suspend fun head(processor: String, key: String): String? = db.own { jdbc ->
        firstId(jdbc, "", mapOf("processor" to processor, "keyHash" to Limits.sha256(key)))
    }

    override suspend fun earlier(processor: String, key: String, position: String): String? = db.own { jdbc ->
        val number = positionNumber(position)
        if (number != null) {
            firstId(jdbc, "AND position_num < :bound", mapOf("processor" to processor, "keyHash" to Limits.sha256(key), "bound" to number))
        } else {
            firstId(jdbc, "AND position_num IS NULL AND position < :bound", mapOf("processor" to processor, "keyHash" to Limits.sha256(key), "bound" to position))
        }
    }

    override suspend fun exists(id: String): Boolean = db.own { jdbc ->
        jdbc.sql("SELECT COUNT(*) FROM kraken_dead_letters WHERE id = :id AND $live").param("id", id).param("now", db.ts(db.now()))
            .query(Long::class.javaObjectType).single() > 0
    }

    override suspend fun keys(processor: String, source: String, segments: Collection<String>): Map<String, Set<String>> {
        if (segments.isEmpty()) return emptyMap()
        return db.own { jdbc ->
            jdbc.sql(
                "SELECT DISTINCT segment, event_key FROM kraken_dead_letters WHERE processor = :processor AND source = :source " +
                    "AND segment IN (:segments) AND event_key IS NOT NULL AND reason <> 'INVALID' AND $live",
            ).param("processor", processor).param("source", source).param("segments", segments.toList()).param("now", db.ts(db.now()))
                .query { rs, _ -> rs.getString("segment") to rs.getString("event_key") }.list()
                .groupBy({ it.first }, { it.second }).mapValues { it.value.toSet() }
        }
    }

    override suspend fun count(processor: String?): Long = db.own { jdbc ->
        jdbc.sql("SELECT COUNT(*) FROM kraken_dead_letters WHERE $live" + (if (processor != null) " AND processor = :processor" else ""))
            .param("now", db.ts(db.now())).also { if (processor != null) it.param("processor", processor) }
            .query(Long::class.javaObjectType).single()
    }

    override suspend fun list(processor: String?, limit: Int): List<DeadLetter> = db.own { jdbc ->
        jdbc.sql(
            "SELECT * FROM kraken_dead_letters WHERE $live" + (if (processor != null) " AND processor = :processor" else "") +
                " ORDER BY parked_at, id LIMIT :limit",
        ).param("now", db.ts(db.now())).param("limit", limit).also { if (processor != null) it.param("processor", processor) }
            .query(::read).list()
    }

    override suspend fun summaries(processor: String): List<DeadLetterStore.Summary> = db.own { jdbc ->
        jdbc.sql("SELECT id, event_key, position, reason, parked_at FROM kraken_dead_letters WHERE processor = :processor AND $live ORDER BY $POSITION_ORDER, id")
            .param("processor", processor).param("now", db.ts(db.now()))
            .query { rs, _ ->
                DeadLetterStore.Summary(
                    rs.getString("id"), rs.getString("event_key"), rs.getString("position"),
                    DeadLetterReason.valueOf(rs.getString("reason")), db.instant(rs, "parked_at")!!,
                )
            }.list()
    }

    override suspend fun get(id: String): DeadLetter? = db.own { jdbc ->
        jdbc.sql("SELECT * FROM kraken_dead_letters WHERE id = :id AND $live").param("id", id).param("now", db.ts(db.now()))
            .query(::read).optional().orElse(null)
    }

    override suspend fun delete(id: String): Boolean = db.own { jdbc -> deleteLive(jdbc, id) }

    override suspend fun deleteInTransaction(id: String): Boolean = db.tx { jdbc -> deleteLive(jdbc, id) }

    override suspend fun recordFailure(id: String, error: DeadLetterError): Boolean = db.own { jdbc ->
        val now = db.ts(db.now())
        jdbc.sql(
            "UPDATE kraken_dead_letters SET attempts = attempts + 1, error_type = :type, error_message = :message, last_attempt_at = :now, " +
                "reason = 'HANDLER_FAILED', blocked_by = NULL WHERE id = :id AND $live",
        ).param("type", Limits.cut(error.type)).param("message", Limits.cut(error.message)).param("now", now).param("id", id).update() > 0
    }

    private fun deleteLive(jdbc: JdbcClient, id: String): Boolean =
        jdbc.sql("DELETE FROM kraken_dead_letters WHERE id = :id AND $live").param("id", id).param("now", db.ts(db.now())).update() > 0

    /** The head among the blocking letters of (processor, key) matching [condition]. */
    private fun firstId(jdbc: JdbcClient, condition: String, params: Map<String, Any?>): String? =
        jdbc.sql(
            "SELECT id FROM kraken_dead_letters WHERE processor = :processor AND event_key_hash = :keyHash AND reason <> 'INVALID' AND $live " +
                "$condition ORDER BY $POSITION_ORDER, id LIMIT 1",
        ).params(params).param("now", db.ts(db.now())).query(String::class.java).optional().orElse(null)

    private fun read(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int) = DeadLetter(
        id = rs.getString("id"),
        processor = rs.getString("processor"),
        source = rs.getString("source"),
        segment = rs.getString("segment"),
        position = rs.getString("position"),
        key = rs.getString("event_key"),
        eventId = rs.getString("event_id"),
        eventType = rs.getString("event_type"),
        headers = Json.strings(rs.getString("headers")),
        payload = rs.getString("payload"),
        payloadBinary = rs.getBytes("payload_binary"),
        reason = DeadLetterReason.valueOf(rs.getString("reason")),
        error = DeadLetterError(rs.getString("error_type"), rs.getString("error_message")),
        blockedBy = rs.getString("blocked_by"),
        attempts = rs.getInt("attempts"),
        parkedAt = db.instant(rs, "parked_at")!!,
        lastAttemptAt = db.instant(rs, "last_attempt_at")!!,
        expireAt = db.instant(rs, "expire_at")!!,
        replay = rs.getBoolean("replay"),
    )
}
