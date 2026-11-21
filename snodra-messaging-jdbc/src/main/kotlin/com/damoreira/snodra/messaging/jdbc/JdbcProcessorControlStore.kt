package com.damoreira.snodra.messaging.jdbc

import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.store.ProcessorControlStore
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Companion.KIND_RESET
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Control
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Lock
import com.damoreira.snodra.messaging.store.ProcessorControlStore.Operation
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet
import java.time.Duration

/**
 * `snodra_processor_control`: one row per processor, created by the first write that needs it (not paused, no
 * operation, no replay). Every write commits on its own; each method is one transaction, so a lock taken, renewed or released
 * is atomic (a conditional `UPDATE` on the row). The replay ranges are a JSON array of `{"source","segment","from","until"}`.
 */
internal class JdbcProcessorControlStore(private val db: Db) : ProcessorControlStore {
    override suspend fun read(processors: Collection<String>): Map<String, Control> {
        if (processors.isEmpty()) return emptyMap()
        return db.own { jdbc ->
            jdbc.sql("SELECT * FROM snodra_processor_control WHERE processor IN (:names)").param("names", processors.toList())
                .query(::read).list().associateBy { it.processor }
        }
    }

    override suspend fun get(processor: String): Control? = db.own { jdbc -> get(jdbc, processor) }

    override suspend fun setPaused(processor: String, paused: Boolean, by: String) {
        db.own { jdbc ->
            val now = db.ts(db.now())
            ensure(jdbc, processor)
            jdbc.sql("UPDATE snodra_processor_control SET paused = :paused, paused_at = :pausedAt, paused_by = :by, updated_at = :now WHERE processor = :processor")
                .param("paused", paused).param("pausedAt", if (paused) now else null).param("by", if (paused) by else null)
                .param("now", now).param("processor", processor).update()
        }
    }

    override suspend fun lock(processor: String, op: String, lease: Duration): Lock = db.own { jdbc ->
        val now = db.now()
        // The row first: on MySQL an UPDATE that finds no row takes a gap lock, and two of those followed by inserts deadlock.
        ensure(jdbc, processor)
        if (renew(jdbc, processor, op, lease)) return@own Lock.Taken
        val taken = jdbc.sql(
            "UPDATE snodra_processor_control SET op_id = :op, op_kind = :kind, op_lease_until = :until, op_prepared_at = :now, updated_at = :now " +
                "WHERE processor = :processor AND (op_id IS NULL OR op_lease_until <= :now)",
        ).param("op", op).param("kind", KIND_RESET).param("until", db.ts(now.plus(lease))).param("now", db.ts(now))
            .param("processor", processor).update() == 1
        if (taken) Lock.Taken else Lock.Held(get(jdbc, processor)?.operation)
    }

    override suspend fun renew(processor: String, op: String, lease: Duration): Boolean = db.own { jdbc -> renew(jdbc, processor, op, lease) }

    override suspend fun release(processor: String, op: String, expiredOnly: Boolean): Boolean = db.own { jdbc ->
        jdbc.sql(
            "UPDATE snodra_processor_control SET op_id = NULL, op_kind = NULL, op_lease_until = NULL, op_prepared_at = NULL, updated_at = :now " +
                "WHERE processor = :processor AND op_id = :op" + (if (expiredOnly) " AND op_lease_until <= :now" else ""),
        ).param("now", db.ts(db.now())).param("processor", processor).param("op", op).update() == 1
    }

    override suspend fun setReplay(processor: String, ranges: List<ReplayRange>) {
        Limits.check("a processor's name", processor, Limits.PROCESSOR)
        db.own { jdbc -> writeReplay(jdbc, processor, ranges) }
    }

    override suspend fun removeReplay(processor: String, range: ReplayRange): Boolean = db.own { jdbc ->
        val ranges = jdbc.sql("SELECT replay FROM snodra_processor_control WHERE processor = :processor FOR UPDATE")
            .param("processor", processor).query { rs, _ -> ranges(rs.getString("replay")) }.optional().orElse(null)
        if (ranges == null || range !in ranges) return@own false
        writeReplay(jdbc, processor, ranges.filter { it != range })
        true
    }

    override suspend fun deleteMarkers(processor: String, ranges: List<ReplayRange>): Long {
        if (ranges.isEmpty()) return 0
        return db.own { jdbc ->
            ranges.sumOf { range ->
                val params = mutableMapOf<String, Any?>("processor" to processor, "source" to range.source, "segment" to range.segment)
                val from = bound("position", ">=", "from", range.from, params)
                val until = bound("position", "<", "until", range.until, params)
                jdbc.sql("DELETE FROM snodra_processed_events WHERE processor = :processor AND source = :source AND segment = :segment AND $from AND $until")
                    .params(params).update().toLong()
            }
        }
    }

    /** A bound on a position compares only positions of its kind: decimal ones by value, the others by their bytes. */
    private fun bound(column: String, op: String, name: String, position: String, params: MutableMap<String, Any?>): String {
        val number = positionNumber(position)
        return if (number != null) {
            params[name] = number
            "${column}_num $op :$name"
        } else {
            params[name] = position
            "${column}_num IS NULL AND $column $op :$name"
        }
    }

    private fun renew(jdbc: JdbcClient, processor: String, op: String, lease: Duration): Boolean {
        val now = db.now()
        return jdbc.sql("UPDATE snodra_processor_control SET op_lease_until = :until, updated_at = :now WHERE processor = :processor AND op_id = :op")
            .param("until", db.ts(now.plus(lease))).param("now", db.ts(now)).param("processor", processor).param("op", op).update() == 1
    }

    /** Creates [processor]'s row (not paused, no operation, no replay) unless it exists. */
    private fun ensure(jdbc: JdbcClient, processor: String) {
        Limits.check("a processor's name", processor, Limits.PROCESSOR)
        jdbc.sql(db.dialect.insertUnlessExists("snodra_processor_control", "processor, paused, replay", ":processor, FALSE, ${db.dialect.json(":replay")}", "processor"))
            .param("processor", processor).param("replay", "[]").update()
    }

    private fun writeReplay(jdbc: JdbcClient, processor: String, ranges: List<ReplayRange>) {
        val json = Json.write("a replay range", ranges.map { mapOf("source" to it.source, "segment" to it.segment, "from" to it.from, "until" to it.until) })
        jdbc.sql("UPDATE snodra_processor_control SET replay = ${db.dialect.json(":replay")}, updated_at = :now WHERE processor = :processor")
            .param("replay", json).param("now", db.ts(db.now())).param("processor", processor).update()
    }

    private fun get(jdbc: JdbcClient, processor: String): Control? =
        jdbc.sql("SELECT * FROM snodra_processor_control WHERE processor = :processor").param("processor", processor).query(::read).optional().orElse(null)

    private fun ranges(json: String?): List<ReplayRange> = json?.let { Json.mapper.readTree(it) }?.values()?.mapNotNull { r ->
        ReplayRange(
            r.get("source")?.asString() ?: return@mapNotNull null,
            r.get("segment")?.asString() ?: return@mapNotNull null,
            r.get("from")?.asString() ?: return@mapNotNull null,
            r.get("until")?.asString() ?: return@mapNotNull null,
        )
    }.orEmpty()

    private fun read(rs: ResultSet, @Suppress("UNUSED_PARAMETER") row: Int): Control {
        val opId = rs.getString("op_id")
        return Control(
            processor = rs.getString("processor"),
            paused = rs.getBoolean("paused"),
            pausedAt = db.instant(rs, "paused_at"),
            pausedBy = rs.getString("paused_by"),
            operation = opId?.let {
                Operation(it, rs.getString("op_kind") ?: KIND_RESET, db.instant(rs, "op_lease_until")!!, db.instant(rs, "op_prepared_at")!!)
            },
            replay = ranges(rs.getString("replay")),
            updatedAt = db.instant(rs, "updated_at"),
        )
    }
}
