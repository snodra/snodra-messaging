package io.damoreira.kraken.messaging.mongo

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Companion.KIND_RESET
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Control
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Lock
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Operation
import org.bson.Document
import org.bson.conversions.Bson
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Date

/**
 * The `processor_control` collection (P6b, ADR 0023): one document per event processor, `_id` its name. Written field by
 * field, every field always present (null, `false` or `[]` when unset); a missing document or field reads as not paused, no
 * operation, no replay. Every write commits on its own, whatever transaction the caller has.
 */
internal class MongoProcessorControlStore(private val mongo: MongoTemplate, private val clock: Clock) : ProcessorControlStore {
    private val collection get() = mongo.independent(COLLECTION)

    override suspend fun read(processors: Collection<String>): Map<String, Control> =
        collection.find(Filters.`in`("_id", processors.toList())).map(::parse).associateBy { it.processor }

    override suspend fun get(processor: String): Control? = collection.find(Filters.eq("_id", processor)).first()?.let(::parse)

    override suspend fun setPaused(processor: String, paused: Boolean, by: String) {
        val now = now()
        collection.updateOne(
            Filters.eq("_id", processor),
            Updates.combine(
                Updates.set("paused", paused),
                Updates.set("pausedAt", if (paused) now else null),
                Updates.set("pausedBy", if (paused) by else null),
                Updates.set("updatedAt", now),
                Updates.setOnInsert("operation", null),
                Updates.setOnInsert("replay", emptyList<Document>()),
            ),
            UpdateOptions().upsert(true),
        )
    }

    override suspend fun lock(processor: String, op: String, lease: Duration): Lock {
        if (renew(processor, op, lease)) return Lock.Taken
        val now = now()
        val free = Filters.and(Filters.eq("_id", processor), Filters.or(Filters.eq("operation", null), Filters.lte("operation.leaseUntil", now)))
        val operation = Document("id", op).append("kind", KIND_RESET).append("leaseUntil", Date.from(clock.instant().plus(lease))).append("preparedAt", now)
        return try {
            collection.updateOne(
                free,
                Updates.combine(
                    Updates.set("operation", operation),
                    Updates.set("updatedAt", now),
                    Updates.setOnInsert("paused", false),
                    Updates.setOnInsert("pausedAt", null),
                    Updates.setOnInsert("pausedBy", null),
                    Updates.setOnInsert("replay", emptyList<Document>()),
                ),
                UpdateOptions().upsert(true),
            )
            Lock.Taken
        } catch (e: MongoWriteException) {
            // The upsert collided with the existing document: another operation holds it.
            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY) throw e
            Lock.Held(get(processor)?.operation)
        }
    }

    override suspend fun renew(processor: String, op: String, lease: Duration): Boolean =
        collection.updateOne(
            held(processor, op),
            Updates.combine(Updates.set("operation.leaseUntil", Date.from(clock.instant().plus(lease))), Updates.set("updatedAt", now())),
        ).matchedCount == 1L

    override suspend fun release(processor: String, op: String, expiredOnly: Boolean): Boolean {
        val filter = if (expiredOnly) Filters.and(held(processor, op), Filters.lte("operation.leaseUntil", now())) else held(processor, op)
        return collection.updateOne(filter, Updates.combine(Updates.set("operation", null), Updates.set("updatedAt", now()))).matchedCount == 1L
    }

    override suspend fun setReplay(processor: String, ranges: List<ReplayRange>) {
        collection.updateOne(
            Filters.eq("_id", processor),
            Updates.combine(Updates.set("replay", ranges.map { rangeDocument(stored(it)) }), Updates.set("updatedAt", now())),
        )
    }

    /** A range whose segment or positions are not decimal cannot have been stored: false. */
    override suspend fun removeReplay(processor: String, range: ReplayRange): Boolean {
        val stored = runCatching { stored(range) }.getOrNull() ?: return false
        return collection.updateOne(
            Filters.and(Filters.eq("_id", processor), Filters.elemMatch("replay", rangeFilter(stored))),
            Updates.combine(Updates.pull("replay", rangeDocument(stored)), Updates.set("updatedAt", now())),
        ).modifiedCount == 1L
    }

    override suspend fun deleteMarkers(processor: String, ranges: List<ReplayRange>): Long {
        val markers = mongo.independent(MongoProcessedEventStore.COLLECTION)
        var deleted = 0L
        for (range in ranges.map(::stored)) {
            deleted += markers.deleteMany(
                Filters.and(
                    Filters.eq("processor", processor), Filters.eq("topic", range.topic), Filters.eq("partition", range.partition),
                    Filters.gte("offset", range.from), Filters.lt("offset", range.until),
                ),
            ).deletedCount
        }
        return deleted
    }

    private fun held(processor: String, op: String): Bson = Filters.and(Filters.eq("_id", processor), Filters.eq("operation.id", op))

    private fun now(): Date = Date.from(clock.instant())

    /**
     * A range as `processor_control.replay` and the markers keep it: the source as `topic`, the segment as an int `partition`,
     * the positions as long offsets (the layout before neutral positions, kept so stored data stays readable). Anything else
     * is refused with an [IllegalArgumentException].
     */
    private class StoredRange(val topic: String, val partition: Int, val from: Long, val until: Long)

    private fun stored(range: ReplayRange): StoredRange {
        fun bad(): Nothing = throw IllegalArgumentException("This store keeps replay ranges as a partition and offsets in decimal: $range")
        return StoredRange(
            range.source, range.segment.toIntOrNull() ?: bad(), range.from.toLongOrNull() ?: bad(), range.until.toLongOrNull() ?: bad(),
        )
    }

    private fun rangeDocument(range: StoredRange) =
        Document("topic", range.topic).append("partition", range.partition).append("from", range.from).append("until", range.until)

    private fun rangeFilter(range: StoredRange): Bson = Filters.and(
        Filters.eq("topic", range.topic), Filters.eq("partition", range.partition), Filters.eq("from", range.from), Filters.eq("until", range.until),
    )

    private fun parse(doc: Document): Control {
        val operation = (doc["operation"] as? Document)?.let { op ->
            Operation(
                id = op.getString("id") ?: "",
                kind = op.getString("kind") ?: KIND_RESET,
                leaseUntil = op.getDate("leaseUntil")?.toInstant() ?: Instant.EPOCH,
                preparedAt = op.getDate("preparedAt")?.toInstant() ?: Instant.EPOCH,
            )
        }
        val replay = (doc["replay"] as? List<*>).orEmpty().filterIsInstance<Document>().mapNotNull { r ->
            val topic = r.getString("topic") ?: return@mapNotNull null
            val partition = (r["partition"] as? Number)?.toInt() ?: return@mapNotNull null
            val from = (r["from"] as? Number)?.toLong() ?: return@mapNotNull null
            val until = (r["until"] as? Number)?.toLong() ?: return@mapNotNull null
            ReplayRange(topic, partition.toString(), from.toString(), until.toString())
        }
        return Control(
            processor = doc.getString("_id"),
            paused = doc.getBoolean("paused") ?: false,
            pausedAt = doc.getDate("pausedAt")?.toInstant(),
            pausedBy = doc.getString("pausedBy"),
            operation = operation,
            replay = replay,
            updatedAt = doc.getDate("updatedAt")?.toInstant(),
        )
    }

    companion object {
        const val COLLECTION = "processor_control"
    }
}
