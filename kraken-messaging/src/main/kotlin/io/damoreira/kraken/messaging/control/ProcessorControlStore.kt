package io.damoreira.kraken.messaging.control

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import org.bson.Document
import org.bson.conversions.Bson
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Date

/**
 * The `processor_control` collection (P6b, ADR 0023): one document per event processor, `_id` its name, the source of truth
 * of its pause, of the reset operation holding its lock and of its open replay ranges. The hub keeps none of it; every
 * instance reads it at start and on every poll. Written with the plain driver, every field always present (null, `false` or
 * `[]` when unset, the shape both libraries write); a missing document or field reads as not paused, no operation, no replay.
 */
open class ProcessorControlStore(
    private val mongo: MongoTemplate,
    private val clock: Clock = Clock.systemUTC(),
) {
    /** The reset operation holding a processor's lock until [leaseUntil]. */
    data class Operation(val id: String, val kind: String, val leaseUntil: Instant, val preparedAt: Instant) {
        fun expired(now: Instant): Boolean = !leaseUntil.isAfter(now)
    }

    /** One processor's document. */
    data class Control(
        val processor: String,
        val paused: Boolean = false,
        val pausedAt: Instant? = null,
        val pausedBy: String? = null,
        val operation: Operation? = null,
        val replay: List<ReplayRange> = emptyList(),
        val updatedAt: Instant? = null,
    )

    /** What [lock] did. */
    sealed interface Lock {
        /** The lock is [op]'s now (it was free, expired, or already [op]'s). */
        data object Taken : Lock

        /** Another operation holds it. */
        data class Held(val by: Operation?) : Lock
    }

    private val collection: MongoCollection<Document> get() = mongo.db.getCollection(COLLECTION)

    /** The documents of [processors] that exist, by name. */
    fun read(processors: Collection<String>): Map<String, Control> =
        collection.find(Filters.`in`("_id", processors.toList())).map(::parse).associateBy { it.processor }

    /** The document of [processor], or null. */
    fun get(processor: String): Control? = collection.find(Filters.eq("_id", processor)).first()?.let(::parse)

    /** Pauses or resumes [processor] ([by]: the control id, or `local`); idempotent. */
    fun setPaused(processor: String, paused: Boolean, by: String) {
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

    /**
     * Takes [processor]'s lock for [op] until [lease] from now, with one conditional update: when no operation holds it, or
     * its lease passed. A lock [op] already holds is only renewed ([renew]). A duplicate key on the upsert means another
     * operation holds it.
     */
    fun lock(processor: String, op: String, lease: Duration): Lock {
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
            if (ErrorCategory.fromErrorCode(e.code) != ErrorCategory.DUPLICATE_KEY) throw e
            Lock.Held(get(processor)?.operation)
        }
    }

    /** Moves the lease of [op] to [lease] from now; false when [op] does not hold [processor]'s lock. */
    fun renew(processor: String, op: String, lease: Duration): Boolean =
        collection.updateOne(
            held(processor, op),
            Updates.combine(Updates.set("operation.leaseUntil", Date.from(clock.instant().plus(lease))), Updates.set("updatedAt", now())),
        ).matchedCount == 1L

    /** Clears [op]'s lock ([expiredOnly]: only when its lease passed); false when [op] does not hold it (then nothing changes). */
    fun release(processor: String, op: String, expiredOnly: Boolean = false): Boolean {
        val filter = if (expiredOnly) Filters.and(held(processor, op), Filters.lte("operation.leaseUntil", now())) else held(processor, op)
        return collection.updateOne(filter, Updates.combine(Updates.set("operation", null), Updates.set("updatedAt", now()))).matchedCount == 1L
    }

    /** Replaces the open replay ranges of [processor] (it exists: its lock is held). */
    fun setReplay(processor: String, ranges: List<ReplayRange>) {
        collection.updateOne(
            Filters.eq("_id", processor),
            Updates.combine(Updates.set("replay", ranges.map(::rangeDocument)), Updates.set("updatedAt", now())),
        )
    }

    /** Removes the open range [range] (exactly that item) of [processor]; false when it was not there any more. */
    fun removeReplay(processor: String, range: ReplayRange): Boolean =
        collection.updateOne(
            Filters.and(Filters.eq("_id", processor), Filters.elemMatch("replay", rangeFilter(range))),
            Updates.combine(Updates.pull("replay", rangeDocument(range)), Updates.set("updatedAt", now())),
        ).modifiedCount == 1L

    private fun held(processor: String, op: String): Bson = Filters.and(Filters.eq("_id", processor), Filters.eq("operation.id", op))

    private fun now(): Date = Date.from(clock.instant())

    private fun rangeDocument(range: ReplayRange) =
        Document("topic", range.topic).append("partition", range.partition).append("from", range.from).append("until", range.until)

    private fun rangeFilter(range: ReplayRange): Bson = Filters.and(
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
            ReplayRange(topic, partition, from, until)
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

        /** `operation.kind` of a reset, the only kind so far. */
        const val KIND_RESET = "reset"
    }
}
