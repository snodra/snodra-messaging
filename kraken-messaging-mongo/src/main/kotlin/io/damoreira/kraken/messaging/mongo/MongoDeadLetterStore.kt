package io.damoreira.kraken.messaging.mongo

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.store.DeadLetterStore
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.Binary
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock
import java.util.Date

/**
 * The `dead_letters` collection (ADR 0022). The documents are written field by field, so their shape is exactly the ADR's
 * table: no `_class`, an absent field instead of null, `topic` (the source), `partition` (the segment) and `offset` (the
 * position) as [Positions] says (Int32 and Int64 for decimal ones, as always), dates and BinData. Every write but
 * [deleteInTransaction] commits on its own, whatever transaction the caller has.
 */
internal class MongoDeadLetterStore(private val mongo: MongoTemplate, private val clock: Clock) : DeadLetterStore {
    private val own get() = mongo.independent(COLLECTION)

    override suspend fun park(letter: DeadLetter): Boolean = try {
        own.insertOne(document(letter))
        true
    } catch (e: MongoWriteException) {
        if (e.error.category == ErrorCategory.DUPLICATE_KEY) false else throw e
    }

    override suspend fun head(processor: String, key: String): String? = firstId(blocking(processor, key))

    override suspend fun earlier(processor: String, key: String, position: String): String? =
        firstId(Filters.and(blocking(processor, key), Filters.lt("offset", Positions.position(position))))

    override suspend fun exists(id: String): Boolean = own.find(Filters.eq("_id", id)).projection(Projections.include("_id")).first() != null

    override suspend fun keys(processor: String, source: String, segments: Collection<String>): Map<String, Set<String>> {
        if (segments.isEmpty()) return emptyMap()
        val filter = Filters.and(
            Filters.eq("processor", processor), Filters.eq("topic", source), Filters.`in`("partition", segments.map(Positions::segment)),
            Filters.exists("key"), Filters.ne("reason", DeadLetterReason.INVALID.name),
        )
        val out = HashMap<String, MutableSet<String>>()
        own.find(filter).projection(Projections.include("partition", "key")).forEach { doc ->
            val segment = Positions.text(doc["partition"]) ?: return@forEach
            out.getOrPut(segment) { HashSet() } += doc.getString("key")
        }
        return out
    }

    override suspend fun count(processor: String?): Long = own.countDocuments(processor?.let { Filters.eq("processor", it) } ?: Document())

    override suspend fun list(processor: String?, limit: Int): List<DeadLetter> =
        own.find(processor?.let { Filters.eq("processor", it) } ?: Document())
            .sort(Sorts.ascending("parkedAt", "_id")).limit(limit).map { read(it) }.toList()

    override suspend fun summaries(processor: String): List<DeadLetterStore.Summary> =
        own.find(Filters.eq("processor", processor)).projection(Projections.include("key", "offset", "reason", "parkedAt"))
            .sort(Sorts.ascending("offset", "_id"))
            .map {
                DeadLetterStore.Summary(
                    it.getString("_id"), it.getString("key"), Positions.text(it["offset"]) ?: "",
                    DeadLetterReason.valueOf(it.getString("reason")), it.getDate("parkedAt").toInstant(),
                )
            }.toList()

    override suspend fun get(id: String): DeadLetter? = own.find(Filters.eq("_id", id)).first()?.let { read(it) }

    override suspend fun delete(id: String): Boolean = own.deleteOne(Filters.eq("_id", id)).deletedCount > 0

    override suspend fun deleteInTransaction(id: String): Boolean = mongo.joined(COLLECTION).deleteOne(Filters.eq("_id", id)).deletedCount > 0

    override suspend fun recordFailure(id: String, error: DeadLetterError): Boolean = own.updateOne(
        Filters.eq("_id", id),
        Updates.combine(
            Updates.inc("attempts", 1),
            Updates.set("error", Document("type", error.type).append("message", error.message)),
            Updates.set("lastAttemptAt", Date.from(clock.instant())),
            Updates.set("reason", DeadLetterReason.HANDLER_FAILED.name),
            Updates.unset("blockedBy"),
        ),
    ).matchedCount > 0

    private fun blocking(processor: String, key: String): Bson =
        Filters.and(Filters.eq("processor", processor), Filters.eq("key", key), Filters.ne("reason", DeadLetterReason.INVALID.name))

    private fun firstId(filter: Bson): String? =
        own.find(filter).sort(Sorts.ascending("offset", "_id")).projection(Projections.include("_id")).first()?.getString("_id")

    private fun document(l: DeadLetter): Document {
        val doc = Document("_id", l.id)
            .append("processor", l.processor)
            .append("topic", l.source)
            .append("partition", Positions.segment(l.segment))
            .append("offset", Positions.position(l.position))
        l.key?.let { doc["key"] = it }
        l.eventId?.let { doc["eventId"] = it }
        l.eventType?.let { doc["eventType"] = it }
        doc["headers"] = Document(l.headers)
        l.payload?.let { doc["payload"] = it }
        l.payloadBinary?.let { doc["payloadBinary"] = Binary(it) }
        doc["reason"] = l.reason.name
        doc["error"] = Document("type", l.error.type).append("message", l.error.message)
        l.blockedBy?.let { doc["blockedBy"] = it }
        doc["attempts"] = l.attempts
        doc["parkedAt"] = Date.from(l.parkedAt)
        doc["lastAttemptAt"] = Date.from(l.lastAttemptAt)
        doc["expireAt"] = Date.from(l.expireAt)
        if (l.replay) doc["replay"] = true
        return doc
    }

    private fun read(d: Document): DeadLetter {
        val error = d.get("error", Document::class.java)
        return DeadLetter(
            id = d.getString("_id"),
            processor = d.getString("processor"),
            source = d.getString("topic"),
            segment = Positions.text(d["partition"]) ?: "",
            position = Positions.text(d["offset"]) ?: "",
            key = d.getString("key"),
            eventId = d.getString("eventId"),
            eventType = d.getString("eventType"),
            headers = (d.get("headers", Document::class.java) ?: Document()).mapValues { it.value?.toString() ?: "" },
            payload = d.getString("payload"),
            payloadBinary = (d["payloadBinary"] as? Binary)?.data,
            reason = DeadLetterReason.valueOf(d.getString("reason")),
            error = DeadLetterError(error?.getString("type") ?: "", error?.getString("message") ?: ""),
            blockedBy = d.getString("blockedBy"),
            attempts = (d["attempts"] as? Number)?.toInt() ?: 0,
            parkedAt = d.getDate("parkedAt").toInstant(),
            lastAttemptAt = (d.getDate("lastAttemptAt") ?: d.getDate("parkedAt")).toInstant(),
            expireAt = d.getDate("expireAt").toInstant(),
            replay = d.getBoolean("replay") ?: false,
        )
    }

    companion object {
        const val COLLECTION = "dead_letters"
    }
}
