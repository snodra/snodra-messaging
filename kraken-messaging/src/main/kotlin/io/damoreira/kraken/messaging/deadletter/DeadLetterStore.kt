package io.damoreira.kraken.messaging.deadletter

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import io.damoreira.kraken.messaging.store.DeadLetterStore.Summary
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.Binary
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.concurrent.TimeUnit

/**
 * The `dead_letters` collection (ADR 0022): building letters from records, parking them (a single-document insert outside
 * any transaction; a duplicate `_id` means already parked), the head and key queries of sequencing, reads, the retry update
 * and the indexes. The documents are written with the plain driver, field by field, so their shape is exactly the ADR's
 * table: no `_class`, an absent field instead of null, Int32 `partition`, Int64 `offset`, dates and BinData.
 */
open class DeadLetterStore(
    private val mongo: MongoTemplate,
    private val retention: Duration,
    private val clock: Clock = Clock.systemUTC(),
) {
    init {
        require(retention >= Duration.ofSeconds(1)) { "dead-letter retention must be at least 1s, was $retention" }
    }

    private val collection: MongoCollection<Document> get() = mongo.db.getCollection(COLLECTION)

    /** The letter of [record] in [processor], as a document ready to [park]; [replay]: parked while the processor replays it. */
    fun letter(
        processor: String, record: ConsumerRecord<out String?, out ByteArray?>, reason: DeadLetterReason, error: DeadLetterError,
        attempts: Int, blockedBy: String? = null, replay: Boolean = false,
    ): Document {
        val now = Date.from(clock.instant())
        val headers = Document()
        record.headers().forEach { headers[it.key()] = lossy(it.value()) } // a repeated name keeps its last value
        val doc = Document("_id", id(processor, record.topic(), record.partition(), record.offset()))
            .append("processor", processor)
            .append("topic", record.topic())
            .append("partition", record.partition())
            .append("offset", record.offset())
        record.key()?.let { doc["key"] = it }
        // A blank id or type is no id or type (as the consumer reads it): absent, never "".
        headers.getString(ID_HEADER)?.takeIf { it.isNotBlank() }?.let { doc["eventId"] = it }
        headers.getString(TYPE_HEADER)?.takeIf { it.isNotBlank() }?.let { doc["eventType"] = it }
        doc["headers"] = headers
        val value = record.value() ?: ByteArray(0)
        if (value.isNotEmpty()) {
            val text = strictUtf8(value)
            if (text != null) doc["payload"] = text else doc["payloadBinary"] = Binary(value)
        }
        doc["reason"] = reason.name
        doc["error"] = Document("type", error.type).append("message", error.message)
        blockedBy?.let { doc["blockedBy"] = it }
        doc["attempts"] = attempts
        doc["parkedAt"] = now
        doc["lastAttemptAt"] = now
        doc["expireAt"] = Date.from(clock.instant().plus(retention))
        if (replay) doc["replay"] = true
        return doc
    }

    /** The SEQUENCED letter of [record] waiting behind [headId]. */
    fun sequenced(processor: String, record: ConsumerRecord<out String?, out ByteArray?>, headId: String, replay: Boolean = false): Document =
        letter(processor, record, DeadLetterReason.SEQUENCED, DeadLetterError.sequenced(headId), attempts = 0, blockedBy = headId, replay = replay)

    /** Inserts [letter] (outside any transaction): true when parked now, false when it was parked already. Throws when MongoDB fails. */
    open fun park(letter: Document): Boolean = try {
        collection.insertOne(letter)
        true
    } catch (e: MongoWriteException) {
        if (e.error.category == ErrorCategory.DUPLICATE_KEY) false else throw e
    }

    /** `_id` of the head letter of ([processor], [key]): the lowest `offset` that is not INVALID (ties by `_id`), or null. */
    fun head(processor: String, key: String): String? = firstId(Filters.and(blocking(processor, key)))

    /** `_id` of the head among the letters of ([processor], [key]) with an offset below [offset], or null. */
    fun earlier(processor: String, key: String, offset: Long): String? =
        firstId(Filters.and(blocking(processor, key), Filters.lt("offset", offset)))

    /** Whether a letter with [id] exists. */
    fun exists(id: String): Boolean = collection.find(Filters.eq("_id", id)).projection(Projections.include("_id")).first() != null

    /** The keys of [processor]'s blocking (not INVALID) letters on [partitions] of [topic], by partition. */
    fun keys(processor: String, topic: String, partitions: Collection<Int>): Map<Int, Set<String>> {
        if (partitions.isEmpty()) return emptyMap()
        val filter = Filters.and(
            Filters.eq("processor", processor), Filters.eq("topic", topic), Filters.`in`("partition", partitions),
            Filters.exists("key"), Filters.ne("reason", DeadLetterReason.INVALID.name),
        )
        val out = HashMap<Int, MutableSet<String>>()
        collection.find(filter).projection(Projections.include("partition", "key"))
            .forEach { out.getOrPut(it.getInteger("partition")) { HashSet() } += it.getString("key") }
        return out
    }

    fun count(processor: String?): Long = collection.countDocuments(processor?.let { Filters.eq("processor", it) } ?: Document())

    /** Letters sorted by `parkedAt`, then `_id`. */
    fun list(processor: String?, limit: Int): List<DeadLetter> =
        collection.find(processor?.let { Filters.eq("processor", it) } ?: Document())
            .sort(Sorts.ascending("parkedAt", "_id")).limit(limit).map { read(it) }.toList()

    /** Every letter of [processor] (id, key, offset, reason, parkedAt only), for `retryAll`. */
    internal fun summaries(processor: String): List<Summary> =
        collection.find(Filters.eq("processor", processor)).projection(Projections.include("key", "offset", "reason", "parkedAt"))
            .map { Summary(it.getString("_id"), it.getString("key"), (it["offset"] as Number).toLong(), DeadLetterReason.valueOf(it.getString("reason")), it.getDate("parkedAt").toInstant()) }
            .toList()

    fun get(id: String): DeadLetter? = collection.find(Filters.eq("_id", id)).first()?.let { read(it) }

    /** Deletes the letter (outside any transaction); true when it existed. */
    fun delete(id: String): Boolean = collection.deleteOne(Filters.eq("_id", id)).deletedCount > 0

    /** Deletes the letter through the template, so inside a running Spring transaction; true when it existed. */
    fun deleteInTransaction(id: String): Boolean =
        mongo.remove(Query(Criteria.where("_id").`is`(id)), COLLECTION).deletedCount > 0

    /** A retry that failed again: `attempts + 1`, [error], `lastAttemptAt`, reason HANDLER_FAILED, no `blockedBy`; false when the letter is gone. */
    fun recordFailure(id: String, error: DeadLetterError): Boolean = collection.updateOne(
        Filters.eq("_id", id),
        Updates.combine(
            Updates.inc("attempts", 1),
            Updates.set("error", Document("type", error.type).append("message", error.message)),
            Updates.set("lastAttemptAt", Date.from(clock.instant())),
            Updates.set("reason", DeadLetterReason.HANDLER_FAILED.name),
            Updates.unset("blockedBy"),
        ),
    ).matchedCount > 0

    /** Creates the three indexes of the ADR (idempotent). */
    fun ensureIndexes() {
        collection.createIndex(Indexes.ascending("processor", "key", "offset"), IndexOptions().name("dead_letters_processor_key_offset"))
        collection.createIndex(Indexes.ascending("processor", "parkedAt"), IndexOptions().name("dead_letters_processor_parked"))
        collection.createIndex(Indexes.ascending("expireAt"), IndexOptions().name("dead_letters_expire").expireAfter(0L, TimeUnit.SECONDS))
    }

    private fun blocking(processor: String, key: String): Bson =
        Filters.and(Filters.eq("processor", processor), Filters.eq("key", key), Filters.ne("reason", DeadLetterReason.INVALID.name))

    private fun firstId(filter: Bson): String? =
        collection.find(filter).sort(Sorts.ascending("offset", "_id")).projection(Projections.include("_id")).first()?.getString("_id")

    @Suppress("UNCHECKED_CAST")
    private fun read(d: Document): DeadLetter {
        val error = d.get("error", Document::class.java)
        return DeadLetter(
            id = d.getString("_id"),
            processor = d.getString("processor"),
            topic = d.getString("topic"),
            partition = (d["partition"] as Number).toInt(),
            offset = (d["offset"] as Number).toLong(),
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
        private const val ID_HEADER = "id"
        private const val TYPE_HEADER = "type"

        /** `<processor>:<topic>:<partition>:<offset>`. */
        fun id(processor: String, topic: String, partition: Int, offset: Long) = "$processor:$topic:$partition:$offset"

        internal fun lossy(bytes: ByteArray?): String = if (bytes == null) "" else String(bytes, Charsets.UTF_8)

        private fun strictUtf8(bytes: ByteArray): String? = try {
            Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString()
        } catch (_: CharacterCodingException) {
            null
        }
    }
}
