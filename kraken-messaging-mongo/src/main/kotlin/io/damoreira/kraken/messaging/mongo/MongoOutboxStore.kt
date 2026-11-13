package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.OutboxStore
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Date

/**
 * The `outbox` collection: `{_id, type, key?, headers, payload (a document), createdAt, seq, sentAt?, attempts, failedAt?}` as
 * the library has always written it, plus `owner` and `leaseUntil` while a relay holds an entry (absent = unclaimed).
 */
internal class MongoOutboxStore(private val mongo: MongoTemplate, private val clock: Clock) : OutboxStore {
    private val collection get() = mongo.joined(COLLECTION)

    override suspend fun append(entry: OutboxEntry) {
        val doc = Document("_id", entry.id).append("type", entry.type)
        entry.key?.let { doc["key"] = it }
        doc["headers"] = Document(entry.headers)
        doc["payload"] = Document.parse(String(entry.payload, Charsets.UTF_8))
        doc["createdAt"] = Date.from(entry.createdAt)
        doc["seq"] = entry.seq
        doc["attempts"] = entry.attempts
        collection.insertOne(doc)
    }

    override suspend fun claim(owner: String, lease: Duration, limit: Int): List<OutboxEntry> {
        val won = LinkedHashMap<String, Document>()
        while (won.size < limit) {
            val candidates = collection.find(Filters.and(unfinished(), claimableBy(owner), Filters.nin("_id", won.keys)))
                .sort(Sorts.ascending("createdAt", "seq")).limit(limit - won.size).toList()
            if (candidates.isEmpty()) break
            for (candidate in candidates) {
                val id = candidate.getString("_id")
                // Conditional on the entry still being claimable: of two relays racing for it, exactly one wins.
                val claimed = collection.findOneAndUpdate(
                    Filters.and(Filters.eq("_id", id), unfinished(), claimableBy(owner)),
                    Updates.combine(Updates.set("owner", owner), Updates.set("leaseUntil", Date.from(clock.instant().plus(lease)))),
                    FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER),
                )
                if (claimed != null) won[id] = claimed
            }
        }
        return won.values.sortedWith(compareBy({ it.getDate("createdAt") }, { (it["seq"] as Number).toLong() })).map(::read)
    }

    override suspend fun markSent(ids: Collection<String>) {
        if (ids.isEmpty()) return
        collection.updateMany(Filters.`in`("_id", ids), Updates.set("sentAt", Date.from(clock.instant())))
    }

    override suspend fun markAttemptFailed(id: String, error: String) {
        collection.updateOne(Filters.eq("_id", id), Updates.inc("attempts", 1))
    }

    override suspend fun markFailed(id: String, error: String) {
        collection.updateOne(Filters.eq("_id", id), Updates.combine(Updates.inc("attempts", 1), Updates.set("failedAt", Date.from(clock.instant()))))
    }

    override suspend fun pending(): Long = collection.countDocuments(unfinished())

    override suspend fun purge(sentBefore: Instant): Long = collection.deleteMany(Filters.lt("sentAt", Date.from(sentBefore))).deletedCount

    /** Not sent and not given up on (a missing field is null). */
    private fun unfinished() = Filters.and(Filters.eq("sentAt", null), Filters.eq("failedAt", null))

    /** Free, already [owner]'s, or whose lease passed. */
    private fun claimableBy(owner: String) = Filters.or(
        Filters.eq("owner", null), Filters.eq("owner", owner), Filters.lte("leaseUntil", Date.from(clock.instant())),
    )

    @Suppress("UNCHECKED_CAST")
    private fun read(d: Document) = OutboxEntry(
        id = d.getString("_id"),
        type = d.getString("type"),
        key = d.getString("key"),
        headers = (d.get("headers", Document::class.java) ?: Document()).mapValues { it.value?.toString() ?: "" },
        payload = d.get("payload", Document::class.java).toJson().toByteArray(Charsets.UTF_8),
        createdAt = d.getDate("createdAt").toInstant(),
        seq = (d["seq"] as Number).toLong(),
        attempts = (d["attempts"] as? Number)?.toInt() ?: 0,
    )

    companion object {
        const val COLLECTION = "outbox"
    }
}
