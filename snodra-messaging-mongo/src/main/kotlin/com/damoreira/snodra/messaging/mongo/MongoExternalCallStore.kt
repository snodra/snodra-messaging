package com.damoreira.snodra.messaging.mongo

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates
import com.damoreira.snodra.messaging.external.ExternalCallStatus
import com.damoreira.snodra.messaging.store.ExternalCallRecord
import com.damoreira.snodra.messaging.store.ExternalCallStore
import org.bson.Document
import org.bson.conversions.Bson
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.Date

/**
 * The `external_calls` collection: `{_id, status, reply?, error?, attempts, updatedAt, leaseUntil?, owner?}`, with `reply` and
 * `error` as JSON text. Every write after [insert] is conditional on the record being the one the caller read or owns.
 */
internal class MongoExternalCallStore(private val mongo: MongoTemplate) : ExternalCallStore {
    private val collection get() = mongo.joined(COLLECTION)

    override suspend fun insert(call: ExternalCallRecord): Boolean {
        val doc = Document("_id", call.id).append("status", call.status.name)
        call.reply?.let { doc["reply"] = String(it, Charsets.UTF_8) }
        call.error?.let { doc["error"] = String(it, Charsets.UTF_8) }
        doc["attempts"] = call.attempts
        doc["updatedAt"] = Date.from(call.updatedAt)
        call.leaseUntil?.let { doc["leaseUntil"] = Date.from(it) }
        call.owner?.let { doc["owner"] = it }
        return try {
            collection.insertOne(doc)
            true
        } catch (e: MongoWriteException) {
            if (e.error.category == ErrorCategory.DUPLICATE_KEY) false else throw e
        }
    }

    override suspend fun find(id: String): ExternalCallRecord? = collection.find(Filters.eq("_id", id)).first()?.let(::read)

    override suspend fun takeOver(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, owner: String, leaseUntil: Instant, now: Instant): ExternalCallRecord? =
        collection.findOneAndUpdate(
            asRead(id, expectedOwner, expectedLeaseUntil),
            Updates.combine(
                Updates.set("leaseUntil", Date.from(leaseUntil)), Updates.set("updatedAt", Date.from(now)),
                Updates.set("owner", owner), Updates.inc("attempts", 1),
            ),
            FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER),
        )?.let(::read)

    override suspend fun giveUp(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, now: Instant): Boolean =
        collection.updateOne(asRead(id, expectedOwner, expectedLeaseUntil), settle(ExternalCallStatus.UNKNOWN, now)).matchedCount > 0

    override suspend fun succeed(id: String, owner: String, reply: ByteArray, now: Instant): Boolean =
        collection.updateOne(owned(id, owner), Updates.combine(settle(ExternalCallStatus.SUCCEEDED, now), Updates.set("reply", String(reply, Charsets.UTF_8)))).matchedCount > 0

    override suspend fun fail(id: String, owner: String, error: ByteArray, now: Instant): Boolean =
        collection.updateOne(owned(id, owner), Updates.combine(settle(ExternalCallStatus.FAILED, now), Updates.set("error", String(error, Charsets.UTF_8)))).matchedCount > 0

    override suspend fun markUnknown(id: String, owner: String, now: Instant): Boolean =
        collection.updateOne(owned(id, owner), settle(ExternalCallStatus.UNKNOWN, now)).matchedCount > 0

    override suspend fun reset(id: String, owner: String): Boolean = collection.deleteOne(owned(id, owner)).deletedCount > 0

    /** The record is settled: [status], no lease, no owner (a settled `SUCCEEDED` call also has no error). */
    private fun settle(status: ExternalCallStatus, now: Instant): Bson = Updates.combine(
        Updates.set("status", status.name), Updates.set("updatedAt", Date.from(now)), Updates.unset("leaseUntil"), Updates.unset("owner"),
        *(if (status == ExternalCallStatus.SUCCEEDED) arrayOf(Updates.unset("error")) else emptyArray()),
    )

    private fun owned(id: String, owner: String): Bson =
        Filters.and(Filters.eq("_id", id), Filters.eq("status", ExternalCallStatus.IN_PROGRESS.name), Filters.eq("owner", owner))

    private fun asRead(id: String, owner: String?, leaseUntil: Instant?): Bson = Filters.and(
        Filters.eq("_id", id), Filters.eq("status", ExternalCallStatus.IN_PROGRESS.name), Filters.eq("owner", owner),
        Filters.eq("leaseUntil", leaseUntil?.let { Date.from(it) }),
    )

    private fun read(d: Document) = ExternalCallRecord(
        id = d.getString("_id"),
        status = ExternalCallStatus.valueOf(d.getString("status")),
        reply = d.getString("reply")?.toByteArray(Charsets.UTF_8),
        error = d.getString("error")?.toByteArray(Charsets.UTF_8),
        attempts = (d["attempts"] as? Number)?.toInt() ?: 0,
        updatedAt = d.getDate("updatedAt").toInstant(),
        leaseUntil = d.getDate("leaseUntil")?.toInstant(),
        owner = d.getString("owner"),
    )

    companion object {
        const val COLLECTION = "external_calls"
    }
}
