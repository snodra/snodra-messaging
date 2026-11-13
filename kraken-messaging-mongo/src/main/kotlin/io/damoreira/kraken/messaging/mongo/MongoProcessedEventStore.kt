package io.damoreira.kraken.messaging.mongo

import com.mongodb.ErrorCategory
import com.mongodb.MongoWriteException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.Date

/** The `processed_events` collection: `{_id, type, processedAt}`, and `processor`, `topic`, `partition`, `offset` on a stage-1 marker. */
internal class MongoProcessedEventStore(private val mongo: MongoTemplate) : ProcessedEventStore {
    private val collection get() = mongo.joined(COLLECTION)

    override suspend fun record(marker: ProcessedMarker): Boolean {
        val doc = Document("_id", marker.id).append("type", marker.type).append("processedAt", Date.from(marker.processedAt))
        marker.processor?.let { doc["processor"] = it }
        marker.topic?.let { doc["topic"] = it }
        marker.partition?.let { doc["partition"] = it }
        marker.offset?.let { doc["offset"] = it }
        return try {
            collection.insertOne(doc)
            true
        } catch (e: MongoWriteException) {
            if (e.error.category == ErrorCategory.DUPLICATE_KEY) false else throw e
        }
    }

    override suspend fun exists(id: String): Boolean =
        collection.find(Filters.eq("_id", id)).projection(Projections.include("_id")).first() != null

    override suspend fun purge(before: Instant): Long = collection.deleteMany(Filters.lt("processedAt", Date.from(before))).deletedCount

    companion object {
        const val COLLECTION = "processed_events"
    }
}
