package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.kafka.ProcessedEventDocument
import io.damoreira.kraken.messaging.store.DeadlineStore
import io.damoreira.kraken.messaging.store.SagaDispatchRecorder
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaMarkerStore
import io.damoreira.kraken.messaging.store.SagaRepository
import io.damoreira.kraken.messaging.store.SagaStores
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import java.time.Instant

/** The MongoDB implementations of the saga ports ([SagaStores]); each joins the thread's Mongo transaction. */
fun SagaStores.Companion.mongo(mongo: MongoTemplate) = SagaStores(
    MongoSagaRepository(mongo), MongoSagaDispatchRecorder(mongo), MongoDeadlineStore(mongo),
    MongoSagaFailureStore(mongo), MongoSagaMarkerStore(mongo),
)

class MongoSagaRepository(private val mongo: MongoTemplate) : SagaRepository {
    override fun findById(id: String): SagaDocument? = mongo.findById(id, SagaDocument::class.java)

    override fun findLive(type: String, key: String, value: String): List<SagaDocument> = mongo.find(
        Query(
            Criteria.where("type").`is`(type).and("ended").`is`(false)
                .and("associations").elemMatch(Criteria.where("k").`is`(key).and("v").`is`(value)),
        ),
        SagaDocument::class.java,
    )

    override fun insert(saga: SagaDocument) {
        mongo.insert(saga)
    }

    override fun update(saga: SagaDocument, expectedVersion: Long): Boolean =
        mongo.replace(Query(Criteria.where("_id").`is`(saga.id).and("version").`is`(expectedVersion)), saga).matchedCount == 1L
}

class MongoSagaDispatchRecorder(private val mongo: MongoTemplate) : SagaDispatchRecorder {
    override fun record(entries: List<SagaDispatchDocument>) {
        if (entries.isNotEmpty()) mongo.insertAll(entries)
    }
}

class MongoDeadlineStore(private val mongo: MongoTemplate) : DeadlineStore {
    override fun insert(deadline: DeadlineDocument) {
        mongo.insert(deadline)
    }

    override fun findScheduled(id: String): DeadlineDocument? =
        mongo.findOne(Query(Criteria.where("_id").`is`(id).and("status").`is`(DeadlineStatus.SCHEDULED)), DeadlineDocument::class.java)

    override fun cancel(sagaId: String, deadlineId: String, now: Instant) = cancelWhere(Criteria.where("_id").`is`(deadlineId).and("sagaId").`is`(sagaId), now)

    override fun cancelByName(sagaId: String, name: String, now: Instant) = cancelWhere(Criteria.where("sagaId").`is`(sagaId).and("name").`is`(name), now)

    override fun cancelAll(sagaId: String, now: Instant) = cancelWhere(Criteria.where("sagaId").`is`(sagaId), now)

    private fun cancelWhere(criteria: Criteria, now: Instant) {
        mongo.updateMulti(
            Query(criteria.and("status").`is`(DeadlineStatus.SCHEDULED)),
            Update().set("status", DeadlineStatus.CANCELLED).set("finishedAt", now),
            DeadlineDocument::class.java,
        )
    }

    override fun markFired(id: String, now: Instant): Boolean = mongo.updateFirst(
        Query(Criteria.where("_id").`is`(id).and("status").`is`(DeadlineStatus.SCHEDULED)),
        Update().set("status", DeadlineStatus.FIRED).set("finishedAt", now),
        DeadlineDocument::class.java,
    ).modifiedCount == 1L
}

class MongoSagaFailureStore(private val mongo: MongoTemplate) : SagaFailureStore {
    override fun find(id: String): SagaFailureDocument? = mongo.findById(id, SagaFailureDocument::class.java)

    override fun save(failure: SagaFailureDocument) {
        mongo.save(failure)
    }

    override fun delete(id: String): Boolean =
        mongo.remove(Query(Criteria.where("_id").`is`(id)), SagaFailureDocument::class.java).deletedCount == 1L

    override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean = mongo.updateFirst(
        Query(Criteria.where("_id").`is`(id)),
        Update().inc("attempts", attempts).set("error", error).set("lastFailedAt", now),
        SagaFailureDocument::class.java,
    ).matchedCount == 1L

    override fun list(sagaType: String?): List<SagaFailureDocument> = mongo.find(
        Query(sagaType?.let { Criteria.where("sagaType").`is`(it) } ?: Criteria()).with(Sort.by("lastFailedAt")),
        SagaFailureDocument::class.java,
    )
}

class MongoSagaMarkerStore(private val mongo: MongoTemplate) : SagaMarkerStore {
    override fun exists(id: String): Boolean = mongo.exists(Query(Criteria.where("_id").`is`(id)), ProcessedEventDocument::class.java)

    override fun insert(id: String, type: String, now: Instant) {
        mongo.insert(ProcessedEventDocument(id, type, now))
    }
}
