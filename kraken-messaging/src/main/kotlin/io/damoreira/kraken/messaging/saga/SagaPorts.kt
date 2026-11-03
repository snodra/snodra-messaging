package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.kafka.ProcessedEventDocument
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import java.time.Instant

/*
 * The persistence the saga runtime needs, as small ports: the Mongo implementations below are the defaults, and
 * `SagaTestFixture` supplies in-memory ones. All of them are called inside the runtime's transaction (when the
 * implementation has one), so the runtime decides what commits together.
 */

/** The `sagas` collection. */
interface SagaRepository {
    fun findById(id: String): SagaDocument?

    /** The live (not ended) instances of [type] associated with `[key] = [value]`. */
    fun findLive(type: String, key: String, value: String): List<SagaDocument>

    /** Inserts a new instance; throws [DuplicateKeyException] when its `startKey` is held by a live instance. */
    fun insert(saga: SagaDocument)

    /** Replaces the stored instance by [saga] when the stored `version` is [expectedVersion]; false when it is not. */
    fun update(saga: SagaDocument, expectedVersion: Long): Boolean
}

/** The saga-side outbox `saga_dispatch`: where the runtime records what a saga sends (the relay reads it, Task 6). */
interface SagaDispatchRecorder {
    fun record(entries: List<SagaDispatchDocument>)
}

/** The `deadlines` collection, as far as the runtime writes it. */
interface DeadlineStore {
    fun insert(deadline: DeadlineDocument)

    /** The deadline [id] when it is still SCHEDULED (not fired, not cancelled), else null. */
    fun findScheduled(id: String): DeadlineDocument?

    /** Cancels the SCHEDULED deadline [deadlineId] of [sagaId]; no effect on any other. */
    fun cancel(sagaId: String, deadlineId: String, now: Instant)

    /** Cancels the SCHEDULED deadlines of [sagaId] named [name]. */
    fun cancelByName(sagaId: String, name: String, now: Instant)

    /** Cancels every SCHEDULED deadline of [sagaId]. */
    fun cancelAll(sagaId: String, now: Instant)

    /** Sets the SCHEDULED deadline [id] to FIRED; false when it was not SCHEDULED. */
    fun markFired(id: String, now: Instant): Boolean
}

/** The `saga_failures` collection: parked events. */
interface SagaFailureStore {
    fun find(id: String): SagaFailureDocument?

    /** Inserts or replaces the failure with that id. */
    fun save(failure: SagaFailureDocument)

    /** Deletes the failure; false when there was none. */
    fun delete(id: String): Boolean

    /**
     * Records that a retry of the failure [id] failed again: `attempts` grows by [attempts], `error` and `lastFailedAt` are
     * replaced. **Never inserts**: false when the failure no longer exists (a `discard` or a concurrent retry got there first),
     * so the bookkeeping of a failed retry cannot bring a discarded failure back.
     */
    fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean

    /** The failures of [sagaType] (every type when null), oldest `lastFailedAt` first. */
    fun list(sagaType: String? = null): List<SagaFailureDocument>
}

/** The exactly-once markers of the saga stage: `processed_events` ids `saga:<sagaType>:<eventId>`. */
interface SagaMarkerStore {
    fun exists(id: String): Boolean

    /** Inserts the marker; throws [DuplicateKeyException] when it exists. */
    fun insert(id: String, type: String, now: Instant)
}

/** The five ports together. */
class SagaPorts(
    val sagas: SagaRepository,
    val dispatch: SagaDispatchRecorder,
    val deadlines: DeadlineStore,
    val failures: SagaFailureStore,
    val markers: SagaMarkerStore,
) {
    companion object {
        /** The MongoDB implementations of all five. */
        fun mongo(mongo: MongoTemplate) = SagaPorts(
            MongoSagaRepository(mongo), MongoSagaDispatchRecorder(mongo), MongoDeadlineStore(mongo),
            MongoSagaFailureStore(mongo), MongoSagaMarkerStore(mongo),
        )
    }
}

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
