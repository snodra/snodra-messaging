package io.damoreira.kraken.messaging.mongo

import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DeadlineStatus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.DeadlineStore
import io.damoreira.kraken.messaging.store.SagaDispatchStore
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaMarkerStore
import io.damoreira.kraken.messaging.store.SagaRepository
import io.damoreira.kraken.messaging.store.SagaStores
import org.bson.Document
import org.bson.conversions.Bson
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.Date

/*
 * The MongoDB implementations of the saga ports ([SagaStores]) over `sagas`, `saga_dispatch`, `saga_dispatch_leases`,
 * `deadlines`, `saga_failures` and `processed_events`. Every one goes through the template's session-aware database, so it
 * joins the thread's Mongo transaction when there is one. The documents are written field by field with the names the
 * library has always used (`_id` for `id`); an absent value is an absent field, never null (the partial unique index on a
 * live `startKey` relies on it). Driver exceptions are translated as the template does: a duplicate key is Spring's
 * `DuplicateKeyException`, a write conflict in a transaction a `TransientDataAccessException`.
 */

internal fun mongoSagaStores(mongo: MongoTemplate) = SagaStores(
    MongoSagaRepository(mongo), MongoSagaDispatchStore(mongo), MongoDeadlineStore(mongo),
    MongoSagaFailureStore(mongo), MongoSagaMarkerStore(mongo),
)

internal class MongoSagaRepository(private val mongo: MongoTemplate) : SagaRepository {
    private val collection get() = mongo.joined(COLLECTION)

    override fun findById(id: String): SagaDocument? = translated { collection.find(Filters.eq("_id", id)).first()?.let(::readSaga) }

    override fun findLive(type: String, key: String, value: String): List<SagaDocument> = translated {
        collection.find(
            Filters.and(
                Filters.eq("type", type), Filters.eq("ended", false),
                Filters.elemMatch("associations", Filters.and(Filters.eq("k", key), Filters.eq("v", value))),
            ),
        ).map(::readSaga).toList()
    }

    override fun insert(saga: SagaDocument) {
        translated { collection.insertOne(sagaDocument(saga)) }
    }

    override fun update(saga: SagaDocument, expectedVersion: Long): Boolean = translated {
        collection.replaceOne(Filters.and(Filters.eq("_id", saga.id), Filters.eq("version", expectedVersion)), sagaDocument(saga)).matchedCount == 1L
    }

    override fun countLiveByType(): Map<String, Long> = translated {
        collection.aggregate(listOf(Aggregates.match(Filters.eq("ended", false)), Aggregates.group("\$type", Accumulators.sum("n", 1))))
            .associate { it.getString("_id") to (it["n"] as Number).toLong() }
    }

    companion object {
        const val COLLECTION = "sagas"
    }
}

internal class MongoSagaDispatchStore(private val mongo: MongoTemplate) : SagaDispatchStore {
    private val collection get() = mongo.joined(COLLECTION)
    private val leases get() = mongo.joined(LEASES)

    override fun record(entries: List<SagaDispatchDocument>) {
        if (entries.isNotEmpty()) translated { collection.insertMany(entries.map(::dispatchDocument)) }
    }

    override fun dueSagas(now: Instant, exclude: Collection<String>, limit: Int): List<String> = translated {
        collection.aggregate(
            listOf(
                Aggregates.match(Filters.and(pending(), Filters.nin("sagaId", exclude))),
                Aggregates.sort(Sorts.ascending("sagaId", "seq")),
                Aggregates.group("\$sagaId", Accumulators.first("next", "\$nextAttemptAt"), Accumulators.first("since", "\$createdAt")),
                Aggregates.match(Filters.or(Filters.eq("next", null), Filters.lte("next", Date.from(now)))),
                Aggregates.sort(Sorts.ascending("since", "_id")), // oldest work first: each saga's first pending entry, by when it was recorded
                Aggregates.limit(limit),
            ),
        ).allowDiskUse(true).map { it.getString("_id") }.toList()
    }

    override fun nextPending(sagaId: String): SagaDispatchDocument? = translated {
        collection.find(Filters.and(Filters.eq("sagaId", sagaId), pending())).sort(Sorts.ascending("seq")).limit(1).first()?.let(::readDispatch)
    }

    override fun markDispatched(id: String, now: Instant) {
        update(id, Updates.set("dispatchedAt", Date.from(now)))
    }

    override fun scheduleRetry(id: String, attempts: Int, nextAttemptAt: Instant) {
        update(id, Updates.combine(Updates.set("attempts", attempts), Updates.set("nextAttemptAt", Date.from(nextAttemptAt))))
    }

    override fun markFailed(id: String, attempts: Int, reason: SagaCommandFailed.Reason, message: String, now: Instant, noticeNextAt: Instant) {
        update(
            id,
            Updates.combine(
                Updates.set("attempts", attempts), Updates.set("failedAt", Date.from(now)), Updates.set("failureReason", reason.name),
                Updates.set("failureMessage", message), Updates.set("noticePending", true), Updates.set("noticeAttempts", 0),
                Updates.set("noticeNextAt", Date.from(noticeNextAt)),
            ),
        )
    }

    override fun claimDueNotices(now: Instant, reserveUntil: Instant, limit: Int): List<SagaDispatchDocument> = translated {
        val claimed = ArrayList<SagaDispatchDocument>()
        while (claimed.size < limit) {
            claimed += collection.findOneAndUpdate(
                Filters.and(Filters.eq("noticePending", true), Filters.or(Filters.eq("noticeNextAt", null), Filters.lte("noticeNextAt", Date.from(now)))),
                Updates.set("noticeNextAt", Date.from(reserveUntil)),
                FindOneAndUpdateOptions().sort(Sorts.ascending("noticeNextAt")).returnDocument(ReturnDocument.AFTER),
            )?.let(::readDispatch) ?: break
        }
        claimed
    }

    override fun scheduleNoticeRetry(id: String, noticeAttempts: Int, noticeNextAt: Instant) {
        update(id, Updates.combine(Updates.set("noticeAttempts", noticeAttempts), Updates.set("noticeNextAt", Date.from(noticeNextAt))))
    }

    override fun clearNotice(id: String) {
        update(id, Updates.combine(Updates.unset("noticePending"), Updates.unset("noticeAttempts"), Updates.unset("noticeNextAt")))
    }

    /** An upsert on `{_id, until < now}`: while a lease holds it collides with the existing `_id`, and the claim fails. */
    override fun claimLease(sagaId: String, owner: String, until: Instant, now: Instant): Boolean = try {
        translated {
            leases.updateOne(
                Filters.and(Filters.eq("_id", sagaId), Filters.lt("until", Date.from(now))),
                Updates.combine(Updates.set("owner", owner), Updates.set("until", Date.from(until))),
                UpdateOptions().upsert(true),
            )
        }
        true
    } catch (_: org.springframework.dao.DuplicateKeyException) {
        false
    }

    override fun renewLease(sagaId: String, owner: String, until: Instant): Boolean = translated {
        leases.updateOne(Filters.and(Filters.eq("_id", sagaId), Filters.eq("owner", owner)), Updates.set("until", Date.from(until))).matchedCount == 1L
    }

    override fun releaseLease(sagaId: String, owner: String) {
        translated { leases.deleteOne(Filters.and(Filters.eq("_id", sagaId), Filters.eq("owner", owner))) }
    }

    override fun pendingDispatches(): Long = translated { collection.countDocuments(pending()) }

    /** Not dispatched and not given up on (a missing field is null). */
    private fun pending(): Bson = Filters.and(Filters.eq("dispatchedAt", null), Filters.eq("failedAt", null))

    private fun update(id: String, update: Bson) {
        translated { collection.updateOne(Filters.eq("_id", id), update) }
    }

    companion object {
        const val COLLECTION = "saga_dispatch"
        const val LEASES = "saga_dispatch_leases"
    }
}

internal class MongoDeadlineStore(private val mongo: MongoTemplate) : DeadlineStore {
    private val collection get() = mongo.joined(COLLECTION)

    override fun insert(deadline: DeadlineDocument) {
        translated { collection.insertOne(deadlineDocument(deadline)) }
    }

    override fun findScheduled(id: String): DeadlineDocument? =
        translated { collection.find(Filters.and(Filters.eq("_id", id), scheduled())).first()?.let(::readDeadline) }

    override fun cancel(sagaId: String, deadlineId: String, now: Instant) = cancelWhere(Filters.and(Filters.eq("_id", deadlineId), Filters.eq("sagaId", sagaId)), now)

    override fun cancelByName(sagaId: String, name: String, now: Instant) = cancelWhere(Filters.and(Filters.eq("sagaId", sagaId), Filters.eq("name", name)), now)

    override fun cancelAll(sagaId: String, now: Instant) = cancelWhere(Filters.eq("sagaId", sagaId), now)

    private fun cancelWhere(filter: Bson, now: Instant) {
        translated { collection.updateMany(Filters.and(filter, scheduled()), finish(DeadlineStatus.CANCELLED, now)) }
    }

    override fun markFired(id: String, now: Instant): Boolean =
        translated { collection.updateOne(Filters.and(Filters.eq("_id", id), scheduled()), finish(DeadlineStatus.FIRED, now)).modifiedCount == 1L }

    override fun claimDueDeadline(owner: String, now: Instant, leaseUntil: Instant): DeadlineDocument? = translated {
        val at = Date.from(now)
        collection.findOneAndUpdate(
            Filters.and(scheduled(), Filters.lte("dueAt", at), Filters.or(Filters.eq("claimedUntil", null), Filters.lt("claimedUntil", at))),
            Updates.combine(Updates.set("claimedBy", owner), Updates.set("claimedUntil", Date.from(leaseUntil))),
            FindOneAndUpdateOptions().sort(Sorts.ascending("dueAt")).returnDocument(ReturnDocument.AFTER),
        )?.let(::readDeadline)
    }

    override fun overdueByName(now: Instant): Map<String, Long> = translated {
        collection.aggregate(
            listOf(
                Aggregates.match(Filters.and(scheduled(), Filters.lte("dueAt", Date.from(now)))),
                Aggregates.group("\$name", Accumulators.sum("n", 1)),
            ),
        ).associate { it.getString("_id") to (it["n"] as Number).toLong() }
    }

    private fun scheduled(): Bson = Filters.eq("status", DeadlineStatus.SCHEDULED.name)

    private fun finish(status: DeadlineStatus, now: Instant): Bson = Updates.combine(Updates.set("status", status.name), Updates.set("finishedAt", Date.from(now)))

    companion object {
        const val COLLECTION = "deadlines"
    }
}

internal class MongoSagaFailureStore(private val mongo: MongoTemplate) : SagaFailureStore {
    private val collection get() = mongo.joined(COLLECTION)

    override fun find(id: String): SagaFailureDocument? = translated { collection.find(Filters.eq("_id", id)).first()?.let(::readFailure) }

    override fun save(failure: SagaFailureDocument) {
        translated { collection.replaceOne(Filters.eq("_id", failure.id), failureDocument(failure), ReplaceOptions().upsert(true)) }
    }

    override fun delete(id: String): Boolean = translated { collection.deleteOne(Filters.eq("_id", id)).deletedCount == 1L }

    override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean = translated {
        collection.updateOne(
            Filters.eq("_id", id),
            Updates.combine(Updates.inc("attempts", attempts), Updates.set("error", error), Updates.set("lastFailedAt", Date.from(now))),
        ).matchedCount == 1L
    }

    override fun list(sagaType: String?): List<SagaFailureDocument> = translated {
        collection.find(sagaType?.let { Filters.eq("sagaType", it) } ?: Document()).sort(Sorts.ascending("lastFailedAt")).map(::readFailure).toList()
    }

    companion object {
        const val COLLECTION = "saga_failures"
    }
}

internal class MongoSagaMarkerStore(private val mongo: MongoTemplate) : SagaMarkerStore {
    private val collection get() = mongo.joined(MongoProcessedEventStore.COLLECTION)

    override fun exists(id: String): Boolean = translated { collection.find(Filters.eq("_id", id)).first() != null }

    override fun insert(id: String, type: String, now: Instant) {
        translated { collection.insertOne(Document("_id", id).append("type", type).append("processedAt", Date.from(now))) }
    }
}

// ---- the documents ---------------------------------------------------------------------------------------------------

/** Puts [value] under [name] unless it is null: an absent value is an absent field. */
private fun Document.optional(name: String, value: Any?): Document = apply { if (value != null) put(name, value) }

private fun Document.instant(name: String): Instant? = getDate(name)?.toInstant()

private fun Document.long(name: String): Long? = (get(name) as? Number)?.toLong()

private fun Document.int(name: String): Int? = (get(name) as? Number)?.toInt()

private fun Document.strings(name: String): Map<String, String> =
    (get(name, Document::class.java) ?: Document()).mapValues { it.value?.toString() ?: "" }

internal fun sagaDocument(s: SagaDocument): Document = Document("_id", s.id)
    .append("type", s.type)
    .append("state", s.state)
    .append("associations", s.associations.map { Document("k", it.k).append("v", it.v) })
    .optional("startKey", s.startKey)
    .append("ended", s.ended)
    .optional("endedAt", s.endedAt?.let(Date::from))
    .append("version", s.version)
    .append("createdAt", Date.from(s.createdAt))
    .append("updatedAt", Date.from(s.updatedAt))

internal fun readSaga(d: Document) = SagaDocument(
    id = d.getString("_id"),
    type = d.getString("type"),
    state = d.getString("state"),
    associations = d.getList("associations", Document::class.java).orEmpty().map { Association(it.getString("k"), it.getString("v")) },
    startKey = d.getString("startKey"),
    ended = d.getBoolean("ended") ?: false,
    endedAt = d.instant("endedAt"),
    version = d.long("version") ?: 0,
    createdAt = d.instant("createdAt")!!,
    updatedAt = d.instant("updatedAt")!!,
)

internal fun dispatchDocument(e: SagaDispatchDocument): Document = Document("_id", e.id)
    .append("sagaId", e.sagaId)
    .append("seq", e.seq)
    .append("kind", e.kind.name)
    .append("type", e.type)
    .append("payload", e.payload)
    .append("headers", Document(e.headers))
    .optional("routingKey", e.routingKey)
    .append("createdAt", Date.from(e.createdAt))
    .optional("dispatchedAt", e.dispatchedAt?.let(Date::from))
    .append("attempts", e.attempts)
    .optional("nextAttemptAt", e.nextAttemptAt?.let(Date::from))
    .optional("failedAt", e.failedAt?.let(Date::from))
    .optional("failureReason", e.failureReason?.name)
    .optional("failureMessage", e.failureMessage)
    .optional("noticePending", e.noticePending)
    .optional("noticeAttempts", e.noticeAttempts)
    .optional("noticeNextAt", e.noticeNextAt?.let(Date::from))

internal fun readDispatch(d: Document) = SagaDispatchDocument(
    id = d.getString("_id"),
    sagaId = d.getString("sagaId"),
    seq = d.long("seq")!!,
    kind = DispatchKind.valueOf(d.getString("kind")),
    type = d.getString("type"),
    payload = d.getString("payload"),
    headers = d.strings("headers"),
    routingKey = d.getString("routingKey"),
    createdAt = d.instant("createdAt")!!,
    dispatchedAt = d.instant("dispatchedAt"),
    attempts = d.int("attempts") ?: 0,
    nextAttemptAt = d.instant("nextAttemptAt"),
    failedAt = d.instant("failedAt"),
    failureReason = d.getString("failureReason")?.let(SagaCommandFailed.Reason::valueOf),
    failureMessage = d.getString("failureMessage"),
    noticePending = d.getBoolean("noticePending"),
    noticeAttempts = d.int("noticeAttempts"),
    noticeNextAt = d.instant("noticeNextAt"),
)

internal fun deadlineDocument(x: DeadlineDocument): Document = Document("_id", x.id)
    .append("sagaType", x.sagaType)
    .append("sagaId", x.sagaId)
    .append("name", x.name)
    .optional("payload", x.payload)
    .optional("payloadType", x.payloadType)
    .append("dueAt", Date.from(x.dueAt))
    .append("correlationId", x.correlationId)
    .append("status", x.status.name)
    .optional("claimedBy", x.claimedBy)
    .optional("claimedUntil", x.claimedUntil?.let(Date::from))
    .optional("finishedAt", x.finishedAt?.let(Date::from))

internal fun readDeadline(d: Document) = DeadlineDocument(
    id = d.getString("_id"),
    sagaType = d.getString("sagaType"),
    sagaId = d.getString("sagaId"),
    name = d.getString("name"),
    payload = d.getString("payload"),
    payloadType = d.getString("payloadType"),
    dueAt = d.instant("dueAt")!!,
    correlationId = d.getString("correlationId"),
    status = DeadlineStatus.valueOf(d.getString("status")),
    claimedBy = d.getString("claimedBy"),
    claimedUntil = d.instant("claimedUntil"),
    finishedAt = d.instant("finishedAt"),
)

internal fun failureDocument(f: SagaFailureDocument): Document = Document("_id", f.id)
    .append("sagaType", f.sagaType)
    .append("eventId", f.eventId)
    .append("eventType", f.eventType)
    .append("payload", f.payload)
    .append("headers", Document(f.headers))
    .append("error", f.error)
    .append("attempts", f.attempts)
    .append("firstFailedAt", Date.from(f.firstFailedAt))
    .append("lastFailedAt", Date.from(f.lastFailedAt))

internal fun readFailure(d: Document) = SagaFailureDocument(
    id = d.getString("_id"),
    sagaType = d.getString("sagaType"),
    eventId = d.getString("eventId"),
    eventType = d.getString("eventType"),
    payload = d.getString("payload"),
    headers = d.strings("headers"),
    error = d.getString("error"),
    attempts = d.int("attempts") ?: 0,
    firstFailedAt = d.instant("firstFailedAt")!!,
    lastFailedAt = d.instant("lastFailedAt")!!,
)
