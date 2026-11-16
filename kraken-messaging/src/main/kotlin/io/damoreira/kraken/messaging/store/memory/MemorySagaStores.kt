package io.damoreira.kraken.messaging.store.memory

import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DeadlineStatus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.DeadlineStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import io.damoreira.kraken.messaging.store.SagaDispatchStore
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaMarkerStore
import io.damoreira.kraken.messaging.store.SagaRepository
import io.damoreira.kraken.messaging.store.SagaStores
import io.damoreira.kraken.messaging.store.checkOutboxPayload
import org.springframework.dao.DuplicateKeyException
import java.time.Instant

/*
 * The saga ports of the in-memory message store. Every one joins the calling thread's transaction when there is one (as the
 * MongoDB add-on's do), so the saga runtime decides what commits together; outside one each call commits at once. Each call is
 * atomic (the store's lock).
 */

internal fun memorySagaStores(store: MemoryMessageStore) = SagaStores(
    MemorySagaRepository(store), MemorySagaDispatchStore(store), MemoryDeadlineStore(store), MemorySagaFailureStore(store), MemorySagaMarkerStore(store),
)

/** A dispatch lease of one saga (`saga_dispatch_leases`). */
internal data class DispatchLease(val owner: String, val until: Instant)

internal class MemorySagaRepository(private val store: MemoryMessageStore) : SagaRepository {
    private fun view() = store.view(store.sagaTable, joined = true)

    override fun findById(id: String): SagaDocument? = store.locked { view().get(id) }

    override fun findLive(type: String, key: String, value: String): List<SagaDocument> = store.locked {
        view().values().filter { s -> s.type == type && !s.ended && s.associations.any { it.k == key && it.v == value } }.sortedBy { it.id }
    }

    override fun insert(saga: SagaDocument) = store.locked {
        val view = view()
        if (view.contains(saga.id)) throw DuplicateKeyException("sagas: a saga ${saga.id} exists")
        requireFreeStartKey(view, saga)
        view.put(saga.id, saga)
    }

    override fun update(saga: SagaDocument, expectedVersion: Long): Boolean = store.locked {
        val view = view()
        if (view.get(saga.id)?.version != expectedVersion) return@locked false
        requireFreeStartKey(view, saga)
        view.put(saga.id, saga)
        true
    }

    override fun countLiveByType(): Map<String, Long> = store.locked {
        view().values().filter { !it.ended }.groupingBy { it.type }.eachCount().mapValues { it.value.toLong() }
    }

    /** A live saga's start key is held by no other live saga. */
    private fun requireFreeStartKey(view: View<SagaDocument>, saga: SagaDocument) {
        val key = saga.startKey ?: return
        if (saga.ended) return
        if (view.values().any { it.id != saga.id && !it.ended && it.startKey == key }) {
            throw DuplicateKeyException("sagas: the start key $key is held by a live saga")
        }
    }
}

internal class MemorySagaDispatchStore(private val store: MemoryMessageStore) : SagaDispatchStore {
    private fun view() = store.view(store.dispatchTable, joined = true)

    private fun leases() = store.view(store.leaseTable, joined = true)

    private fun pending(e: SagaDispatchDocument) = e.dispatchedAt == null && e.failedAt == null

    override fun record(entries: List<SagaDispatchDocument>) {
        if (entries.isEmpty()) return
        // An event the outbox would refuse is refused here, inside the saga's transaction: never committed, then stuck.
        for (e in entries) if (e.kind == DispatchKind.EVENT) checkOutboxPayload(e.payload.toByteArray(Charsets.UTF_8), "saga event ${e.id} (${e.type})")
        store.locked {
            val view = view()
            val ids = HashSet<String>()
            for (e in entries) if (!ids.add(e.id) || view.contains(e.id)) throw DuplicateKeyException("saga_dispatch: an entry ${e.id} exists")
            for (e in entries) view.put(e.id, e)
        }
    }

    override fun dueSagas(now: Instant, exclude: Collection<String>, limit: Int): List<String> = store.locked {
        val excluded = exclude.toSet()
        view().values().filter { pending(it) && it.sagaId !in excluded }
            .groupBy { it.sagaId }.values.map { entries -> entries.minBy { it.seq } }
            .filter { it.nextAttemptAt == null || !it.nextAttemptAt.isAfter(now) }
            .sortedWith(compareBy({ it.createdAt }, { it.sagaId }))
            .take(limit).map { it.sagaId }
    }

    override fun nextPending(sagaId: String): SagaDispatchDocument? = store.locked {
        view().values().filter { it.sagaId == sagaId && pending(it) }.minByOrNull { it.seq }
    }

    override fun markDispatched(id: String, now: Instant) = update(id) { it.copy(dispatchedAt = now) }

    override fun scheduleRetry(id: String, attempts: Int, nextAttemptAt: Instant) = update(id) { it.copy(attempts = attempts, nextAttemptAt = nextAttemptAt) }

    override fun markFailed(id: String, attempts: Int, reason: SagaCommandFailed.Reason, message: String, now: Instant, noticeNextAt: Instant) =
        update(id) {
            it.copy(
                attempts = attempts, failedAt = now, failureReason = reason, failureMessage = message,
                noticePending = true, noticeAttempts = 0, noticeNextAt = noticeNextAt,
            )
        }

    override fun claimDueNotices(now: Instant, reserveUntil: Instant, limit: Int): List<SagaDispatchDocument> = store.locked {
        val view = view()
        view.values().filter { it.noticePending == true && (it.noticeNextAt == null || !it.noticeNextAt.isAfter(now)) }
            .sortedWith(compareBy(nullsFirst()) { it.noticeNextAt }).take(limit)
            .map { e -> e.copy(noticeNextAt = reserveUntil).also { view.put(e.id, it) } }
    }

    override fun scheduleNoticeRetry(id: String, noticeAttempts: Int, noticeNextAt: Instant) =
        update(id) { it.copy(noticeAttempts = noticeAttempts, noticeNextAt = noticeNextAt) }

    override fun clearNotice(id: String) = update(id) { it.copy(noticePending = null, noticeAttempts = null, noticeNextAt = null) }

    override fun claimLease(sagaId: String, owner: String, until: Instant, now: Instant): Boolean = store.locked {
        val leases = leases()
        val current = leases.get(sagaId)
        if (current != null && !current.until.isBefore(now)) return@locked false
        leases.put(sagaId, DispatchLease(owner, until))
        true
    }

    override fun renewLease(sagaId: String, owner: String, until: Instant): Boolean = store.locked {
        val leases = leases()
        if (leases.get(sagaId)?.owner != owner) return@locked false
        leases.put(sagaId, DispatchLease(owner, until))
        true
    }

    override fun releaseLease(sagaId: String, owner: String) = store.locked {
        val leases = leases()
        if (leases.get(sagaId)?.owner == owner) leases.delete(sagaId)
        Unit
    }

    override fun pending(): Long = store.locked { view().values().count(::pending).toLong() }

    private fun update(id: String, change: (SagaDispatchDocument) -> SagaDispatchDocument) = store.locked {
        view().update(id, change)
        Unit
    }
}

internal class MemoryDeadlineStore(private val store: MemoryMessageStore) : DeadlineStore {
    private fun view() = store.view(store.deadlineTable, joined = true)

    private fun scheduled(d: DeadlineDocument) = d.status == DeadlineStatus.SCHEDULED

    override fun insert(deadline: DeadlineDocument) = store.locked {
        val view = view()
        if (view.contains(deadline.id)) throw DuplicateKeyException("deadlines: a deadline ${deadline.id} exists")
        view.put(deadline.id, deadline)
    }

    override fun findScheduled(id: String): DeadlineDocument? = store.locked { view().get(id)?.takeIf(::scheduled) }

    override fun cancel(sagaId: String, deadlineId: String, now: Instant) = cancelWhere(now) { it.id == deadlineId && it.sagaId == sagaId }

    override fun cancelByName(sagaId: String, name: String, now: Instant) = cancelWhere(now) { it.sagaId == sagaId && it.name == name }

    override fun cancelAll(sagaId: String, now: Instant) = cancelWhere(now) { it.sagaId == sagaId }

    private fun cancelWhere(now: Instant, matches: (DeadlineDocument) -> Boolean) = store.locked {
        val view = view()
        for (d in view.values().filter { scheduled(it) && matches(it) }) view.put(d.id, d.copy(status = DeadlineStatus.CANCELLED, finishedAt = now))
    }

    override fun markFired(id: String, now: Instant): Boolean = store.locked {
        val view = view()
        val d = view.get(id)?.takeIf(::scheduled) ?: return@locked false
        view.put(id, d.copy(status = DeadlineStatus.FIRED, finishedAt = now))
        true
    }

    override fun claimDue(owner: String, now: Instant, leaseUntil: Instant): DeadlineDocument? = store.locked {
        val view = view()
        val due = view.values().filter { scheduled(it) && !it.dueAt.isAfter(now) && (it.claimedUntil == null || it.claimedUntil.isBefore(now)) }
            .minWithOrNull(compareBy({ it.dueAt }, { it.id })) ?: return@locked null
        due.copy(claimedBy = owner, claimedUntil = leaseUntil).also { view.put(due.id, it) }
    }

    override fun countOverdueByName(now: Instant): Map<String, Long> = store.locked {
        view().values().filter { scheduled(it) && !it.dueAt.isAfter(now) }.groupingBy { it.name }.eachCount().mapValues { it.value.toLong() }
    }
}

internal class MemorySagaFailureStore(private val store: MemoryMessageStore) : SagaFailureStore {
    private fun view() = store.view(store.failureTable, joined = true)

    override fun find(id: String): SagaFailureDocument? = store.locked { view().get(id) }

    override fun save(failure: SagaFailureDocument) = store.locked { view().put(failure.id, failure) }

    override fun delete(id: String): Boolean = store.locked { view().delete(id) }

    override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean = store.locked {
        view().update(id) { it.copy(attempts = it.attempts + attempts, error = error, lastFailedAt = now) } != null
    }

    override fun list(sagaType: String?): List<SagaFailureDocument> = store.locked {
        view().values().filter { sagaType == null || it.sagaType == sagaType }.sortedWith(compareBy({ it.lastFailedAt }, { it.id }))
    }
}

/** The saga stage's markers: `processed_events` rows, as every other marker. */
internal class MemorySagaMarkerStore(private val store: MemoryMessageStore) : SagaMarkerStore {
    private fun view() = store.view(store.markerTable, joined = true)

    override fun exists(id: String): Boolean = store.locked { view().contains(id) }

    override fun insert(id: String, type: String, now: Instant) = store.locked {
        val view = view()
        if (view.contains(id)) throw DuplicateKeyException("processed_events: a marker $id exists")
        view.put(id, ProcessedMarker(id, type, now))
    }
}
