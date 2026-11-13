package io.damoreira.kraken.messaging.store

import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import org.springframework.dao.DuplicateKeyException
import java.time.Instant

/*
 * The persistence the saga runtime, the dispatch relay, the deadline scheduler and the saga gauges need, as small ports
 * (ADR 0021, ADR 0028): a message store add-on implements them ([SagaStores], [MessageStore.sagas]), and `SagaTestFixture`
 * supplies in-memory ones. What the runtime calls runs inside its transaction (when the implementation has one), so the
 * runtime decides what commits together; what the relay, the scheduler and the gauges call outside one commits on its own.
 * Times are the caller's (the application clock), passed in.
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

    /** How many live (not ended) instances each saga type has; a type without one is absent (the `kraken.sagas.active` gauge). */
    fun countLiveByType(): Map<String, Long>
}

/**
 * The saga-side outbox `saga_dispatch` and the dispatch leases `saga_dispatch_leases`: the runtime [record]s what a saga
 * sends, the dispatch relay sends it after the commit, in `seq` order per saga, one relay instance per saga at a time.
 *
 * An entry is *pending* while it is neither dispatched (`dispatchedAt`) nor given up on (`failedAt`). A failed command carries
 * a pending failure notice (`noticePending`) until the relay delivered its `SagaCommandFailed` locally.
 */
interface SagaDispatchStore {
    /**
     * Inserts [entries] (none: nothing). An EVENT entry whose payload the outbox would refuse (not a JSON object whose integers
     * fit in a signed 64-bit integer, see [OutboxEntry]) is refused here with an [IllegalArgumentException] and nothing is inserted, so the saga's transaction rolls
     * back (its attempt fails and is retried, then parked) instead of committing an entry the relay could never publish.
     */
    fun record(entries: List<SagaDispatchDocument>)

    /**
     * The ids of the sagas whose first pending entry (lowest `seq`) is due: its `nextAttemptAt` is absent or not after [now].
     * Oldest work first (by that entry's `createdAt`, then saga id), at most [limit], leaving out the sagas in [exclude].
     */
    fun dueSagas(now: Instant, exclude: Collection<String>, limit: Int): List<String>

    /** The first pending entry of [sagaId] (lowest `seq`), due or not, or null. */
    fun nextPending(sagaId: String): SagaDispatchDocument?

    /** The entry [id] was sent: `dispatchedAt` [now]; it is never sent again. */
    fun markDispatched(id: String, now: Instant)

    /** A transient failure of the entry [id]: `attempts` becomes [attempts] and it is not due before [nextAttemptAt]. */
    fun scheduleRetry(id: String, attempts: Int, nextAttemptAt: Instant)

    /**
     * The command [id] is given up on with [reason] and [message]: `attempts` [attempts], `failedAt` [now], and a pending failure
     * notice (`noticeAttempts` 0) reserved until [noticeNextAt] for the caller, which delivers it next. Never sent again.
     */
    fun markFailed(id: String, attempts: Int, reason: SagaCommandFailed.Reason, message: String, now: Instant, noticeNextAt: Instant)

    /**
     * Up to [limit] entries with a pending notice that is due (`noticeNextAt` absent or not after [now]), earliest first, each
     * reserved for the caller by moving its `noticeNextAt` to [reserveUntil]: a concurrent claim never returns it before then.
     * The entries are returned as updated.
     */
    fun claimDueNotices(now: Instant, reserveUntil: Instant, limit: Int): List<SagaDispatchDocument>

    /** Delivering the notice of [id] failed: `noticeAttempts` becomes [noticeAttempts], the next try not before [noticeNextAt]. */
    fun scheduleNoticeRetry(id: String, noticeAttempts: Int, noticeNextAt: Instant)

    /** The notice of [id] is settled: no pending notice, no notice attempts, no `noticeNextAt`. */
    fun clearNotice(id: String)

    /**
     * Takes the dispatch lease of [sagaId] for [owner] until [until], when no lease exists or its `until` is before [now].
     * False while another (or this) owner's lease holds. Atomic: two claims never both succeed while a lease holds.
     */
    fun claimLease(sagaId: String, owner: String, until: Instant, now: Instant): Boolean

    /** Moves [owner]'s lease of [sagaId] to [until]; false when [owner] does not hold it (any more). */
    fun renewLease(sagaId: String, owner: String, until: Instant): Boolean

    /** Drops [owner]'s lease of [sagaId]; no effect on another owner's. */
    fun releaseLease(sagaId: String, owner: String)

    /** How many entries are pending (the `kraken.saga.dispatch.pending` gauge). */
    fun pending(): Long
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

    /**
     * Claims the SCHEDULED deadline due first (`dueAt` not after [now]) that nobody holds (no `claimedUntil`, or one before
     * [now]): it is [owner]'s until [leaseUntil] (`claimedBy`, `claimedUntil`), and returned as updated; null when none is due.
     * Atomic: two claims never return the same deadline while its lease holds.
     */
    fun claimDue(owner: String, now: Instant, leaseUntil: Instant): DeadlineDocument?

    /** How many SCHEDULED deadlines are due (`dueAt` not after [now]), per name; a name without one is absent. */
    fun countOverdueByName(now: Instant): Map<String, Long>
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
class SagaStores(
    val sagas: SagaRepository,
    val dispatch: SagaDispatchStore,
    val deadlines: DeadlineStore,
    val failures: SagaFailureStore,
    val markers: SagaMarkerStore,
) {
}
