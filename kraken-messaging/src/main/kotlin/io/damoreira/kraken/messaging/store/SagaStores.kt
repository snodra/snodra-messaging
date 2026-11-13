package io.damoreira.kraken.messaging.store

import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import org.springframework.dao.DuplicateKeyException
import java.time.Instant

/*
 * The persistence the saga runtime needs, as small ports (ADR 0021): a message store add-on implements them ([SagaStores],
 * [MessageStore.sagas]), and `SagaTestFixture` supplies in-memory ones. All of them are called inside the runtime's
 * transaction (when the implementation has one), so the runtime decides what commits together.
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
class SagaStores(
    val sagas: SagaRepository,
    val dispatch: SagaDispatchRecorder,
    val deadlines: DeadlineStore,
    val failures: SagaFailureStore,
    val markers: SagaMarkerStore,
) {
    /** For an add-on's factory (`SagaStores.mongo(...)`). */
    companion object
}
