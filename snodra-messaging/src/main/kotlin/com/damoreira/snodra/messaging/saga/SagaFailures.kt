package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.store.SagaFailureStore

/**
 * Admin API over the parked saga events (`saga_failures`): an event whose saga handler failed all its attempts waits here
 * until the cause is fixed, then is retried (re-delivered to the saga type that failed, applied once) or discarded.
 * A retry runs the type's handlers again with the runtime's retries; it ignores the type's marker (which parking left
 * by design) and deletes the failure in the same transaction that applies the event, so concurrent retries apply it
 * once. A failure that fails again stays, with `attempts` increased and the new error. A discarded event keeps its
 * marker: redelivering the record does not apply it either. A retry of a saga type this service does not run is refused
 * with an [IllegalStateException] and writes nothing (H1): the parked event keeps its error.
 */
class SagaFailures(private val store: SagaFailureStore, private val runtime: SagaRuntime) {
    /** The parked events of saga type [type] (every type when null), the longest waiting first. */
    fun list(type: String? = null): List<SagaFailureDocument> = store.list(type)

    /**
     * Re-delivers the parked event [id] (`<sagaType>:<eventId>`); true when it was applied, false when unknown or failed again.
     *
     * @throws IllegalStateException when its saga type does not run in this service (the event is left as it was)
     */
    fun retry(id: String): Boolean = store.find(id)?.let(runtime::reapply) ?: false

    /**
     * Retries every parked event of saga type [type]; returns how many were applied.
     *
     * @throws IllegalStateException when [type] does not run in this service and has parked events (none is touched)
     */
    fun retryAll(type: String): Int {
        val parked = store.list(type)
        if (parked.isNotEmpty()) runtime.checkRuns(type) { "its parked events (${parked.size}) are left as they were" }
        return parked.count(runtime::reapply)
    }

    /** Drops the parked event [id] without applying it; false when unknown. */
    fun discard(id: String): Boolean = store.delete(id)
}
