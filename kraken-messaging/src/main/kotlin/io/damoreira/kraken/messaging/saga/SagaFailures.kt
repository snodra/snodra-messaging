package io.damoreira.kraken.messaging.saga

import org.springframework.data.mongodb.core.MongoTemplate

/**
 * Admin API over the parked saga events (`saga_failures`): an event whose saga handler failed all its attempts waits here
 * until the cause is fixed, then is retried (re-delivered to the saga type that failed, applied once) or discarded.
 * A retry runs the type's handlers again with the runtime's retries; it ignores the type's marker (which parking left
 * by design) and deletes the failure in the same transaction that applies the event, so concurrent retries apply it
 * once. A failure that fails again stays, with `attempts` increased and the new error. A discarded event keeps its
 * marker: redelivering the record does not apply it either.
 */
class SagaFailures(private val store: SagaFailureStore, private val runtime: SagaRuntime) {
    constructor(mongo: MongoTemplate, runtime: SagaRuntime) : this(MongoSagaFailureStore(mongo), runtime)

    /** The parked events of saga type [type] (every type when null), the longest waiting first. */
    fun list(type: String? = null): List<SagaFailureDocument> = store.list(type)

    /** Re-delivers the parked event [id] (`<sagaType>:<eventId>`); true when it was applied, false when unknown or failed again. */
    fun retry(id: String): Boolean = store.find(id)?.let(runtime::reapply) ?: false

    /** Retries every parked event of saga type [type]; returns how many were applied. */
    fun retryAll(type: String): Int = store.list(type).count(runtime::reapply)

    /** Drops the parked event [id] without applying it; false when unknown. */
    fun discard(id: String): Boolean = store.delete(id)
}
