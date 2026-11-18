package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType

/**
 * What the dispatch relay tells a saga when a command it recorded could not be delivered or failed: a saga does not
 * wait for replies, so a failure comes back as this event, dispatched locally (never published to `kraken.events`) with
 * the dedupe key `failed:<dispatchId>`. A saga handles it with `@SagaEventHandler(associationProperty = "sagaId")` after
 * associating itself with its own id (`saga.associateWith("sagaId", saga.sagaId)`); unhandled, it is logged at error and
 * counted. [idempotencyKey] is the failed command's `<sagaId>:<seq>`, [commandType] its wire type name.
 */
@MessageType("snodra.SagaCommandFailed")
data class SagaCommandFailed(
    val sagaId: String,
    val commandType: String,
    val idempotencyKey: String,
    val reason: Reason,
    val message: String,
) : Event {
    /** Why the command failed (the spec's retry taxonomy): the permanent kinds at once, [EXHAUSTED] after the transient retries. */
    enum class Reason {
        /** Nobody handles the command type. */
        NO_HANDLER,

        /** The handler requires an authenticated caller and a saga command has none (or the expression denied it). */
        DENIED,

        /** The handler could not read the payload. */
        INVALID,

        /** The handler failed (any other error). */
        FAILED,

        /** An idempotent adapter crashed mid-call and cannot tell whether the call happened (`@IdempotentHandler`). */
        UNKNOWN_OUTCOME,

        /** A transient failure persisted through all the relay's attempts. */
        EXHAUSTED,
    }
}
