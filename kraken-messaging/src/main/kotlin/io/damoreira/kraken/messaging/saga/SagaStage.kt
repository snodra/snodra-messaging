package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext

/**
 * The second stage of local event dispatch: the saga runtime. [interested] says whether any saga type reacts to events
 * of [type] (so a record no ordinary handler wants is still processed); [process] hands the event to every interested
 * saga type, each in its own transaction. By contract [process] never throws for a failing saga handler: such an event
 * is parked in `saga_failures`, so the consumer moves on (S1 plan, R4). [process] returns whether the event reached at
 * least one saga instance (an event a saga type already applied counts, and so does one it parked: H1): false when no
 * instance matched, which the relay needs to know for a `SagaCommandFailed` nobody received.
 */
interface SagaStage {
    fun interested(type: String): Boolean
    fun process(type: String, payload: ByteArray, ctx: MessageContext): Boolean

    /** The event types some saga type reacts to (listed in the `default` processor's status, ADR 0022). */
    fun eventTypes(): Set<String> = emptySet()

    companion object {
        /** No sagas: not interested in anything. */
        val NONE: SagaStage = object : SagaStage {
            override fun interested(type: String) = false
            override fun process(type: String, payload: ByteArray, ctx: MessageContext) = false
            override fun toString() = "SagaStage.NONE"
        }
    }
}
