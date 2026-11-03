package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext

/**
 * The second stage of local event dispatch: the saga runtime. [interested] says whether any saga type reacts to events
 * of [type] (so a record no ordinary handler wants is still processed); [process] hands the event to every interested
 * saga type, each in its own transaction. By contract [process] never throws for a failing saga handler: such an event
 * is parked in `saga_failures`, so the consumer moves on (S1 plan, R4).
 */
interface SagaStage {
    fun interested(type: String): Boolean
    fun process(type: String, payload: ByteArray, ctx: MessageContext)

    companion object {
        /** No sagas: not interested in anything. */
        val NONE: SagaStage = object : SagaStage {
            override fun interested(type: String) = false
            override fun process(type: String, payload: ByteArray, ctx: MessageContext) = Unit
            override fun toString() = "SagaStage.NONE"
        }
    }
}
