package com.damoreira.snodra.messaging.saga

/** What a saga handler can do to its own instance; a handler parameter resolved per call by the saga runtime. */
interface SagaLifecycle {
    /** The id of this saga instance (`sagas._id`). */
    val sagaId: String

    /** Routes later events whose association `key = value` to this instance. */
    fun associateWith(key: String, value: String)

    /** Stops routing events with the association `key = value` to this instance. */
    fun removeAssociationWith(key: String, value: String)

    /** Ends the instance after the handler returns, as `@EndSaga` does. */
    fun end()
}
