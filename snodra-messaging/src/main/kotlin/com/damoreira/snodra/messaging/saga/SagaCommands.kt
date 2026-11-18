package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event

/**
 * What a saga handler may send: recorded in the saga's transaction and dispatched after it committed, in the order the
 * calls were made (never for a state that rolled back). A saga does not wait for replies.
 */
interface SagaCommands {
    fun send(command: Command<*>, routingKey: String? = null)
    fun publish(event: Event, key: String? = null)
}
