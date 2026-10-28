package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Command

/** Sends a command through the kraken hub and awaits the reply from the owning service. */
interface CommandBus {
    suspend fun <R> send(command: Command<R>, routingKey: String? = null): R
}
