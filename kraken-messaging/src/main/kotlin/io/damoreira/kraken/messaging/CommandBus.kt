package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Command
import kotlinx.coroutines.runBlocking

/** Sends a command through the kraken hub and awaits the reply from the owning service. */
interface CommandBus {
    suspend fun <R> send(command: Command<R>, routingKey: String? = null): R

    /** Blocking variant of [send] for non-coroutine callers; never call it from a coroutine. */
    fun <R> sendBlocking(command: Command<R>, routingKey: String? = null): R = runBlocking { send(command, routingKey) }
}
