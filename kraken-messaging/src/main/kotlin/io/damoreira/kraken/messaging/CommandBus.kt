package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Command
import kotlinx.coroutines.runBlocking

/** Sends a command through the kraken hub and awaits the reply from the owning service. */
interface CommandBus {
    suspend fun <R> send(command: Command<R>, routingKey: String? = null): R

    /** Like [send] with per-call [SendOptions] (priority, timeout, routing key, retry). */
    suspend fun <R> send(command: Command<R>, options: SendOptions): R = send(command, options.routingKey)

    /** Blocking variant of [send] for non-coroutine callers; never call it from a coroutine. */
    fun <R> sendBlocking(command: Command<R>, routingKey: String? = null): R = runBlocking { send(command, routingKey) }

    /** Blocking variant of [send] with [SendOptions]; never call it from a coroutine. */
    fun <R> sendBlocking(command: Command<R>, options: SendOptions): R = runBlocking { send(command, options) }
}
