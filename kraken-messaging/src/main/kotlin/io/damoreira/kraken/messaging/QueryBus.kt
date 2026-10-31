package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Query
import kotlinx.coroutines.runBlocking

/** Sends a query through the kraken hub and awaits the reply from the owning service. */
interface QueryBus {
    suspend fun <R> send(query: Query<R>): R

    /** Like [send] with per-call [SendOptions] (priority, timeout, retry; a routing key is ignored for queries). */
    suspend fun <R> send(query: Query<R>, options: SendOptions): R = send(query)

    /** Blocking variant of [send] for non-coroutine callers; never call it from a coroutine. */
    fun <R> sendBlocking(query: Query<R>): R = runBlocking { send(query) }

    /** Blocking variant of [send] with [SendOptions]; never call it from a coroutine. */
    fun <R> sendBlocking(query: Query<R>, options: SendOptions): R = runBlocking { send(query, options) }
}
