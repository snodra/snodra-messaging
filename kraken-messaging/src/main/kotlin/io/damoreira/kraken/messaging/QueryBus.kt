package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Query
import kotlinx.coroutines.runBlocking

/** Sends a query through the kraken hub and awaits the reply from the owning service. */
interface QueryBus {
    suspend fun <R> send(query: Query<R>): R

    /** Blocking variant of [send] for non-coroutine callers; never call it from a coroutine. */
    fun <R> sendBlocking(query: Query<R>): R = runBlocking { send(query) }
}
