package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Query

/** Sends a query through the kraken hub and awaits the reply from the owning service. */
interface QueryBus {
    suspend fun <R> send(query: Query<R>): R
}
