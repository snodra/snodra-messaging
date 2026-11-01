package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Query
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
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

    /**
     * Scatter-gather (ADR 0017): asks every live instance of every service owning the query's type (a shared type,
     * `@QueryHandler(shared = true)`; on an exclusive type every instance of its owner) and returns every answer that
     * arrived by the deadline, in arrival order, with the counts of failed and timed-out targets. Partial answers are
     * answers: a failed or missing target does not fail the call. Always goes through the hub, also when this instance
     * owns the type (it is one of the targets then).
     *
     * @throws UnsupportedByHubException when the hub does not offer `scatter-gather` (nothing is sent)
     * @throws NoHandlerException when nobody owns the type
     * @throws HubConnectionLostException when the stream to the hub breaks before the end (outcome unknown)
     */
    suspend fun <R> scatterGather(query: Query<R>, options: ScatterOptions = ScatterOptions()): ScatterResult<R> =
        throw UnsupportedOperationException("${this::class.simpleName} does not support scatter-gather")

    /**
     * [scatterGather] as a [Flow]: emits each part as it arrives and completes normally on the end. The counts of
     * failed and timed-out targets are not part of the flow; use [scatterGather] when they matter. Errors before any
     * part (no owner, unsupported hub) and a broken stream fail the flow with [scatterGather]'s exceptions.
     */
    fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions = ScatterOptions()): Flow<ScatterPart<R>> =
        flow { scatterGather(query, options).parts.forEach { emit(it) } }
}
