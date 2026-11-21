package com.damoreira.snodra.messaging

import com.damoreira.snodra.messaging.api.Query
import com.damoreira.snodra.messaging.api.StreamingQuery
import com.damoreira.snodra.messaging.api.SubscriptionQuery
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking

/** Sends a query through the snodra hub and awaits the reply from the owning service. */
interface QueryBus {
    suspend fun <R> send(query: Query<R>): R

    /** Like [send] with per-call [SendOptions] (priority, timeout, retry; a routing key is ignored for queries). */
    suspend fun <R> send(query: Query<R>, options: SendOptions): R = send(query)

    /** Blocking variant of [send] for non-coroutine callers; never call it from a coroutine. */
    fun <R> sendBlocking(query: Query<R>): R = runBlocking { send(query) }

    /** Blocking variant of [send] with [SendOptions]; never call it from a coroutine. */
    fun <R> sendBlocking(query: Query<R>, options: SendOptions): R = runBlocking { send(query, options) }

    /**
     * Scatter-gather: asks every live instance of every service owning the query's type (a shared type,
     * `@QueryHandler(shared = true)`; on an exclusive type every instance of its owner) and returns every answer that
     * arrived by the deadline, in arrival order, with the counts of failed and timed-out targets. Partial answers are
     * answers: a failed or missing target does not fail the call. Always goes through the hub, also when this instance
     * owns the type (it is one of the targets then).
     *
     * @throws NoHandlerException when nobody owns the type
     * @throws HubConnectionLostException when the stream to the hub breaks before the end (outcome unknown)
     */
    suspend fun <R> scatterGather(query: Query<R>, options: ScatterOptions = ScatterOptions()): ScatterResult<R>

    /**
     * [scatterGather] as a [Flow]: emits each part as it arrives and completes normally on the end. The counts of
     * failed and timed-out targets are not part of the flow; use [scatterGather] when they matter. Errors before any
     * part (no owner, a command) and a broken stream fail the flow with [scatterGather]'s exceptions.
     */
    fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions = ScatterOptions()): Flow<ScatterPart<R>> =
        flow { scatterGather(query, options).parts.forEach { emit(it) } }

    /**
     * Streaming query: one instance of the type's owner (one of every owner's instances on a shared
     * type) streams its answer part by part under this collector's back-pressure. The flow is cold: each collection
     * sends one request through the hub; parts are emitted as they arrive and the flow completes when the handler's
     * flow does. The handler is paced by credits ([StreamOptions.initialCredit]); cancelling the collector (or an
     * exception in it) cancels the handler. Always goes through the hub, also when this instance owns the type.
     *
     * An end with an error fails the flow exactly like a unary [send] fails on an error with the same hub code; the
     * parts emitted before it stay emitted. Retry (the configured policy or [StreamOptions.retry]) applies with the
     * query rules only until the first part.
     *
     * @throws NoHandlerException when nobody owns the type
     * @throws RemoteHandlerException when the handler's flow failed (`HANDLER_ERROR`)
     * @throws MessageTimeoutException when no part arrived within the idle timeout (`TIMEOUT`)
     * @throws HandlerDisconnectedException when the handler's instance left mid-stream (`HANDLER_DISCONNECTED`)
     * @throws HandlerBusyException when the handler's hub queue was full (`RESOURCE_EXHAUSTED`)
     * @throws StreamCancelledException when the hub ended the stream for outliving its maximum duration (`CANCELLED`)
     * @throws HubConnectionLostException when the stream to the hub breaks before the end (outcome unknown)
     */
    fun <R> stream(query: StreamingQuery<R>, options: StreamOptions = StreamOptions()): Flow<R>

    /**
     * Subscription query: returns once the initial result [I] arrived, with the [U] updates the handlers
     * push afterwards as [SubscriptionResult.updates]. One handler instance answers the initial result (a point query's
     * choice among the owners' instances); **every** instance of every owner is a target for updates, as they are when the subscription opens: an instance that registers later is never added and a target that
     * leaves is not replaced (updates are delivered at most once; resubscribe to hear every instance again). Always goes
     * through the hub, also when this instance owns the type.
     *
     * Retry (the configured policy or [SubscriptionOptions.retry]) applies with the query rules only until the initial
     * result; the caller's bearer token and trace context are read when this is called.
     *
     * @throws NoHandlerException when nobody owns the type
     * @throws RemoteHandlerException when the initial request failed (`HANDLER_ERROR`) or a handler's emitter did
     * @throws MessageTimeoutException when the initial result did not arrive within [SubscriptionOptions.timeout]
     * @throws HandlerDisconnectedException when the handler instances left (`HANDLER_DISCONNECTED`)
     * @throws HandlerBusyException when an instance's subscription limit or the hub queue is full (`RESOURCE_EXHAUSTED`)
     * @throws HubConnectionLostException when the stream to the hub breaks before the initial result (outcome unknown)
     */
    suspend fun <I, U> subscribe(query: SubscriptionQuery<I, U>, options: SubscriptionOptions = SubscriptionOptions()): SubscriptionResult<I, U>
}
