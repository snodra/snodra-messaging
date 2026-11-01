package io.damoreira.kraken.messaging.api

/** A fact that happened; published to Kafka topic `kraken.events`. */
interface Event

/** An intent handled by exactly one service; routed through the hub. [R] is the reply type. */
interface Command<R>

/** A read request handled by one service; routed through the hub. [R] is the reply type. */
interface Query<R>

/**
 * A read request whose answer is a stream of [R] (P4b, ADR 0017): one handler instance emits the elements one by one
 * under the sender's back-pressure (credits), routed through the hub with `mode = STREAMING`. Handled by a
 * `@QueryHandler` returning `Flow<R>`; sent with `QueryBus.stream`. Not a [Query]: it cannot be sent as a point query
 * or a scatter-gather.
 */
interface StreamingQuery<out R>

/**
 * A read request whose answer is an initial result [I] followed by the updates [U] every handler instance of the type
 * pushes (P4c, ADR 0017), until the subscriber cancels or the handlers complete. Routed through the hub with
 * `mode = SUBSCRIPTION`: one capable instance answers the initial result through its `@QueryHandler` (which returns
 * [I]), and every capable instance's `QueryUpdateEmitter` delivers the updates. Sent with `QueryBus.subscribe`. Not a
 * [Query]: it cannot be sent as a point query, a scatter-gather or a stream.
 */
interface SubscriptionQuery<out I, out U>
