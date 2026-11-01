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
