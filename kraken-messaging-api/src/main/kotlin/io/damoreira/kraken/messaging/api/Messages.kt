package io.damoreira.kraken.messaging.api

/** A fact that happened; published to Kafka topic `kraken.events`. */
interface Event

/** An intent handled by exactly one service; routed through the hub. [R] is the reply type. */
interface Command<R>

/** A read request handled by one service; routed through the hub. [R] is the reply type. */
interface Query<R>
