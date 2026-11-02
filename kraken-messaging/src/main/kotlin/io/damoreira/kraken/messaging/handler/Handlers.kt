package io.damoreira.kraken.messaging.handler

/** Marks a method `fun on(event: E)` as the handler for event type E. Routed by parameter type. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class EventHandler

/** Marks a method `fun handle(command: C): R` as the single handler for command type C. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class CommandHandler

/**
 * Marks a method `fun handle(query: Q): R` as the handler for query type Q.
 *
 * [shared] (P4a, ADR 0017) registers Q as a shared query type: many services may own it, a point query goes to one
 * instance of any owner (round-robin) and a scatter-gather asks all of them. Without it Q is exclusive: one owning
 * service, and the hub refuses a second one. Every registrant of Q, in every service, must declare it the same way:
 * the hub refuses a mix (`ALREADY_EXISTS` at register time, logged and surfaced as a registration failure like any
 * conflict), and one service declaring Q both ways fails at startup.
 *
 * Two query shapes are served by a method with a different contract: a `StreamingQuery<R>` (P4b) by one returning
 * `Flow<R>`, a `SubscriptionQuery<I, U>` (P4c) by one returning the initial result `I`; the updates `U` of a subscription
 * are pushed with `QueryUpdateEmitter`.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class QueryHandler(val shared: Boolean = false)
