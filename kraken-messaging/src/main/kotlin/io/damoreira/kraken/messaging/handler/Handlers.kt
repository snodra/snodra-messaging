package io.damoreira.kraken.messaging.handler

/** Marks a method `fun on(event: E)` as the handler for event type E. Routed by parameter type. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class EventHandler

/** Marks a method `fun handle(command: C): R` as the single handler for command type C. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class CommandHandler

/** Marks a method `fun handle(query: Q): R` as the handler for query type Q. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class QueryHandler
