package io.damoreira.kraken.messaging.api

/**
 * Stable wire name of a message, e.g. `@MessageType("gymnotes.WorkoutLogged")`.
 * The Kotlin class may be renamed or moved; this value must not change within a major version.
 * See ../kraken/docs/02-messaging.md.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class MessageType(val value: String)
