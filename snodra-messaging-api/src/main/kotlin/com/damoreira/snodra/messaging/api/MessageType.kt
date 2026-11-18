package com.damoreira.snodra.messaging.api

/**
 * Stable wire name of a message, e.g. `@MessageType("gymnotes.WorkoutLogged")`.
 * The Kotlin class may be renamed or moved; this value must not change within a major version.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class MessageType(val value: String)
