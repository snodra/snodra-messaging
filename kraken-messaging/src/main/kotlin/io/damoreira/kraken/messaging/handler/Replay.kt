package io.damoreira.kraken.messaging.handler

/**
 * Keeps an `@EventHandler` (on the method) or every `@EventHandler` of a class (on the class) out of a replay (P6b, ADR 0023):
 * after a reset of its processor to an earlier position, the records in the replay range run the processor's other handlers
 * again but not this one. Use it for handlers with effects that must happen once (an e-mail, a call to a partner); a
 * projection that is rebuilt from the events should replay. Records past the range run it as usual. Sagas never replay.
 */
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class DisallowReplay

/**
 * Marks a method that prepares its processor for a replay (P6b, ADR 0023), typically by clearing a projection: a reset of the
 * processor that makes a replay range runs every reset handler of the processor once, on the instance that executes the
 * reset, before any marker or offset changes; one that throws fails the reset and changes nothing. The method takes no
 * parameter or one [io.damoreira.kraken.messaging.control.ResetContext] (the target and the ranges) and may suspend. Its
 * processor is its class's `@ProcessingGroup`, `default` without one. A reset to `latest` replays nothing and runs none.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class ResetHandler
