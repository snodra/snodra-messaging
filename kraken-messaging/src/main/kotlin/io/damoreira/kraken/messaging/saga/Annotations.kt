package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.api.Event
import kotlin.reflect.KClass

/**
 * Marks a class as a saga: a long-running process that reacts to events, keeps its state (plain properties, persisted
 * as JSON after each handled message) and may end. The class is discovered at startup, needs a no-argument constructor
 * and at least one [StartSaga] handler; an instance is created per saga and is never a Spring singleton.
 *
 * [name] is the saga's type name in `sagas.type` (default: the class's fully qualified name); set it to keep stored
 * sagas valid when the class is renamed or moved.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Saga(val name: String = "")

/**
 * Marks a [SagaEventHandler] that may create a saga instance when no live instance is associated with the event.
 * With [forceNew] it always creates a new instance, even when one is associated.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class StartSaga(val forceNew: Boolean = false)

/**
 * Marks a method `fun on(event: E, ...)` of a [Saga] as the handler of event type E (or of [payloadType]).
 *
 * The event's [associationProperty] value (or, when [associationHeader] is set, that message header's value) is looked up
 * as the association `keyName = value` among the live instances of the saga type; every match handles the event.
 * [keyName] defaults to [associationProperty] (to [associationHeader] when there is no property). With a header, the
 * property may be left empty. [payloadType] narrows a handler declared on a supertype of the event.
 *
 * Besides the event (first parameter) a handler may take `SagaLifecycle`, `SagaCommands`, `DeadlineManager`,
 * `MessageContext` and any Spring bean by type. It must not be `suspend`: it runs inside the saga's Mongo transaction.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class SagaEventHandler(
    val associationProperty: String,
    val keyName: String = "",
    val associationHeader: String = "",
    val payloadType: KClass<out Event> = Event::class,
)

/** The saga instance ends after the handler returns; allowed on [SagaEventHandler]s and [DeadlineHandler]s. */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class EndSaga

/**
 * Marks a method of a [Saga] as the handler of the deadlines named [deadlineName] that this instance scheduled. Its
 * first parameter, when present and not a lifecycle type, receives the deadline's payload.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class DeadlineHandler(val deadlineName: String)
