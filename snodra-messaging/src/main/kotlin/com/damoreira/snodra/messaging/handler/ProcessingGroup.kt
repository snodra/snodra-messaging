package com.damoreira.snodra.messaging.handler

/**
 * Puts every `@EventHandler` method of the annotated class in the named event processor (P6a, ADR 0022). Each processor
 * reads the service's events in its own subscription of the event store (`<prefix>.<name>`, where `<prefix>` is
 * `snodra.messaging.events.subscription-prefix`, else the service name; on Kafka the consumer group), with its own position,
 * its own idempotency markers (`processor:<name>:<eventId>`) and its own dead letters, so a slow or failing handler group never
 * holds the others. Classes without it are in processor `default` (subscription `<prefix>.default`, markers
 * `processor:default:<eventId>`), the only processor that runs sagas.
 *
 * [value] must match `^[a-z0-9][a-z0-9-]{0,47}$` and must not be `default`; a violation fails the startup naming the
 * handler. One event type may be handled in several processors: each gets the record. Command and query handlers of the
 * class are not affected.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class ProcessingGroup(val value: String)

/** Names of event processors (ADR 0022) and the marker id each one writes in `processed_events`. */
object Processors {
    /** The processor of every `@EventHandler` whose class has no [ProcessingGroup]. */
    const val DEFAULT = "default"

    private val NAME = Regex("^[a-z0-9][a-z0-9-]{0,47}$")

    /** Whether [name] is a valid explicit processor name (the pattern, and not [DEFAULT]). */
    fun isValidName(name: String): Boolean = name != DEFAULT && NAME.matches(name)

    /**
     * The subscription name of [processor] on the event store, [DEFAULT] included: `<prefix>.<processor>` (the prefix is
     * `snodra.messaging.events.subscription-prefix`, else the service name). On Kafka the consumer group, as it always was.
     */
    fun subscriptionName(prefix: String, processor: String): String = "$prefix.$processor"

    /** The stage-1 marker `_id` of [dedupeId] in [processor], [DEFAULT] included: `processor:<processor>:<dedupeId>`. */
    fun markerId(processor: String, dedupeId: String): String = "processor:$processor:$dedupeId"

    /** External source names that would let a dedupe id collide with a marker family of ADR 0021 or 0022. */
    val RESERVED_SOURCE_NAMES: Set<String> = setOf("processor", "saga", "ingress", "failed")
}
