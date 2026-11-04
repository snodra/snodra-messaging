package io.damoreira.kraken.messaging.handler

/**
 * Puts every `@EventHandler` method of the annotated class in the named event processor (P6a, ADR 0022). Each processor
 * is its own Kafka consumer in its own consumer group (`<group>.<name>`, where `<group>` is the unnamed processor's group:
 * `kraken.messaging.kafka.group-id`, else the service name) on the same events topic, with its own position, its own
 * idempotency markers (`processor:<name>:<eventId>`) and its own dead letters, so a slow or failing handler group never
 * holds the others. Classes without it are in processor `default`, which keeps the service's group and the plain
 * `<eventId>` markers, and is the only processor that runs sagas.
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

    /** The consumer group of [processor]: [defaultGroup] for [DEFAULT], `<defaultGroup>.<processor>` otherwise. */
    fun groupId(defaultGroup: String, processor: String): String =
        if (processor == DEFAULT) defaultGroup else "$defaultGroup.$processor"

    /** The stage-1 marker `_id` of [dedupeId] in [processor]: unchanged for [DEFAULT], `processor:<name>:<dedupeId>` otherwise. */
    fun markerId(processor: String, dedupeId: String): String =
        if (processor == DEFAULT) dedupeId else "processor:$processor:$dedupeId"

    /** External source names that would let a dedupe id collide with a marker family of ADR 0021 or 0022. */
    val RESERVED_SOURCE_NAMES: Set<String> = setOf("processor", "saga", "ingress", "failed")
}
