package io.damoreira.kraken.messaging.eventstore

/**
 * The event store (ADR 0027): where a service's published events are kept and read from by its event processors, behind one
 * interface, with one add-on per event store implementing it.
 *
 * Every event store provides the contract below, which `EventStoreConformanceTest` (in `kraken-messaging-test`) checks:
 *
 * - **Publish**: the outbox relay hands it one [OutgoingEvent]; [publish] returns once the event store accepted it.
 * - **One subscription per processor**: every [SubscriptionSpec.name] receives every event of its source, whatever other
 *   processors and services do.
 * - **Competing instances**: the [Subscription]s of one name share its segments; adding one spreads them.
 * - **Order per key**: events with the same key go to one segment and reach a processor in publish order.
 * - **At-least-once with acknowledgement**: the core acknowledges after the handler and the processed-event record committed;
 *   what was not acknowledged comes again, and the message store's processed events skip it (ADR 0028,
 *   [io.damoreira.kraken.messaging.store.ProcessedEventStore]).
 * - **Pause and resume**: a subscription stops delivering without losing its place.
 * - **The headers**: the seven [EventHeaders] travel as the event store's native metadata.
 *
 * and declares the [Capability]s it offers (`replay`, `browse`, `lag`, `native-dead-letter`).
 *
 * Segments and positions are opaque strings that only the event store reads, at most [MAX_SEGMENT_BYTES] and
 * [MAX_POSITION_BYTES] bytes; [compare] orders two positions of one segment, and is the only way the core orders them.
 * Payloads are JSON bytes, carried as given.
 *
 * A new subscription name starts at the earliest position of every segment.
 */
interface EventStore {
    /** The add-on's name (`kafka`, `rabbitmq-streams`, `memory` or a third party's), reported to the hub as the processors' event store. */
    val name: String

    /** What it offers beyond the required contract. */
    val capabilities: Set<Capability>

    /** Whether it declares [capability]. */
    fun supports(capability: Capability): Boolean = capability in capabilities

    /**
     * Appends [event] to [OutgoingEvent.source]; returns once the event store accepted it. Two publishes of one key, the second
     * called after the first returned, are delivered in that order. Throws [EventStoreException.Rejected] when the event store
     * will never take this event (the relay gives it up at once), [EventStoreException.Failed] when another try may succeed.
     */
    suspend fun publish(event: OutgoingEvent)

    /** Joins the subscription [spec] as one more instance. */
    suspend fun subscribe(spec: SubscriptionSpec): Subscription

    /**
     * The reset side of the subscription [spec], for a reset run while no instance reads it; throws
     * [EventStoreException.Unsupported] without [Capability.REPLAY].
     */
    fun admin(spec: SubscriptionSpec): SubscriptionAdmin

    /** The order of two positions [a] and [b] of one segment: negative when [a] comes first, zero when equal, positive otherwise. */
    fun compare(a: String, b: String): Int
}
