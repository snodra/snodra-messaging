package io.damoreira.kraken.messaging.eventstore

/**
 * The headers every event carries (ADR 0027), as the event store's native metadata.
 */
object EventHeaders {
    /** The event's type name. */
    const val TYPE = "type"

    /** The event's id. */
    const val ID = "id"

    const val CORRELATION_ID = "correlationId"

    /** When the event was recorded, RFC 3339 in UTC with milliseconds. */
    const val TIMESTAMP = "timestamp"

    /** The publishing service's name (not the event store's source). */
    const val SOURCE = "source"

    /** The caller's user id; empty when none. */
    const val USER_ID = "userId"

    /** The W3C trace context of the message that caused the event. */
    const val TRACEPARENT = "traceparent"

    /** The seven headers. */
    val ALL: List<String> = listOf(TYPE, ID, CORRELATION_ID, TIMESTAMP, SOURCE, USER_ID, TRACEPARENT)
}

/** The most bytes of a segment's name (hub.proto bounds: a longer one is dropped by the hub). */
const val MAX_SEGMENT_BYTES = 128

/** The most bytes of a position (hub.proto bounds: a longer one reads as unknown on the hub). */
const val MAX_POSITION_BYTES = 128

/**
 * The dead-letter source of [source] (`<source>.dlt`): where the dead-letter mode `event-store` sends what a processor cannot
 * handle, on an event store declaring [Capability.NATIVE_DEAD_LETTER].
 */
fun deadLetterSource(source: String): String = "$source.dlt"

/** What an event store may offer beyond the required contract (ADR 0027); the hub and the dashboard offer only what an event store declares. */
enum class Capability(
    /** The protocol's name (`ProcessorStatus.capabilities`). */
    val wireName: String,
) {
    /** Reset to earliest, latest, a time or explicit positions: reset handlers, replay ranges ([SubscriptionAdmin]). */
    REPLAY("replay"),

    /** The event browser can read it: search, one record, the live tail. */
    BROWSE("browse"),

    /** How far a subscription is behind, per segment ([SegmentStatus.lag]). */
    LAG("lag"),

    /** A dead-letter source ([deadLetterSource]): the dead-letter mode `event-store`. */
    NATIVE_DEAD_LETTER("native-dead-letter"),
    ;

    override fun toString(): String = wireName
}

/**
 * One event the outbox relay hands the event store ([EventStore.publish]). [source] is the source it goes to:
 * the service's events source, or its dead-letter source ([deadLetterSource]). [id] and [typeName] are also in headers
 * [EventHeaders.ID] and [EventHeaders.TYPE]. [key] is the order key: events with one key reach a processor in publish order;
 * null spreads them as the event store does. [headers] are carried as given, as the event store's native metadata: the seven
 * [EventHeaders] and any other (a dead letter's `kraken-dlt-*`). Headers are UTF-8 text, one value per name: a record read with
 * a header that is not UTF-8, or with a name repeated, is not carried byte for byte (a dead letter sent to the event store
 * carries its text, the last value of a name). [payload] is JSON bytes, carried as given.
 */
class OutgoingEvent(
    val source: String,
    val id: String,
    val typeName: String,
    val key: String?,
    val headers: Map<String, String>,
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean = other is OutgoingEvent && source == other.source && id == other.id &&
        typeName == other.typeName && key == other.key && headers == other.headers && payload.contentEquals(other.payload)

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String =
        "OutgoingEvent(source=$source, id=$id, typeName=$typeName, key=$key, headers=$headers, payload=${payload.size} bytes)"
}

/** What [Subscription.next] gives: an assignment change or a [Delivery]. */
sealed interface SubscriptionEvent {
    /** These segments became this subscription's (in addition to the ones it has). */
    data class Assigned(val segments: List<String>) : SubscriptionEvent

    /**
     * These segments are this subscription's no longer; what it delivered of them and did not acknowledge comes again to
     * whichever subscription gets them. Always followed by an [Assigned] (possibly empty), which ends the change.
     */
    data class Revoked(val segments: List<String>) : SubscriptionEvent
}

/**
 * One event a subscription delivers, read from [segment] (a segment of the source: a unit of order per key) at [position]
 * (which [Subscription.ack] takes). [headers], [key] and [payload] are as published; [timestampMs] is when the event store
 * appended it, epoch milliseconds (its own clock). Headers are UTF-8 text, one value per name: a header value that is not UTF-8
 * is read lossily and a repeated name keeps its last value, so such a record is not byte-preserved when it is dead-lettered.
 *
 * [segment] and [position] are opaque strings of at most [MAX_SEGMENT_BYTES] and [MAX_POSITION_BYTES] bytes that only the event
 * store reads (decimal numbers for the first-wave stores); the core orders two positions of one segment
 * only through [EventStore.compare].
 */
class Delivery(
    val segment: String,
    val position: String,
    val key: String?,
    val headers: Map<String, String>,
    val payload: ByteArray,
    val timestampMs: Long,
) : SubscriptionEvent {
    override fun equals(other: Any?): Boolean = other is Delivery && segment == other.segment && position == other.position &&
        key == other.key && headers == other.headers && payload.contentEquals(other.payload) && timestampMs == other.timestampMs

    override fun hashCode(): Int = 31 * segment.hashCode() + position.hashCode()

    override fun toString(): String =
        "Delivery(segment=$segment, position=$position, key=$key, headers=$headers, payload=${payload.size} bytes, timestampMs=$timestampMs)"
}

/**
 * A processor's subscription: every subscription of one [name] shares the events of [source] (competing instances), and each
 * name receives every event (one per processor). [name] is `<service>.<processor>` (the name the event store knows the
 * subscription by); [source] the source read.
 */
data class SubscriptionSpec(val name: String, val source: String)

/**
 * One segment assigned to a subscription, as [Subscription.segments] reports it (hub.proto `SegmentStatus`, without the replay
 * range, which is the core's). Positions as in [Delivery]; null when unknown.
 */
data class SegmentStatus(
    val segment: String,
    /** The next position this subscription delivers. */
    val position: String? = null,
    /** The subscription's committed position (the one after its last acknowledgement). */
    val committed: String? = null,
    /** The position of the next event appended to the segment. */
    val head: String? = null,
    /**
     * How many events lie between [committed] and [head] (between [position] and [head] when nothing is committed): what is not
     * acknowledged yet. Null when unknown, always null without [Capability.LAG].
     */
    val lag: Long? = null,
)

/** What an event store operation fails with. */
sealed class EventStoreException(message: String, cause: Throwable? = null) : RuntimeException(message, cause) {
    /** A bad argument: an unknown segment, a position the segment does not have. */
    class Invalid(why: String) : EventStoreException("invalid: $why")

    /**
     * Another instance still reads the subscription ([members] describes them), so its positions cannot move
     * (`CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE`).
     */
    class SubscribersActive(val members: String) : EventStoreException("the subscription has active subscribers: $members")

    /** The event store lacks [capability] (`CONTROL_REFUSAL_UNSUPPORTED`). */
    class Unsupported(val capability: Capability) : EventStoreException("the event store lacks the capability $capability")

    /**
     * The event store refused the event for good (too large, not authorized, a destination it cannot have): publishing it again
     * cannot succeed, so the outbox relay gives it up at once.
     */
    class Rejected(why: String, cause: Throwable? = null) : EventStoreException("rejected by the event store: $why", cause)

    /** The event store failed or cannot be reached; trying again may succeed. */
    class Failed(why: String, cause: Throwable? = null) : EventStoreException("event store: $why", cause)
}
