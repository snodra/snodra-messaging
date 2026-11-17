package com.damoreira.snodra.messaging.external

import com.damoreira.snodra.messaging.api.Event

/**
 * Turns a record of a partner's source (`snodra.messaging.external.<add-on>.sources.<name>`, for example a Kafka topic with
 * `snodra-messaging-kafka`) into a snodra event. A bean named like the source implements it; the library reads the source and
 * dispatches what [translate] returns locally.
 *
 * Return null to ignore the record. A thrown exception retries the record (as the event processors do) and then skips it.
 */
fun interface ExternalRecordTranslator {
    fun translate(record: ExternalRecord): TranslatedEvent?
}

/**
 * One record of a partner's source, as a translator reads it: the [source] it was read from (a topic on Kafka), its [key],
 * [headers] (UTF-8 text) and [payload] as published, and where it sits in the source ([segment] and [position]: the event store's
 * opaque strings, a partition and an offset in decimal on Kafka).
 */
class ExternalRecord(
    val source: String,
    val key: String?,
    val headers: Map<String, String>,
    val payload: ByteArray,
    val segment: String,
    val position: String,
) {
    override fun equals(other: Any?): Boolean = other is ExternalRecord && source == other.source && key == other.key &&
        headers == other.headers && payload.contentEquals(other.payload) && segment == other.segment && position == other.position

    override fun hashCode(): Int = 31 * (31 * source.hashCode() + segment.hashCode()) + position.hashCode()

    override fun toString(): String =
        "ExternalRecord(source=$source, key=$key, headers=$headers, payload=${payload.size} bytes, segment=$segment, position=$position)"
}

/**
 * What a record became: the [event] and, optionally, the key the partner's own id gives it. The dedupe id of the local
 * dispatch is `"<source>:<dedupeKey>"`, by default `"<source>:<segment>:<position>"` (`"<source>:<partition>:<offset>"` on
 * Kafka): two records with the same dedupe key are applied once.
 */
data class TranslatedEvent(val event: Event, val dedupeKey: String? = null)
