package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.api.Event
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Turns a record of a partner's Kafka topic (`kraken.messaging.external.kafka.sources.<name>`) into a kraken event. A bean
 * named like the source implements it; the library runs the consumer and dispatches what [translate] returns locally.
 *
 * Return null to ignore the record. A thrown exception retries the record (as the main consumer does) and then skips it.
 */
fun interface ExternalRecordTranslator {
    fun translate(record: ConsumerRecord<String, ByteArray>): TranslatedEvent?
}

/**
 * What a record became: the [event] and, optionally, the key the partner's own id gives it. The dedupe id of the local
 * dispatch is `"<source>:<dedupeKey>"`, by default `"<source>:<partition>:<offset>"`: two records with the same dedupe key
 * are applied once.
 */
data class TranslatedEvent(val event: Event, val dedupeKey: String? = null)
