package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.ExternalRecordTranslator
import io.damoreira.kraken.messaging.external.TranslatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component

/**
 * Turns a record of the printer's own topic (`kraken.messaging.external.kafka.sources.printer`) into [PostcardPrinted]. The
 * bean is named like the source; the printer's delivery id is the dedupe key, so a record produced twice applies once.
 */
@Component("printer")
class PrinterTranslator(private val codec: JsonCodec) : ExternalRecordTranslator {
    override fun translate(record: ConsumerRecord<String, ByteArray>): TranslatedEvent {
        val notification = codec.decode(record.value(), PrinterNotification::class)
        return TranslatedEvent(PostcardPrinted(notification.jobId, notification.name), dedupeKey = notification.deliveryId)
    }
}
