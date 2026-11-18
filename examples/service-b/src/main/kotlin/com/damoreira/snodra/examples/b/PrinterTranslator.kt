package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.PostcardPrinted
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.external.ExternalRecord
import com.damoreira.snodra.messaging.external.ExternalRecordTranslator
import com.damoreira.snodra.messaging.external.TranslatedEvent
import org.springframework.stereotype.Component

/**
 * Turns a record of the printer's own topic (`snodra.messaging.external.kafka.sources.printer`) into [PostcardPrinted]. The
 * bean is named like the source; the printer's delivery id is the dedupe key, so a record produced twice applies once.
 */
@Component("printer")
class PrinterTranslator(private val codec: JsonCodec) : ExternalRecordTranslator {
    override fun translate(record: ExternalRecord): TranslatedEvent {
        val notification = codec.decode(record.payload, PrinterNotification::class)
        return TranslatedEvent(PostcardPrinted(notification.jobId, notification.name), dedupeKey = notification.deliveryId)
    }
}
