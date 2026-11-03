package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.TranslatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class PrinterTranslatorTest {
    private val translator = PrinterTranslator(JsonCodec())

    private fun record(json: String) = ConsumerRecord("printer.events", 0, 7L, "g1", json.toByteArray())

    @Test fun `a record of the printer topic becomes PostcardPrinted, deduplicated by the printer's delivery id`() {
        val translated = translator.translate(record("""{"deliveryId":"printed-g1","jobId":"g1","name":"topic-ada"}"""))

        assertEquals(TranslatedEvent(PostcardPrinted("g1", "topic-ada"), dedupeKey = "printed-g1"), translated)
    }
}
