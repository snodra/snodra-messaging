package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.PostcardPrinted
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.external.ExternalRecord
import com.damoreira.snodra.messaging.external.TranslatedEvent
import kotlin.test.Test
import kotlin.test.assertEquals

class PrinterTranslatorTest {
    private val translator = PrinterTranslator(JsonCodec())

    private fun record(json: String) = ExternalRecord("printer.events", "g1", emptyMap(), json.toByteArray(), segment = "0", position = "7")

    @Test fun `a record of the printer topic becomes PostcardPrinted, deduplicated by the printer's delivery id`() {
        val translated = translator.translate(record("""{"deliveryId":"printed-g1","jobId":"g1","name":"topic-ada"}"""))

        assertEquals(TranslatedEvent(PostcardPrinted("g1", "topic-ada"), dedupeKey = "printed-g1"), translated)
    }
}
