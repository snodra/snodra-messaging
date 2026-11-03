package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.ExternalRecordTranslator
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** The translator bean must be named like its source (`printer`), so nothing else in the context may take that name. */
class PrinterBeansTest {
    @Test fun `the printer client and the translator named after the source live in one context`() {
        ApplicationContextRunner()
            .withUserConfiguration(PrinterConfiguration::class.java, PrinterTranslator::class.java)
            .withBean(JsonCodec::class.java, { JsonCodec() })
            .withPropertyValues("examples.printer.url=http://printer")
            .run { context ->
                assertEquals(null, context.startupFailure)
                assertIs<PrinterTranslator>(context.getBean("printer"))
                assertEquals(1, context.getBeansOfType(ExternalRecordTranslator::class.java).size)
                assertIs<HttpPrinter>(context.getBean(Printer::class.java))
            }
    }
}
