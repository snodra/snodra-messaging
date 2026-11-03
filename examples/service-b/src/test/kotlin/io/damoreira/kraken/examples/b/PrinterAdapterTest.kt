package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PollPrintJob
import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.examples.api.PrintPostcard
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.external.IdempotentHandler
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberFunctions
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** A printer that remembers what it was asked and answers jobs with the given states. */
private class RecordingPrinter(var job: PrinterJob? = null) : Printer {
    val submitted = mutableListOf<Triple<String, String, String>>()
    override fun submit(jobId: String, name: String, idempotencyKey: String) { submitted += Triple(jobId, name, idempotencyKey) }
    override fun status(jobId: String): PrinterJob? = job
}

class PrinterAdapterTest {
    private val buses = InMemoryBuses(typePrefix = "service-b")
    private val printer = RecordingPrinter()
    private val adapter = PrinterAdapter(printer, buses)

    private fun ctx(headers: Map<String, String>) = MessageContext("m1", "examples.PrintPostcard", "c1", null, emptySet(), null, headers)

    @Test fun `PrintPostcard submits the job to the printer under the idempotency key of the command`() {
        adapter.print(PrintPostcard("g1", "webhook-ada"), ctx(mapOf("idempotency-key" to "saga-1:0")))

        assertEquals(listOf(Triple("g1", "webhook-ada", "saga-1:0")), printer.submitted)
    }

    @Test fun `PrintPostcard without an idempotency key is refused, the partner could not de-duplicate it`() {
        val e = assertFailsWith<IllegalStateException> { adapter.print(PrintPostcard("g1", "webhook-ada"), ctx(emptyMap())) }
        assertTrue("idempotency-key" in e.message!!, e.message)
        assertTrue(printer.submitted.isEmpty())
    }

    @Test fun `PollPrintJob publishes PostcardPrinted when the printer says the job is done`() {
        printer.job = PrinterJob("g1", "poll-ada", done = true)

        adapter.poll(PollPrintJob("g1", 3))

        val published = buses.published.single()
        assertEquals("poll-ada", published.key)
        assertEquals(PostcardPrinted("g1", "poll-ada"), assertIs<PostcardPrinted>(published.event))
    }

    @Test fun `PollPrintJob publishes nothing while the job is pending or unknown`() {
        printer.job = PrinterJob("g1", "poll-ada", done = false)
        adapter.poll(PollPrintJob("g1", 1))
        printer.job = null
        adapter.poll(PollPrintJob("g1", 2))

        assertTrue(buses.published.isEmpty())
    }

    @Test fun `both handlers are idempotent, the poll one per attempt`() {
        val keys = PrinterAdapter::class.memberFunctions.filter { it.name == "print" || it.name == "poll" }
            .associate { it.name to assertNotNull(it.findAnnotation<IdempotentHandler>(), it.name).key }

        assertEquals("headers['idempotency-key']", keys["print"])
        assertEquals("'poll:' + message.jobId + ':' + message.attempt", keys["poll"])
    }
}
