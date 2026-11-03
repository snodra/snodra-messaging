package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.external.ExternalCallUnknownOutcomeException
import io.damoreira.kraken.messaging.external.RetryableExternalException
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.client.match.MockRestRequestMatchers.content
import org.springframework.test.web.client.match.MockRestRequestMatchers.header
import org.springframework.test.web.client.match.MockRestRequestMatchers.method
import org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import org.springframework.test.web.client.response.MockRestResponseCreators.withException
import org.springframework.test.web.client.response.MockRestResponseCreators.withStatus
import org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import org.springframework.web.client.RestClient
import java.net.ConnectException
import java.net.SocketException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PrinterClientTest {
    private val builder = RestClient.builder().baseUrl("http://printer")
    private val server = MockRestServiceServer.bindTo(builder).build()
    private val printer = HttpPrinter(builder.build())

    @Test fun `submit posts the job with the idempotency key`() {
        server.expect(requestTo("http://printer/jobs"))
            .andExpect(method(HttpMethod.POST))
            .andExpect(header("Idempotency-Key", "saga-1:0"))
            .andExpect(content().json("""{"jobId":"g1","name":"webhook-ada"}"""))
            .andRespond(withSuccess("""{"jobId":"g1","status":"ACCEPTED"}""", MediaType.APPLICATION_JSON))

        printer.submit("g1", "webhook-ada", "saga-1:0")

        server.verify()
    }

    @Test fun `a printer that cannot be reached is retryable because nothing was sent`() {
        server.expect(requestTo("http://printer/jobs")).andRespond(withException(ConnectException("Connection refused")))

        assertFailsWith<RetryableExternalException> { printer.submit("g1", "webhook-ada", "k") }
    }

    @Test fun `a 5xx and a 429 are retryable`() {
        server.expect(requestTo("http://printer/jobs")).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE))
        server.expect(requestTo("http://printer/jobs")).andRespond(withStatus(HttpStatus.TOO_MANY_REQUESTS))

        assertFailsWith<RetryableExternalException> { printer.submit("g1", "webhook-ada", "k") }
        assertFailsWith<RetryableExternalException> { printer.submit("g1", "webhook-ada", "k") }
    }

    @Test fun `a 4xx is permanent`() {
        server.expect(requestTo("http://printer/jobs")).andRespond(withStatus(HttpStatus.BAD_REQUEST))

        val e = assertFailsWith<IllegalStateException> { printer.submit("g1", "webhook-ada", "k") }
        assertTrue("400" in e.message!!, e.message)
    }

    @Test fun `a connection lost after the request went out leaves the outcome unknown`() {
        server.expect(requestTo("http://printer/jobs")).andRespond(withException(SocketException("Connection reset")))

        assertFailsWith<ExternalCallUnknownOutcomeException> { printer.submit("g1", "crash-ada", "k") }
    }

    @Test fun `status reads the job, done once the printer says so`() {
        server.expect(requestTo("http://printer/jobs/g1")).andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("""{"jobId":"g1","name":"poll-ada","status":"PENDING"}""", MediaType.APPLICATION_JSON))
        server.expect(requestTo("http://printer/jobs/g1"))
            .andRespond(withSuccess("""{"jobId":"g1","name":"poll-ada","status":"DONE"}""", MediaType.APPLICATION_JSON))

        assertEquals(PrinterJob("g1", "poll-ada", done = false), printer.status("g1"))
        assertEquals(PrinterJob("g1", "poll-ada", done = true), printer.status("g1"))
    }

    @Test fun `status of a job the printer does not know is null`() {
        server.expect(requestTo("http://printer/jobs/g9")).andRespond(withStatus(HttpStatus.NOT_FOUND))

        assertNull(printer.status("g9"))
    }

    @Test fun `status failures are retryable, a read can simply be repeated`() {
        server.expect(requestTo("http://printer/jobs/g1")).andRespond(withException(SocketException("Connection reset")))
        server.expect(requestTo("http://printer/jobs/g1")).andRespond(withStatus(HttpStatus.BAD_GATEWAY))

        assertFailsWith<RetryableExternalException> { printer.status("g1") }
        assertFailsWith<RetryableExternalException> { printer.status("g1") }
    }
}
