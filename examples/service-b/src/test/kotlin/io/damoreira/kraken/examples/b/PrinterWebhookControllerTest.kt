package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.ExternalEventIngress
import io.damoreira.kraken.messaging.external.WebhookSignatureVerifier
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.servlet.assertj.MockMvcTester
import kotlin.test.BeforeTest
import kotlin.test.Test

@WebMvcTest(PrinterWebhookController::class)
@Import(PrinterWebhookControllerTest.Config::class)
@TestPropertySource(properties = ["examples.printer.webhook-secret=test-secret"])
class PrinterWebhookControllerTest {
    @TestConfiguration
    class Config {
        @Bean fun verifier() = WebhookSignatureVerifier()
        @Bean fun codec() = JsonCodec()
        @Bean fun ingress(): ExternalEventIngress = mockk()
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var ingress: ExternalEventIngress

    private val body = """{"deliveryId":"printed-g1","jobId":"g1","name":"webhook-ada"}"""
    private fun signature(secret: String = "test-secret") = "sha256=" + WebhookSignatureVerifier.hmacHex(secret, body.toByteArray())

    @BeforeTest fun reset() {
        clearMocks(ingress)
        every { ingress.accept(any(), any(), any(), any()) } returns ExternalEventIngress.Result.ACCEPTED
    }

    @Test fun `a request signed with the shared secret is accepted as the printer's event printed-g1`() {
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Signature", signature()).content(body))
            .hasStatus(HttpStatus.OK)
        verify(exactly = 1) { ingress.accept("printer", "printed-g1", PostcardPrinted("g1", "webhook-ada"), any()) }
    }

    @Test fun `the shared secret header works as well`() {
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Webhook-Secret", "test-secret").content(body))
            .hasStatus(HttpStatus.OK)
        verify(exactly = 1) { ingress.accept("printer", "printed-g1", any(), any()) }
    }

    @Test fun `a duplicate delivery is answered 200 too, the printer must not retry it`() {
        every { ingress.accept(any(), any(), any(), any()) } returns ExternalEventIngress.Result.DUPLICATE
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Signature", signature()).content(body))
            .hasStatus(HttpStatus.OK)
    }

    @Test fun `a wrong signature is 401 and reaches nothing`() {
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Signature", signature("other")).content(body))
            .hasStatus(HttpStatus.UNAUTHORIZED)
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).content(body)).hasStatus(HttpStatus.UNAUTHORIZED)
        verify(exactly = 0) { ingress.accept(any(), any(), any(), any()) }
    }

    @Test fun `a signed body that is not a printer notification is 400`() {
        val bad = """{"nothing":"here"}"""
        val sig = "sha256=" + WebhookSignatureVerifier.hmacHex("test-secret", bad.toByteArray())
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Signature", sig).content(bad))
            .hasStatus(HttpStatus.BAD_REQUEST)
        verify(exactly = 0) { ingress.accept(any(), any(), any(), any()) }
    }

    @Test fun `a failure while dispatching is not swallowed, the printer retries on the 5xx`() {
        every { ingress.accept(any(), any(), any(), any()) } throws IllegalStateException("mongo is down")
        assertThat(mvc.post().uri("/printer/webhook").contentType(MediaType.APPLICATION_JSON).header("X-Kraken-Signature", signature()).content(body))
            .failure().hasMessageContaining("mongo is down")
    }
}
