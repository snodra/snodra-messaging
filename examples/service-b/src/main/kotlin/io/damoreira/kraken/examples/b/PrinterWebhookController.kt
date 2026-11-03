package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.external.ExternalEventIngress
import io.damoreira.kraken.messaging.external.WebhookSignatureVerifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController

/** What the printer tells service-b when a postcard is printed: on its webhook and on its Kafka topic alike. */
data class PrinterNotification(val deliveryId: String, val jobId: String, val name: String)

/**
 * The printer's webhook: verify the raw body against the shared secret, then hand the event to the library, which applies it
 * once however often the printer sends it (`deliveryId` is the printer's id of the delivery). 200 for a new and for a repeated
 * delivery, 401 for a bad signature, 400 for a body that is no notification; any other failure is thrown, so the printer
 * gets a 5xx and retries.
 */
@RestController
class PrinterWebhookController(
    private val verifier: WebhookSignatureVerifier,
    private val ingress: ExternalEventIngress,
    private val codec: JsonCodec,
    @Value("\${examples.printer.webhook-secret}") private val secret: String,
) {
    @PostMapping("/printer/webhook")
    fun receive(@RequestBody body: ByteArray, @RequestHeader headers: HttpHeaders): ResponseEntity<Void> {
        if (!verifier.verify(secret, null, headers.toSingleValueMap(), body)) return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build()
        val notification = try {
            codec.decode(body, PrinterNotification::class)
        } catch (_: InvalidMessageException) {
            return ResponseEntity.badRequest().build()
        }
        ingress.accept("printer", notification.deliveryId, PostcardPrinted(notification.jobId, notification.name))
        return ResponseEntity.ok().build()
    }
}
