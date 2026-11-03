package io.damoreira.kraken.examples.printer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

@SpringBootApplication
class FakePrinterApplication

fun main(args: Array<String>) {
    runApplication<FakePrinterApplication>(*args)
}

/** Wires the printer to the real world: the webhook over HTTP (signed), the topic over Kafka, a thread pool for the printing time. */
@Configuration(proxyBeanMethods = false)
class PrinterWiring {
    private val log = LoggerFactory.getLogger(PrinterWiring::class.java)

    @Bean
    fun printer(
        mapper: JsonMapper,
        @Value("\${printer.webhook-url}") webhookUrl: String,
        @Value("\${printer.webhook-secret}") secret: String,
        @Value("\${printer.topic}") topic: String,
        @Value("\${printer.kafka-bootstrap}") bootstrap: String,
        @Value("\${printer.print-delay}") printDelay: Duration,
        @Value("\${printer.poll-delay}") pollDelay: Duration,
    ): FakePrinter {
        val pool = ScheduledThreadPoolExecutor(2)
        val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(3)).build()
        val producer by lazy {
            KafkaProducer<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                ),
            )
        }
        val notifier = object : Notifier {
            override fun webhook(n: Notification) = retrying("webhook of ${n.jobId}") {
                val body = mapper.writeValueAsBytes(n)
                val request = HttpRequest.newBuilder(URI.create(webhookUrl)).timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json").header("X-Kraken-Signature", sign(secret, body))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build()
                val status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
                check(status in 200..299) { "the webhook answered $status" }
            }

            override fun topic(n: Notification) = retrying("topic record of ${n.jobId}") {
                producer.send(ProducerRecord(topic, n.jobId, mapper.writeValueAsString(n))).get(10, TimeUnit.SECONDS)
            }
        }
        return FakePrinter(
            clock = { Clock.systemUTC().instant() },
            notifier = notifier,
            later = { delay, task -> pool.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS) },
            crash = { Runtime.getRuntime().halt(1) },
            pollDelay = pollDelay,
            printDelay = printDelay,
        )
    }

    /** The customer may be restarting: a real partner retries its notification, so does this one (10 tries, 2 s apart). */
    private fun retrying(what: String, action: () -> Unit) {
        for (attempt in 1..10) {
            try {
                action()
                log.info("Printer: {} delivered (attempt {})", what, attempt)
                return
            } catch (e: Exception) {
                log.warn("Printer: {} failed on attempt {}: {}", what, attempt, e.toString())
                Thread.sleep(2000)
            }
        }
        log.error("Printer: giving up on {}", what)
    }
}
