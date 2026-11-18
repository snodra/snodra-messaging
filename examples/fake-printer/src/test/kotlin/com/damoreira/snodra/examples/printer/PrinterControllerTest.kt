package com.damoreira.snodra.examples.printer

import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.assertj.MockMvcTester
import java.time.Duration
import kotlin.test.Test

@WebMvcTest(PrinterController::class)
@Import(PrinterControllerTest.Config::class)
class PrinterControllerTest {
    @TestConfiguration
    class Config {
        @Bean fun printer() = FakePrinter(
            clock = { java.time.Instant.parse("2026-11-03T10:00:00Z") },
            notifier = object : Notifier {
                override fun webhook(n: Notification, url: String?) = Unit
                override fun topic(n: Notification, topic: String?) = Unit
            },
            later = { _, _ -> },
            crash = {},
            replyHosts = setOf("service-c"),
            pollDelay = Duration.ofSeconds(5),
        )
    }

    @Autowired lateinit var mvc: MockMvcTester

    @Test fun `POST jobs accepts a job, saying when a key was already seen`() {
        fun post() = mvc.post().uri("/jobs").contentType(MediaType.APPLICATION_JSON).header("Idempotency-Key", "k1").content("""{"jobId":"g1","name":"poll-ada"}""")

        assertThat(post()).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g1","status":"ACCEPTED","duplicate":false}""")
        assertThat(post()).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g1","status":"ACCEPTED","duplicate":true}""")
    }

    @Test fun `POST jobs takes an optional reply target`() {
        val body = """{"jobId":"g3","name":"poll-c","replyWebhook":"http://service-c:8080/printer/webhook","replyTopic":"printer.banner.events"}"""
        assertThat(mvc.post().uri("/jobs").contentType(MediaType.APPLICATION_JSON).header("Idempotency-Key", "k3").content(body))
            .hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g3","status":"ACCEPTED","duplicate":false}""")
    }

    @Test fun `POST jobs with a reply webhook on a host the printer does not serve is 400`() {
        val body = """{"jobId":"g4","name":"webhook-c","replyWebhook":"http://169.254.169.254/latest/meta-data/"}"""
        assertThat(mvc.post().uri("/jobs").contentType(MediaType.APPLICATION_JSON).header("Idempotency-Key", "k4").content(body))
            .hasStatus(HttpStatus.BAD_REQUEST)
    }

    @Test fun `POST jobs without an idempotency key is 400`() {
        assertThat(mvc.post().uri("/jobs").contentType(MediaType.APPLICATION_JSON).content("""{"jobId":"g1","name":"poll-ada"}""")).hasStatus(HttpStatus.BAD_REQUEST)
    }

    @Test fun `GET jobs id reports the job, 404 for an unknown one`() {
        mvc.post().uri("/jobs").contentType(MediaType.APPLICATION_JSON).header("Idempotency-Key", "k2").content("""{"jobId":"g2","name":"poll-bob"}""").exchange()

        assertThat(mvc.get().uri("/jobs/g2")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g2","name":"poll-bob","status":"PENDING"}""")
        assertThat(mvc.get().uri("/jobs/none")).hasStatus(HttpStatus.NOT_FOUND)
    }
}
