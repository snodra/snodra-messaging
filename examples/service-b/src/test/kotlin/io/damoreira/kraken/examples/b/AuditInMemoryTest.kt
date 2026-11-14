package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.test.InMemoryEvents
import io.damoreira.kraken.messaging.test.KrakenInMemory
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.assertj.MockMvcTester
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The zero-infrastructure twin of [AuditControllerTest] (ADR 0027, ADR 0028): the whole of service-b on the in-memory event
 * store and message store, no broker, no database, no hub. Where the slice test mocks the event bus and the dead letters, this
 * one runs the real `audit` processor: a note published over HTTP goes through the outbox and the event store to the processor,
 * a poison note is parked as a dead letter, and its retry through the admin API handles it.
 */
@SpringBootTest(properties = ["kraken.messaging.events.consumer.max-attempts=1"])
@AutoConfigureMockMvc
@KrakenInMemory
class AuditInMemoryTest {
    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var events: InMemoryEvents
    @Autowired lateinit var audit: AuditHandler
    @Autowired lateinit var notifier: AuditNotifier

    private fun until(what: String, done: () -> Boolean) {
        val deadline = System.nanoTime() + 10_000_000_000L
        while (!done()) {
            if (System.nanoTime() > deadline) throw AssertionError("$what within 10 s")
            Thread.sleep(20)
        }
    }

    @Test fun `a note posted over HTTP is published, then handled by the audit processor and its notifier`() {
        assertThat(mvc.post().uri("/audit/notes").contentType(MediaType.APPLICATION_JSON).content("""{"key":"k1","text":"hello"}"""))
            .hasStatus(HttpStatus.ACCEPTED)
        val note = events.awaitPublished(AuditNote::class).single { it.key == "k1" }
        assertEquals("hello", note.text)
        assertEquals("k1", events.published().single { it.type == "examples.AuditNote" && it.key == "k1" }.key, "the note's key is its order key")
        until("the note audited and notified") { note in audit.handled && note in notifier.notified }
        assertThat(mvc.get().uri("/audit/handled")).hasStatus(HttpStatus.OK)
            .bodyJson().extractingPath("$[?(@.key == 'k1')].text").asArray().containsExactly("hello")
    }

    @Test fun `a poison note is parked as a dead letter and handled once retried`() {
        assertThat(mvc.post().uri("/audit/failing?on=true")).hasStatus(HttpStatus.OK)
        try {
            val id = events.publish(AuditNote("k2", "poison-1", Instant.EPOCH), key = "k2")
            val letter = events.awaitDeadLetters("audit").single { it.eventId == id }
            assertEquals("k2", letter.key)
            assertThat(mvc.get().uri("/audit/dead-letters?processor=audit")).hasStatus(HttpStatus.OK)
                .bodyJson().extractingPath("$[0].reason").isEqualTo("HANDLER_FAILED")
        } finally {
            assertThat(mvc.post().uri("/audit/failing?on=false")).hasStatus(HttpStatus.OK)
        }
        val letter = events.deadLetters("audit").single()
        assertThat(mvc.post().uri("/audit/dead-letters/${letter.id}/retry")).bodyJson().isLenientlyEqualTo("""{"result":"Retried"}""")
        assertTrue(audit.handled.any { it.text == "poison-1" }, "handled by the retry")
        assertTrue(events.deadLetters("audit").isEmpty(), "the letter is gone")
    }
}
