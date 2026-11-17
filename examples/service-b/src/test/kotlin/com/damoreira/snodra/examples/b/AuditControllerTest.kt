package com.damoreira.snodra.examples.b

import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetterError
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.deadletter.RetryAllResult
import com.damoreira.snodra.messaging.deadletter.RetryResult
import com.damoreira.snodra.messaging.test.InMemoryBuses
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
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.servlet.assertj.MockMvcTester
import java.time.Instant
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@WebMvcTest(AuditController::class)
@Import(AuditControllerTest.Config::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AuditControllerTest {
    @TestConfiguration
    class Config {
        @Bean fun buses() = InMemoryBuses(typePrefix = "service-b")
        @Bean fun audit() = AuditHandler()
        @Bean fun notifier() = AuditNotifier()
        @Bean fun deadLetters(): DeadLetters = mockk()
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses
    @Autowired lateinit var audit: AuditHandler
    @Autowired lateinit var notifier: AuditNotifier
    @Autowired lateinit var deadLetters: DeadLetters

    private fun letter(id: String, key: String?, reason: DeadLetterReason, blockedBy: String? = null) = DeadLetter(
        id = id, processor = "audit", source = "kraken.events", segment = "0", position = "7", key = key, eventId = "e-$id",
        eventType = "examples.AuditNote", headers = emptyMap(), payload = "{}", payloadBinary = null, reason = reason,
        error = DeadLetterError("IllegalStateException", "boom"), blockedBy = blockedBy,
        attempts = if (reason == DeadLetterReason.SEQUENCED) 0 else 5,
        parkedAt = Instant.EPOCH, lastAttemptAt = Instant.EPOCH, expireAt = Instant.EPOCH,
    )

    @BeforeTest fun reset() = clearMocks(deadLetters)

    @Test fun `POST notes publishes an AuditNote with the key as the Kafka key`() {
        assertThat(mvc.post().uri("/audit/notes").contentType(MediaType.APPLICATION_JSON).content("""{"key":"k1","text":"hello"}"""))
            .hasStatus(HttpStatus.ACCEPTED)
            .bodyJson().extractingPath("$.key").isEqualTo("k1")
        val published = buses.published.single()
        assertEquals("examples.AuditNote", published.type)
        assertEquals("k1", published.key)
        assertEquals("hello", (published.event as AuditNote).text)
    }

    @Test fun `a blank key is 400 and publishes nothing`() {
        assertThat(mvc.post().uri("/audit/notes").contentType(MediaType.APPLICATION_JSON).content("""{"key":" ","text":"x"}"""))
            .hasStatus(HttpStatus.BAD_REQUEST)
        assertTrue(buses.published.isEmpty())
    }

    @Test fun `POST failing switches the processor's failure on and off`() {
        assertThat(mvc.post().uri("/audit/failing?on=true")).hasStatus(HttpStatus.OK).bodyJson().extractingPath("$.failing").isEqualTo(true)
        assertTrue(audit.failing)
        assertThat(mvc.post().uri("/audit/failing?on=false")).bodyJson().extractingPath("$.failing").isEqualTo(false)
        assertTrue(!audit.failing)
    }

    @Test fun `GET handled lists the notes the processor handled`() {
        audit.on(AuditNote("k1", "first", Instant.EPOCH))
        assertThat(mvc.get().uri("/audit/handled")).hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""[{"key":"k1","text":"first"}]""")
    }

    @Test fun `GET dead-letters lists the letters of a processor as views`() {
        every { deadLetters.list("audit", 100) } returns listOf(
            letter("audit:kraken.events:0:7", "k1", DeadLetterReason.HANDLER_FAILED),
            letter("audit:kraken.events:0:8", "k1", DeadLetterReason.SEQUENCED, blockedBy = "audit:kraken.events:0:7"),
        )
        assertThat(mvc.get().uri("/audit/dead-letters?processor=audit")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo(
            """[{"id":"audit:kraken.events:0:7","key":"k1","reason":"HANDLER_FAILED","attempts":5,"error":"IllegalStateException: boom"},
                {"id":"audit:kraken.events:0:8","reason":"SEQUENCED","attempts":0,"blockedBy":"audit:kraken.events:0:7"}]""",
        )
    }

    @Test fun `GET dead-letters without a processor lists every processor`() {
        every { deadLetters.list(null, 100) } returns emptyList()
        assertThat(mvc.get().uri("/audit/dead-letters")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("[]")
    }

    @Test fun `GET count answers the number of letters`() {
        every { deadLetters.count("audit") } returns 2
        assertThat(mvc.get().uri("/audit/dead-letters/count?processor=audit")).bodyJson().isLenientlyEqualTo("""{"count":2}""")
    }

    @Test fun `retry answers each result by name, with the failure or the blocking letter as detail`() {
        every { deadLetters.retry("a") } returns RetryResult.Retried
        every { deadLetters.retry("b") } returns RetryResult.Failed("boom")
        every { deadLetters.retry("c") } returns RetryResult.Blocked("a")
        every { deadLetters.retry("d") } returns RetryResult.NotFound
        assertThat(mvc.post().uri("/audit/dead-letters/a/retry")).bodyJson().isLenientlyEqualTo("""{"result":"Retried"}""")
        assertThat(mvc.post().uri("/audit/dead-letters/b/retry")).bodyJson().isLenientlyEqualTo("""{"result":"Failed","detail":"boom"}""")
        assertThat(mvc.post().uri("/audit/dead-letters/c/retry")).bodyJson().isLenientlyEqualTo("""{"result":"Blocked","detail":"a"}""")
        assertThat(mvc.post().uri("/audit/dead-letters/d/retry")).bodyJson().isLenientlyEqualTo("""{"result":"NotFound"}""")
    }

    @Test fun `retry takes the letter id with its colons and dots`() {
        every { deadLetters.retry("audit:kraken.events:0:7") } returns RetryResult.Duplicate
        assertThat(mvc.post().uri("/audit/dead-letters/audit:kraken.events:0:7/retry")).bodyJson().isLenientlyEqualTo("""{"result":"Duplicate"}""")
    }

    @Test fun `retry-all runs for the audit processor by default`() {
        every { deadLetters.retryAll("audit") } returns RetryAllResult(2, 0, 0, 0)
        assertThat(mvc.post().uri("/audit/dead-letters/retry-all")).hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"retried":2,"failed":0,"blocked":0,"skipped":0}""")
    }

    @Test fun `discard is 200 for a letter that was there and 404 for one that was not`() {
        every { deadLetters.discard("a") } returns true
        every { deadLetters.discard("b") } returns false
        assertThat(mvc.delete().uri("/audit/dead-letters/a")).hasStatus(HttpStatus.OK)
        assertThat(mvc.delete().uri("/audit/dead-letters/b")).hasStatus(HttpStatus.NOT_FOUND)
        verify(exactly = 1) { deadLetters.discard("a") }
    }

    @Test fun `a processor this instance does not run is 400 and a mode without letters is 409`() {
        every { deadLetters.list("nope", 100) } throws IllegalArgumentException("processor nope does not run in this instance")
        every { deadLetters.count(null) } throws IllegalStateException("dead letters are not kept in mode log")
        assertThat(mvc.get().uri("/audit/dead-letters?processor=nope")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/audit/dead-letters/count")).hasStatus(HttpStatus.CONFLICT)
    }

    @Test fun `GET resets lists the resets of the audit processor and GET notified what the notifier sent once (P6b)`() {
        audit.onReset(
            com.damoreira.snodra.messaging.control.ResetContext(
                "audit", "op-1", com.damoreira.snodra.messaging.eventstore.ResetTarget.Earliest,
                listOf(com.damoreira.snodra.messaging.eventstore.ReplayRange("kraken.events", "0", "0", "3")),
            ),
        )
        assertThat(mvc.get().uri("/audit/resets")).hasStatus(HttpStatus.OK).bodyJson()
            .isLenientlyEqualTo("""[{"operationId":"op-1","target":"earliest","ranges":[{"partition":0,"from":0,"until":3}]}]""")
        notifier.on(AuditNote("k1", "first", Instant.EPOCH))
        assertThat(mvc.get().uri("/audit/notified")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""[{"key":"k1","text":"first"}]""")
    }
}
