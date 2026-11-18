package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.PostcardFailed
import com.damoreira.snodra.examples.api.PostcardPrinted
import com.damoreira.snodra.messaging.test.InMemoryBuses
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.test.web.servlet.assertj.MockMvcTester
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class PostcardOutcomesTest {
    private val outcomes = PostcardOutcomes()

    @Test fun `an unknown job has no outcome`() {
        assertNull(outcomes.of("nope"))
    }

    @Test fun `PostcardPrinted records the job as printed`() {
        outcomes.on(PostcardPrinted("g1", "webhook-ada"))

        assertEquals(PostcardOutcome("g1", "PRINTED", null), outcomes.of("g1"))
    }

    @Test fun `PostcardFailed records the job as failed with its reason`() {
        outcomes.on(PostcardFailed("g2", "silent-ada", "TIMEOUT"))

        assertEquals(PostcardOutcome("g2", "FAILED", "TIMEOUT"), outcomes.of("g2"))
    }

    @Test fun `both are event handlers the library will find`() {
        val buses = InMemoryBuses(typePrefix = "service-b")
        buses.register(outcomes)
        assertEquals(setOf("examples.PostcardPrinted", "examples.PostcardFailed"), buses.registry.eventTypes())
    }
}

@WebMvcTest(PostcardController::class)
@Import(PostcardControllerTest.Config::class)
class PostcardControllerTest {
    @TestConfiguration
    class Config {
        @Bean fun outcomes() = PostcardOutcomes()
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var outcomes: PostcardOutcomes

    @Test fun `GET postcards of a finished job returns its outcome`() {
        outcomes.on(PostcardPrinted("g1", "webhook-ada"))
        outcomes.on(PostcardFailed("g2", "silent-ada", "TIMEOUT"))

        assertThat(mvc.get().uri("/postcards/g1")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g1","status":"PRINTED"}""")
        assertThat(mvc.get().uri("/postcards/g2")).hasStatus(HttpStatus.OK).bodyJson().isLenientlyEqualTo("""{"jobId":"g2","status":"FAILED","reason":"TIMEOUT"}""")
    }

    @Test fun `GET postcards of a job still in flight is 404`() {
        assertThat(mvc.get().uri("/postcards/g3")).hasStatus(HttpStatus.NOT_FOUND)
    }
}
