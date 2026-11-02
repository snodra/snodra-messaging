package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.test.InMemoryBuses
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.servlet.assertj.MockMvcTester
import kotlin.test.Test
import kotlin.test.assertEquals

@WebMvcTest(CounterController::class)
@Import(CounterControllerTest.Config::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CounterControllerTest {
    @TestConfiguration
    class Config {
        @Bean fun buses() = InMemoryBuses(typePrefix = "service-b")
        @Bean fun counterHandler(buses: InMemoryBuses) = CounterHandler("service-b", buses.emitter)
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var handler: CounterHandler

    @Test fun `POST increment returns the new value of the counter`() {
        assertThat(mvc.post().uri("/counters/smoke/increment"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"service":"service-b","name":"smoke","value":1}""")
        assertThat(mvc.post().uri("/counters/smoke/increment"))
            .bodyJson().extractingPath("$.value").isEqualTo(2)
        assertEquals(3, handler.increment("smoke").value, "the controller increments the handler's own counter")
    }

    @Test fun `a blank name is 400`() {
        assertThat(mvc.post().uri("/counters/{name}/increment", " ")).hasStatus(HttpStatus.BAD_REQUEST)
    }
}
