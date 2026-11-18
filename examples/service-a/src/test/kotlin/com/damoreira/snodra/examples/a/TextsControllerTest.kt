package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Reverse
import com.damoreira.snodra.examples.api.Reversed
import com.damoreira.snodra.examples.api.Shout
import com.damoreira.snodra.examples.api.Shouted
import com.damoreira.snodra.examples.api.TextReversed
import com.damoreira.snodra.examples.api.TextShouted
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.test.InMemoryBuses
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
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertTrue

@WebMvcTest(TextsController::class)
@Import(TextsControllerTest.Buses::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TextsControllerTest {
    @TestConfiguration
    class Buses {
        @Bean fun inMemoryBuses() = InMemoryBuses(typePrefix = "service-a")
        @Bean fun textsListener() = TextsListener()
    }

    /** Stand-in for service-c. */
    class StubC {
        val commands = CopyOnWriteArrayList<Shout>()
        val queries = CopyOnWriteArrayList<Reverse>()
        @CommandHandler fun handle(c: Shout): Shouted { commands += c; return Shouted(c.text.uppercase()) }
        @QueryHandler fun handle(q: Reverse): Reversed { queries += q; return Reversed(q.text.reversed()) }
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses
    @Autowired lateinit var listener: TextsListener

    @Test fun `POST shouts returns the reply`() {
        buses.register(StubC())
        assertThat(mvc.post().uri("/shouts").contentType(MediaType.APPLICATION_JSON).content("""{"text":"ada"}"""))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"text":"ADA"}""")
    }

    @Test fun `GET reverse returns the reply`() {
        buses.register(StubC())
        assertThat(mvc.get().uri("/reverse/abc"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"text":"cba"}""")
    }

    @Test fun `blank text is 400 and nothing reaches the wire`() {
        val c = StubC()
        buses.register(c)
        assertThat(mvc.post().uri("/shouts").contentType(MediaType.APPLICATION_JSON).content("""{"text":"  "}""")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/reverse/{text}", " ")).hasStatus(HttpStatus.BAD_REQUEST) // a URI variable, so the space is encoded once
        assertTrue(c.commands.isEmpty())
        assertTrue(c.queries.isEmpty())
    }

    @Test fun `no handler maps to 503`() {
        assertThat(mvc.post().uri("/shouts").contentType(MediaType.APPLICATION_JSON).content("""{"text":"ada"}"""))
            .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
            .bodyJson().extractingPath("$.error").isEqualTo("NoHandlerException")
    }

    @Test fun `GET texts delivered lists both kinds`() {
        val ctx = MessageContext("m-1", "examples.TextShouted", "c-1", null, emptySet(), null, emptyMap())
        listener.on(TextShouted("ADA", Instant.EPOCH), ctx)
        listener.on(TextReversed("cba", Instant.EPOCH), ctx)
        assertThat(mvc.get().uri("/texts/delivered"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""[{"kind":"shouted","text":"ADA"},{"kind":"reversed","text":"cba"}]""")
    }
}
