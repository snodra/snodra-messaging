package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.CountGreetings
import io.damoreira.kraken.examples.api.GreetingAccepted
import io.damoreira.kraken.examples.api.GreetingCount
import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.test.InMemoryBuses
import io.damoreira.kraken.examples.api.SendGreeting
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.servlet.assertj.MockMvcTester
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@WebMvcTest(GreetingsController::class)
@Import(GreetingsControllerTest.Buses::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // InMemoryBuses has no unregister; each test gets its own
class GreetingsControllerTest {
    @TestConfiguration
    class Buses {
        @Bean fun inMemoryBuses() = InMemoryBuses(typePrefix = "service-a")
        @Bean fun greetingsListener() = GreetingsListener()
    }

    /** Stand-in for service-b: owns the command and the query, records every command it saw. */
    class StubOwner {
        val commands = CopyOnWriteArrayList<SendGreeting>()
        @CommandHandler fun handle(c: SendGreeting): GreetingAccepted { commands += c; return GreetingAccepted("id-1", c.name) }
        @QueryHandler fun handle(q: CountGreetings): GreetingCount = GreetingCount(q.name, 2)
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses
    @Autowired lateinit var listener: GreetingsListener

    @Test fun `POST greetings sends the command and returns the reply`() {
        buses.register(StubOwner())
        assertThat(mvc.post().uri("/greetings").contentType(MediaType.APPLICATION_JSON).content("""{"name":"Ada"}"""))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"id":"id-1","name":"Ada"}""")
    }

    @Test fun `GET count sends the query and returns the reply`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/greetings/Ada/count"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"name":"Ada","count":2}""")
    }

    @Test fun `no handler maps to 503`() {
        assertThat(mvc.post().uri("/greetings").contentType(MediaType.APPLICATION_JSON).content("""{"name":"Ada"}"""))
            .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
            .bodyJson().extractingPath("$.error").isEqualTo("NoHandlerException")
    }

    @Test fun `blank name is 400 and nothing reaches the wire`() {
        val owner = StubOwner()
        buses.register(owner)
        assertThat(mvc.post().uri("/greetings").contentType(MediaType.APPLICATION_JSON).content("""{"name":"  "}"""))
            .hasStatus(HttpStatus.BAD_REQUEST)
        assertTrue(owner.commands.isEmpty())
        assertTrue(buses.published.isEmpty())
    }

    @Test fun `GET delivered lists the events the listener received`() {
        val event = GreetingDelivered("id-9", "Ada", Instant.EPOCH)
        listener.on(event, MessageContext("m-1", "examples.GreetingDelivered", "c-1", null, emptySet(), null, emptyMap()))
        assertThat(mvc.get().uri("/greetings/delivered"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().extractingPath("$[0].id").isEqualTo("id-9")
        assertEquals(listOf(event), listener.delivered)
    }
}
