package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Counter
import com.damoreira.snodra.examples.api.CounterChanged
import com.damoreira.snodra.examples.api.IncrementCounter
import com.damoreira.snodra.examples.api.OpenSubscriptions
import com.damoreira.snodra.examples.api.SubscriptionCount
import com.damoreira.snodra.examples.api.WatchCounter
import com.damoreira.snodra.messaging.emit
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.test.InMemoryBuses
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.InstanceOfAssertFactories
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
@Import(CounterControllerTest.Buses::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CounterControllerTest {
    @TestConfiguration
    class Buses {
        @Bean fun inMemoryBuses() = InMemoryBuses(typePrefix = "service-a")
    }

    /** Stand-in for the owners: the initial value is [start]; the handler pushes [pushed] updates right after it opened the subscription. */
    class StubOwner(private val buses: InMemoryBuses, private val start: Long = 0, private val pushed: Int = 0) {
        @QueryHandler(shared = true)
        fun handle(q: WatchCounter): Counter {
            for (n in 1..pushed) {
                buses.emitter.emit<WatchCounter, CounterChanged>({ it.name == q.name }, CounterChanged("stub", "stub-1", q.name, start + n))
            }
            return Counter("stub", "stub-1", q.name, start)
        }

        @QueryHandler(shared = true)
        fun handle(q: OpenSubscriptions) = SubscriptionCount("stub", "stub-1", 2)

        @CommandHandler
        fun handle(c: IncrementCounter) = Counter(c.service, "stub-1", c.name, 1)
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses

    @Test fun `GET subscribe counter returns the initial result and the requested number of updates, then cancels`() {
        buses.register(StubOwner(buses, start = 7, pushed = 5))
        assertThat(mvc.get().uri("/subscribe/counter?name=x&updates=3"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo(
                """{"initial":{"service":"stub","instance":"stub-1","name":"x","value":7},
                    "updates":[{"service":"stub","instance":"stub-1","name":"x","value":8},
                               {"service":"stub","instance":"stub-1","name":"x","value":9},
                               {"service":"stub","instance":"stub-1","name":"x","value":10}]}""",
            )
        assertEquals(0, buses.openSubscriptions, "the subscription is cancelled once the updates were collected")
    }

    @Test fun `updates=0 returns only the initial result`() {
        buses.register(StubOwner(buses))
        assertThat(mvc.get().uri("/subscribe/counter?name=x&updates=0"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"initial":{"name":"x","value":0},"updates":[]}""")
        assertEquals(0, buses.openSubscriptions)
    }

    @Test fun `the timeout ends the collection with the updates that arrived`() {
        buses.register(StubOwner(buses, pushed = 2))
        assertThat(mvc.get().uri("/subscribe/counter?name=x&updates=5&timeoutMs=300"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().extractingPath("$.updates").asInstanceOf(InstanceOfAssertFactories.LIST).hasSize(2)
        assertEquals(0, buses.openSubscriptions, "cancelled after the timeout too")
    }

    @Test fun `a counter nobody knows still answers its initial result`() {
        buses.register(StubOwner(buses))
        assertThat(mvc.get().uri("/subscribe/counter?name=unknown&updates=0"))
            .bodyJson().extractingPath("$.initial.value").isEqualTo(0)
    }

    @Test fun `bad parameters are 400`() {
        buses.register(StubOwner(buses))
        assertThat(mvc.get().uri("/subscribe/counter?name={n}", " ")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/subscribe/counter?name=x&updates=-1")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/subscribe/counter?name=x&timeoutMs=0")).hasStatus(HttpStatus.BAD_REQUEST)
    }

    @Test fun `no owner maps to 503`() {
        assertThat(mvc.get().uri("/subscribe/counter?name=x"))
            .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
            .bodyJson().extractingPath("$.error").isEqualTo("NoHandlerException")
    }

    @Test fun `POST increment sends the command to the service and returns its counter`() {
        buses.register(StubOwner(buses))
        assertThat(mvc.post().uri("/counters/smoke/increment"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"service":"service-c","name":"smoke","value":1}""")
        assertThat(mvc.post().uri("/counters/smoke/increment?service=service-b"))
            .bodyJson().extractingPath("$.service").isEqualTo("service-b")
    }

    @Test fun `GET open subscriptions scatter-gathers the owners' counts`() {
        buses.register(StubOwner(buses))
        assertThat(mvc.get().uri("/subscribe/open"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo(
                """{"instances":[{"service":"stub","instance":"stub-1","open":2}],"failed":0}""",
            )
    }
}
