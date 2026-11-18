package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Row
import com.damoreira.snodra.examples.api.Rows
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.test.InMemoryBuses
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.web.servlet.assertj.MockMvcTester
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

@WebMvcTest(RowsController::class)
@Import(RowsControllerTest.Buses::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RowsControllerTest {
    @TestConfiguration
    class Buses {
        @Bean fun inMemoryBuses() = InMemoryBuses(typePrefix = "service-a")
    }

    /** Stand-in for the owners of the shared streaming type; counts the rows it produced. */
    class StubOwner {
        val produced = AtomicInteger()

        @QueryHandler(shared = true)
        fun handle(q: Rows): Flow<Row> = flow {
            for (n in 1..q.count) {
                if (q.failAt > 0 && n == q.failAt) throw IllegalStateException("Rows failed at row $n")
                produced.incrementAndGet()
                emit(Row("stub", "stub-1", n))
            }
        }
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses

    @Test fun `GET stream rows collects the rows in order with the count and the answering service`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/stream/rows?count=3&credit=2"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo(
                """{"rows":[{"service":"stub","instance":"stub-1","n":1},{"service":"stub","instance":"stub-1","n":2},
                            {"service":"stub","instance":"stub-1","n":3}],"count":3,"service":"stub"}""",
            )
    }

    @Test fun `an empty stream has no service`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/stream/rows?count=0"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"rows":[],"count":0,"service":null}""")
    }

    @Test fun `take cancels the stream after that many rows`() {
        val owner = StubOwner()
        buses.register(owner)
        assertThat(mvc.get().uri("/stream/rows?count=1000&take=5"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().extractingPath("$.count").isEqualTo(5)
        assertEquals(5, owner.produced.get(), "the handler's flow is cancelled with the collector")
    }

    @Test fun `maxResults ends the stream after that many rows`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/stream/rows?count=10&maxResults=4"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().extractingPath("$.count").isEqualTo(4)
    }

    @Test fun `a failing handler is 502 with its error`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/stream/rows?count=10&failAt=3"))
            .hasStatus(HttpStatus.BAD_GATEWAY)
            .bodyJson().isLenientlyEqualTo(
                """{"error":"RemoteHandlerException","message":"IllegalStateException: Rows failed at row 3"}""",
            )
    }

    @Test fun `bad parameters are 400`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/stream/rows?count=-1")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/stream/rows?credit=0")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/stream/rows?maxResults=-1")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/stream/rows?take=-1")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/stream/rows?failAt=-1")).hasStatus(HttpStatus.BAD_REQUEST)
    }

    @Test fun `no owner maps to 503`() {
        assertThat(mvc.get().uri("/stream/rows"))
            .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
            .bodyJson().extractingPath("$.error").isEqualTo("NoHandlerException")
    }
}
