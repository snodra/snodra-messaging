package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.Inventory
import io.damoreira.kraken.examples.api.InventoryLevel
import io.damoreira.kraken.messaging.handler.QueryHandler
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

@WebMvcTest(InventoryController::class)
@Import(InventoryControllerTest.Buses::class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class InventoryControllerTest {
    @TestConfiguration
    class Buses {
        @Bean fun inMemoryBuses() = InMemoryBuses(typePrefix = "service-a")
    }

    /** Stand-in for the owners of the shared type. */
    class StubOwner(private val fail: Boolean = false) {
        @QueryHandler(shared = true)
        fun handle(q: Inventory): InventoryLevel {
            if (fail) error("out of stock")
            return InventoryLevel("stub", "stub-1", q.item, 3, 1)
        }
    }

    @Autowired lateinit var mvc: MockMvcTester
    @Autowired lateinit var buses: InMemoryBuses

    @Test fun `GET inventory returns the one answer`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/inventory?item=bolt"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"service":"stub","instance":"stub-1","item":"bolt","count":3,"answered":1}""")
    }

    @Test fun `GET scatter inventory returns every part with the counts`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/scatter/inventory"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo(
                """{"parts":[{"service":"in-memory","instance":"in-memory","result":{"service":"stub","item":"widget","count":3},"error":null}],
                   "failed":0,"timedOut":0}""",
            )
    }

    @Test fun `a failed part carries its code and message`() {
        buses.register(StubOwner(fail = true))
        assertThat(mvc.get().uri("/scatter/inventory"))
            .hasStatus(HttpStatus.OK)
            .bodyJson().isLenientlyEqualTo("""{"parts":[{"result":null,"error":{"code":"HANDLER_ERROR"}}],"failed":1,"timedOut":0}""")
    }

    @Test fun `bad parameters are 400`() {
        buses.register(StubOwner())
        assertThat(mvc.get().uri("/inventory?item= ")).hasStatus(HttpStatus.BAD_REQUEST)
        assertThat(mvc.get().uri("/scatter/inventory?maxResults=-1")).hasStatus(HttpStatus.BAD_REQUEST)
    }

    @Test fun `no owner maps to 503`() {
        assertThat(mvc.get().uri("/scatter/inventory"))
            .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
            .bodyJson().extractingPath("$.error").isEqualTo("NoHandlerException")
    }
}
