package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.CountGreetings
import io.damoreira.kraken.examples.api.GreetingCount
import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.examples.api.SendGreeting
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class GreetingHandlersTest {
    private fun buses() = InMemoryBuses(typePrefix = "service-b")

    @Test fun `SendGreeting is accepted with a fresh id and publishes GreetingDelivered keyed by name`() = runBlocking {
        val buses = buses()
        buses.register(GreetingHandlers(buses))

        val accepted = buses.send(SendGreeting("Ada"))

        assertEquals("Ada", accepted.name)
        assertTrue(accepted.id.isNotBlank())
        val published = buses.published.single()
        assertEquals("examples.GreetingDelivered", published.type) // explicit wire type, not "service-b.GreetingDelivered"
        assertEquals("Ada", published.key)
        val event = assertIs<GreetingDelivered>(published.event)
        assertEquals(accepted.id, event.id)
        assertEquals("Ada", event.name)
    }

    @Test fun `CountGreetings counts accepted greetings per name`() = runBlocking {
        val buses = buses()
        buses.register(GreetingHandlers(buses))
        buses.send(SendGreeting("Ada"))
        buses.send(SendGreeting("Ada"))
        buses.send(SendGreeting("Bob"))

        assertEquals(GreetingCount("Ada", 2), buses.send(CountGreetings("Ada")))
        assertEquals(GreetingCount("Bob", 1), buses.send(CountGreetings("Bob")))
    }

    @Test fun `a configured count delay slows the query down`() = runBlocking {
        val buses = buses()
        buses.register(GreetingHandlers(buses, java.time.Duration.ofMillis(150)))
        val started = System.nanoTime()
        assertEquals(GreetingCount("Ada", 0), buses.send(CountGreetings("Ada")))
        assertTrue(System.nanoTime() - started >= 150_000_000, "the delay was not applied")
    }

    @Test fun `count for unknown name is 0`() = runBlocking {
        val buses = buses()
        buses.register(GreetingHandlers(buses))

        assertEquals(GreetingCount("Nobody", 0), buses.send(CountGreetings("Nobody")))
    }
}
