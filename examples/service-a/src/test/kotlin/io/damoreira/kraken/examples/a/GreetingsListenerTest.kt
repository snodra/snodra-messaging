package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class GreetingsListenerTest {
    @Test fun `records delivered greetings`() = runBlocking {
        val listener = GreetingsListener()
        val buses = InMemoryBuses(typePrefix = "service-a").register(listener)
        val event = GreetingDelivered("id-1", "Ada", Instant.EPOCH)

        buses.publish(event, "Ada")

        assertEquals(listOf(event), listener.delivered)
    }
}
