package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.GreetingDelivered
import com.damoreira.snodra.messaging.test.InMemoryBuses
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
