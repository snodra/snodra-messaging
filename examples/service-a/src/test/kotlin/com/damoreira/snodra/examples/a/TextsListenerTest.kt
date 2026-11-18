package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.TextReversed
import com.damoreira.snodra.examples.api.TextShouted
import com.damoreira.snodra.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class TextsListenerTest {
    @Test fun `records both events in order`() = runBlocking {
        val listener = TextsListener()
        val buses = InMemoryBuses(typePrefix = "service-a").register(listener)

        buses.publish(TextShouted("ADA", Instant.EPOCH), "ada")
        buses.publish(TextReversed("cba", Instant.EPOCH), "abc")

        assertEquals(listOf(TextEvent("shouted", "ADA", Instant.EPOCH), TextEvent("reversed", "cba", Instant.EPOCH)), listener.delivered)
    }
}
