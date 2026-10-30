package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.TextReversed
import io.damoreira.kraken.examples.api.TextShouted
import io.damoreira.kraken.messaging.test.InMemoryBuses
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
