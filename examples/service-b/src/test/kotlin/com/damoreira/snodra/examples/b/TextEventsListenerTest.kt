package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.TextReversed
import com.damoreira.snodra.examples.api.TextShouted
import com.damoreira.snodra.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class TextEventsListenerTest {
    @Test fun `logs and counts both events`() = runBlocking {
        val listener = TextEventsListener()
        val buses = InMemoryBuses(typePrefix = "service-b").register(listener)

        buses.publish(TextShouted("ADA", Instant.EPOCH), "ada")
        buses.publish(TextReversed("cba", Instant.EPOCH), "abc")
        buses.publish(TextShouted("BOB", Instant.EPOCH), "bob")

        assertEquals(3, listener.received)
    }
}
