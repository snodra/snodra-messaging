package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.EventHandler
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals

@MessageType("test.Ping")
data class Ping(val text: String) : Command<Pong>
data class Pong(val echoed: String, val at: Instant)
@MessageType("test.Happened")
data class Happened(val id: String) : Event

class Handlers {
    val events = mutableListOf<Happened>()
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
    @EventHandler fun on(event: Happened) { events += event }
}

class InMemoryBusesTest {
    @Test fun `send round trips through local handler with typed response`() = runBlocking {
        val buses = InMemoryBuses().register(Handlers())
        val pong: Pong = buses.send(Ping("x"))
        assertEquals("x", pong.echoed)
    }

    @Test fun `publish dispatches to event handlers and records`() = runBlocking {
        val handlers = Handlers()
        val buses = InMemoryBuses().register(handlers)
        val event = Happened("e1")
        buses.publish(event, "k1")
        assertEquals(listOf(event), handlers.events)
        assertEquals(listOf(InMemoryBuses.PublishedEvent("test.Happened", "k1", event)), buses.published)
    }

    @Test fun `no handler surfaces NoHandlerException`() = runBlocking<Unit> {
        val buses = InMemoryBuses()
        assertThrows<NoHandlerException> { buses.send(Ping("x")) }
    }
}
