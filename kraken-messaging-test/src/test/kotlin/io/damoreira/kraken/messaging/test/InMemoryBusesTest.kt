package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.handler.QueryHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
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
import kotlin.test.assertFailsWith

@MessageType("test.Ping")
data class Ping(val text: String) : Command<Pong>
data class Pong(val echoed: String, val at: Instant)
@MessageType("test.Happened")
data class Happened(val id: String) : Event

@MessageType("test.Rows")
data class Rows(val count: Int, val failAt: Int = -1) : StreamingQuery<Int>

class Handlers {
    val events = mutableListOf<Happened>()
    @QueryHandler fun rows(query: Rows): Flow<Int> = flow {
        repeat(query.count) { if (it == query.failAt) throw IllegalStateException("boom") else emit(it + 1) }
    }
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

    @Test fun `annotated handler is reachable with the configured user and roles`() = runBlocking {
        val buses = InMemoryBuses(userId = "alice", roles = setOf("kraken-admin")).register(AdminOnly())
        assertEquals("admin:alice", buses.send(Ping("x")).echoed)

        val anonymous = InMemoryBuses(userId = null).register(AdminOnly())
        assertFailsWith<io.damoreira.kraken.messaging.UnauthenticatedException> { anonymous.send(Ping("x")) }
        Unit
    }

    @Test fun `stream collects the streaming handler's flow, capped by maxResults, failing as a remote handler`() = runBlocking {
        val buses = InMemoryBuses().register(Handlers())
        assertEquals(listOf(1, 2, 3), buses.stream(Rows(3)).toList())
        assertEquals(listOf(1, 2), buses.stream(Rows(3), StreamOptions(maxResults = 2)).toList())
        val seen = mutableListOf<Int>()
        val e = assertFailsWith<RemoteHandlerException> { buses.stream(Rows(3, failAt = 1)).collect { seen += it } }
        assertEquals("IllegalStateException: boom", e.message)
        assertEquals(listOf(1), seen)
        assertFailsWith<NoHandlerException> { InMemoryBuses().stream(Rows(1)).toList() }
        Unit
    }
}

class AdminOnly {
    @io.damoreira.kraken.messaging.handler.CommandHandler
    @io.damoreira.kraken.messaging.handler.RequiresAuthentication("hasRole('kraken-admin')")
    fun handle(command: Ping, ctx: io.damoreira.kraken.messaging.MessageContext) =
        Pong("admin:${ctx.userId}", java.time.Instant.EPOCH)
}
