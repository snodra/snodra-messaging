package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.api.SubscriptionQuery
import io.damoreira.kraken.messaging.SubscriptionOptions
import io.damoreira.kraken.messaging.complete
import io.damoreira.kraken.messaging.completeExceptionally
import io.damoreira.kraken.messaging.emit
import kotlinx.coroutines.flow.take
import io.damoreira.kraken.messaging.handler.QueryHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.saga.RawOutcome
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
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

@MessageType("test.Watch")
data class Watch(val name: String) : SubscriptionQuery<Level, Changed>
data class Level(val name: String, val value: Int)
data class Changed(val name: String, val value: Int)

class WatchHandler(val failInitial: Boolean = false, val duringInitial: () -> Unit = {}) {
    @QueryHandler fun handle(q: Watch): Level {
        duringInitial()
        if (failInitial) throw IllegalStateException("no level")
        return Level(q.name, 0)
    }
}

class Handlers {
    val events = mutableListOf<Happened>()
    @QueryHandler fun rows(query: Rows): Flow<Int> = flow {
        repeat(query.count) { if (it == query.failAt) throw IllegalStateException("boom") else emit(it + 1) }
    }
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
    @EventHandler fun on(event: Happened) { events += event }
}

class InMemoryBusesTest {
    @Test
    fun `sendRaw runs a command by wire type name and reports an unknown type as NO_HANDLER`() = runBlocking {
        val buses = InMemoryBuses().register(Handlers())

        assertEquals(RawOutcome.Success, buses.sendRaw("test.Ping", """{"text":"hi"}""".toByteArray()))
        val unknown = buses.sendRaw("test.Nobody", "{}".toByteArray())

        assertEquals(SagaCommandFailed.Reason.NO_HANDLER, (unknown as RawOutcome.Permanent).reason)
    }

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

    @Test fun `subscribe answers the initial result from the handler and then the emitted updates in order`() = runBlocking {
        val buses = InMemoryBuses().register(WatchHandler())
        val result = buses.subscribe(Watch("a"))
        assertEquals(Level("a", 0), result.initial)
        assertEquals(1, buses.openSubscriptions)

        buses.emitter.emit<Watch, Changed>({ it.name == "a" }, Changed("a", 1))
        buses.emitter.emit<Watch, Changed>({ it.name == "a" }, Changed("a", 2))
        buses.emitter.complete<Watch> { it.name == "a" }

        assertEquals(listOf(Changed("a", 1), Changed("a", 2)), result.updates.toList())
        assertEquals(0, buses.openSubscriptions, "a completed subscription is removed")
    }

    @Test fun `a subscription is registered before its initial result is read, as in the library, so an emit meanwhile is delivered after it`() = runBlocking {
        lateinit var buses: InMemoryBuses
        buses = InMemoryBuses().register(WatchHandler(duringInitial = {
            buses.emitter.emit<Watch, Changed>({ it.name == "a" }, Changed("a", 1)) // between the registration and the end of the initial
        }))
        val result = buses.subscribe(Watch("a"))
        assertEquals(Level("a", 0), result.initial)
        buses.emitter.emit<Watch, Changed>({ it.name == "a" }, Changed("a", 2))
        buses.emitter.complete<Watch> { it.name == "a" }
        assertEquals(listOf(Changed("a", 1), Changed("a", 2)), result.updates.toList())

        // An initial that fails drops what was emitted meanwhile and leaves nothing open.
        lateinit var failing: InMemoryBuses
        failing = InMemoryBuses().register(WatchHandler(failInitial = true, duringInitial = {
            failing.emitter.emit<Watch, Changed>({ true }, Changed("a", 1))
        }))
        assertFailsWith<RemoteHandlerException> { failing.subscribe(Watch("a")) }
        assertEquals(0, failing.openSubscriptions)
        Unit
    }

    @Test fun `an update goes only to the subscriptions whose query passes the filter, and with none it is a no-op`() = runBlocking {
        val buses = InMemoryBuses().register(WatchHandler())
        buses.emitter.emit<Watch, Changed>({ true }, Changed("x", 9)) // nobody subscribed yet: nothing happens
        val a = buses.subscribe(Watch("a"))
        val b = buses.subscribe(Watch("b"))

        buses.emitter.emit<Watch, Changed>({ it.name == "b" }, Changed("b", 1))
        buses.emitter.emit<Watch, Changed>({ error("a broken filter skips its subscription") }, Changed("?", 0))
        buses.emitter.complete<Watch> { true }

        assertEquals(emptyList(), a.updates.toList())
        assertEquals(listOf(Changed("b", 1)), b.updates.toList())
    }

    @Test fun `completeExceptionally fails the updates as a remote handler error, after the updates before it`() = runBlocking {
        val buses = InMemoryBuses().register(WatchHandler())
        val result = buses.subscribe(Watch("a"))
        buses.emitter.emit<Watch, Changed>({ true }, Changed("a", 1))
        buses.emitter.completeExceptionally<Watch>({ true }, IllegalStateException("feed down"))

        val seen = mutableListOf<Changed>()
        val e = assertFailsWith<RemoteHandlerException> { result.updates.collect { seen += it } }
        assertEquals("IllegalStateException: feed down", e.message)
        assertEquals(listOf(Changed("a", 1)), seen)
        assertEquals(0, buses.openSubscriptions)
    }

    @Test fun `maxResults ends the subscription after that many updates`() = runBlocking {
        val buses = InMemoryBuses().register(WatchHandler())
        val result = buses.subscribe(Watch("a"), SubscriptionOptions(maxResults = 2))
        for (n in 1..5) buses.emitter.emit<Watch, Changed>({ true }, Changed("a", n))

        assertEquals(listOf(Changed("a", 1), Changed("a", 2)), result.updates.toList())
        assertEquals(0, buses.openSubscriptions)
    }

    @Test fun `cancel removes the subscription and completes the updates, and so does take(n)`() = runBlocking {
        val buses = InMemoryBuses().register(WatchHandler())
        val cancelled = buses.subscribe(Watch("a"))
        cancelled.cancel()
        buses.emitter.emit<Watch, Changed>({ true }, Changed("a", 2))
        assertEquals(0, buses.openSubscriptions)
        assertEquals(emptyList(), cancelled.updates.toList())

        val taken = buses.subscribe(Watch("a"))
        for (n in 1..3) buses.emitter.emit<Watch, Changed>({ true }, Changed("a", n))
        assertEquals(listOf(Changed("a", 1)), taken.updates.take(1).toList())
        assertEquals(0, buses.openSubscriptions, "the collector stopped: the subscription is cancelled")
    }

    @Test fun `subscribe fails like a unary query, with nothing left open`() = runBlocking {
        assertFailsWith<NoHandlerException> { InMemoryBuses().subscribe(Watch("a")) }

        val failing = InMemoryBuses().register(WatchHandler(failInitial = true))
        val e = assertFailsWith<RemoteHandlerException> { failing.subscribe(Watch("a")) }
        assertEquals("IllegalStateException: no level", e.message)
        assertEquals(0, failing.openSubscriptions, "a failed open leaves nothing behind")
    }
}

class AdminOnly {
    @io.damoreira.kraken.messaging.handler.CommandHandler
    @io.damoreira.kraken.messaging.handler.RequiresAuthentication("hasRole('kraken-admin')")
    fun handle(command: Ping, ctx: io.damoreira.kraken.messaging.MessageContext) =
        Pong("admin:${ctx.userId}", java.time.Instant.EPOCH)

}
