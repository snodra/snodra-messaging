package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.DuplicateHandlerException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class AllKindsHandlers {
    @EventHandler fun on(event: Happened) {}
    @CommandHandler suspend fun handle(command: Ping, ctx: MessageContext): Pong = Pong(ctx.userId!!, Instant.EPOCH)
    @QueryHandler fun handle(query: GetCount): Count = Count(7)
}

class DuplicateCommandHandlers {
    @CommandHandler fun a(command: Ping): Pong = Pong("a", Instant.EPOCH)
    @CommandHandler fun b(command: Ping): Pong = Pong("b", Instant.EPOCH)
}

class TwoEventHandlers {
    @EventHandler fun first(event: Happened) {}
    @EventHandler fun second(event: Happened) {}
}

class WrongSecondParameter {
    @CommandHandler fun handle(command: Ping, other: String): Pong = Pong("x", Instant.EPOCH)
}

class ZeroParameters {
    @EventHandler fun on() {}
}

class Throwing {
    @CommandHandler fun handle(command: Ping): Pong = throw IllegalStateException("boom")
}

class AuthOnEvent {
    @EventHandler @RequiresAuthentication fun on(event: Happened) {}
}

class AuthOnCommand {
    @CommandHandler @RequiresAuthentication("hasRole('kraken-admin')") fun handle(command: Ping): Pong = Pong("x", Instant.EPOCH)
    @QueryHandler fun handle(query: GetCount): Count = Count(1)
}

class HandlerRegistryTest {
    private val registry = HandlerRegistry(MessageTypeResolver("test"))
    private val ctx = MessageContext("m1", "test.Ping", "c1", "alice", setOf("USER"), null, emptyMap())

    @Test fun `registers event command and query handlers by parameter type`() {
        val added = registry.register(AllKindsHandlers())
        assertEquals(3, added.size)
        assertEquals(setOf("test.Happened"), registry.eventTypes())
        assertEquals(setOf("test.Ping"), registry.commandTypes())
        assertEquals(setOf("test.GetCount"), registry.queryTypes())
        val command = assertNotNull(registry.commandHandler("test.Ping"))
        assertEquals(HandlerKind.COMMAND, command.kind)
        assertEquals(Ping::class, command.messageClass)
        assertTrue(command.wantsContext)
        assertEquals(1, registry.eventHandlers("test.Happened").size)
        assertEquals(false, assertNotNull(registry.queryHandler("test.GetCount")).wantsContext)
        assertNull(registry.commandHandler("test.Nope"))
    }

    @Test fun `duplicate command handler fails`() {
        val ex = assertThrows<DuplicateHandlerException> { registry.register(DuplicateCommandHandlers()) }
        assertTrue(ex.message!!.contains("DuplicateCommandHandlers.a"))
        assertTrue(ex.message!!.contains("DuplicateCommandHandlers.b"))
    }

    @Test fun `two event handlers for one type are allowed`() {
        registry.register(TwoEventHandlers())
        assertEquals(listOf("first", "second"), registry.eventHandlers("test.Happened").map { it.function.name })
    }

    @Test fun `wrong second parameter fails with method name in message`() {
        val ex = assertThrows<IllegalStateException> { registry.register(WrongSecondParameter()) }
        assertEquals("Handler WrongSecondParameter.handle must take (message) or (message, MessageContext)", ex.message)
    }

    @Test fun `zero parameter handler fails`() {
        val ex = assertThrows<IllegalStateException> { registry.register(ZeroParameters()) }
        assertEquals("Handler ZeroParameters.on must take (message) or (message, MessageContext)", ex.message)
    }

    @Test fun `invoke passes context when wanted and supports suspend`() = runBlocking {
        registry.register(AllKindsHandlers())
        val pong = registry.commandHandler("test.Ping")!!.invoke(Ping("hi"), ctx) as Pong
        assertEquals("alice", pong.echoed)
        assertEquals(Count(7), registry.queryHandler("test.GetCount")!!.invoke(GetCount("k"), ctx))
        assertSame(Unit, registry.eventHandlers("test.Happened").single().invoke(Happened("1", Instant.EPOCH), ctx))
    }

    @Test fun `invoke unwraps handler exception`() {
        registry.register(Throwing())
        val ex = assertThrows<IllegalStateException> {
            runBlocking { registry.commandHandler("test.Ping")!!.invoke(Ping("x"), ctx) }
        }
        assertEquals("boom", ex.message)
    }

    @Test fun `onChange fires after register`() {
        var fired = 0
        registry.onChange { fired++ }
        registry.register(AllKindsHandlers())
        assertEquals(1, fired)
        registry.register(Any())
        assertEquals(1, fired)
    }

    @Test fun `requires authentication on event handler fails registration`() {
        val e = assertThrows<IllegalStateException> { HandlerRegistry(MessageTypeResolver("test")).register(AuthOnEvent()) }
        assertTrue(e.message!!.contains("AuthOnEvent.on"), e.message)
        assertTrue(e.message!!.contains("@RequiresAuthentication is not supported on @EventHandler"), e.message)
    }

    @Test fun `handler method carries compiled authorization`() {
        val r = HandlerRegistry(MessageTypeResolver("test"))
        r.register(AuthOnCommand())
        assertEquals("hasRole('kraken-admin')", assertNotNull(r.commandHandler("test.Ping")).authorization?.source)
        assertNull(assertNotNull(r.queryHandler("test.GetCount")).authorization)
    }
}
