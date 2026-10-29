package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class InvokerHandlers {
    val seen = mutableListOf<String>()
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
    @EventHandler fun first(event: Happened) { seen += "first:${event.id}" }
    @EventHandler fun second(event: Happened) { seen += "second:${event.id}" }
}

class BoomHandlers {
    @CommandHandler fun handle(command: Ping): Pong = throw IllegalStateException("boom")
}

class UnauthHandlers {
    @CommandHandler fun handle(command: Ping): Pong = throw UnauthenticatedException("token expired")
}

class HandlerInvokerTest {
    private val codec = JsonCodec()
    private val registry = HandlerRegistry(MessageTypeResolver("test"))
    private val invoker = HandlerInvoker(registry, codec)
    private fun ctx(type: String) = MessageContext("m1", type, "c1", "user-1", emptySet(), null, emptyMap())

    @Test fun `command success returns encoded response`() = runBlocking {
        registry.register(InvokerHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"))
        val success = assertIs<InvocationResult.Success>(result)
        assertEquals("x", codec.decode(success.payload, Pong::class).echoed)
    }

    @Test fun `query without handler is NO_HANDLER`() = runBlocking {
        val result = invoker.invokeQuery("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"))
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.NO_HANDLER, failure.code)
        assertEquals("no handler for type test.GetCount", failure.message)
    }

    @Test fun `bad payload is INVALID_REQUEST with type in message`() = runBlocking {
        registry.register(InvokerHandlers())
        val result = invoker.invokeCommand("test.Ping", "[1,2]".toByteArray(), ctx("test.Ping"))
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.INVALID_REQUEST, failure.code)
        assertTrue(failure.message.contains("Ping"), failure.message)
    }

    @Test fun `handler exception is HANDLER_ERROR without stack trace`() = runBlocking {
        registry.register(BoomHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"))
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.HANDLER_ERROR, failure.code)
        assertEquals("IllegalStateException: boom", failure.message)
    }

    @Test fun `unauthenticated exception maps to UNAUTHENTICATED`() = runBlocking {
        registry.register(UnauthHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"))
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.UNAUTHENTICATED, failure.code)
        assertEquals("token expired", failure.message)
    }

    @Test fun `event invokes all handlers`() = runBlocking {
        val handlers = InvokerHandlers()
        registry.register(handlers)
        invoker.invokeEvent("test.Happened", codec.encode(Happened("e1", Instant.EPOCH)), ctx("test.Happened"))
        assertEquals(setOf("first:e1", "second:e1"), handlers.seen.toSet())
    }
}
