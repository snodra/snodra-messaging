package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.KrakenForwardedAuthentication
import io.damoreira.kraken.messaging.security.MessagePrincipal
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
import io.mockk.every
import io.mockk.mockk
import jakarta.validation.ConstraintViolation
import jakarta.validation.ConstraintViolationException
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
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

class IllegalArgHandlers {
    @CommandHandler fun handle(command: Ping): Pong = throw IllegalArgumentException("bad input")
}

class ViolationHandlers {
    private fun violation(path: String, msg: String) = mockk<ConstraintViolation<Any>> {
        every { propertyPath.toString() } returns path
        every { message } returns msg
    }
    @CommandHandler fun handle(command: Ping): Pong =
        throw ConstraintViolationException("invalid", setOf(violation("text", "must not be blank"), violation("", "global problem"), violation("a", "too short")))
}

class FailingEventHandlers {
    val seen = mutableListOf<String>()
    val failure = IllegalStateException("first failed")
    @EventHandler fun a(event: Happened) { seen += "a"; throw failure }
    @EventHandler fun b(event: Happened) { seen += "b" }
}

class AuthHandlers {
    @CommandHandler @RequiresAuthentication("hasRole('kraken-admin')") fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

class DenyingHandlers {
    @CommandHandler fun handle(command: Ping): Pong = throw AccessDeniedException("nested send denied")
}

class SpringDenyingHandlers {
    @CommandHandler fun handle(command: Ping): Pong = throw org.springframework.security.access.AccessDeniedException("spring denied")
}

class HandlerInvokerTest {
    private val codec = JsonCodec()
    private val registry = HandlerRegistry(MessageTypeResolver("test"))
    private val invoker = HandlerInvoker(registry, codec)
    private fun ctx(type: String) = MessageContext("m1", type, "c1", "user-1", emptySet(), null, emptyMap())

    @Test fun `command success returns encoded response`() = runBlocking {
        registry.register(InvokerHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null)
        val success = assertIs<InvocationResult.Success>(result)
        assertEquals("x", codec.decode(success.payload, Pong::class).echoed)
    }

    @Test fun `query without handler is NO_HANDLER`() = runBlocking {
        val result = invoker.invokeQuery("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.NO_HANDLER, failure.code)
        assertEquals("no handler for type test.GetCount", failure.message)
    }

    @Test fun `bad payload is INVALID_REQUEST with type in message`() = runBlocking {
        registry.register(InvokerHandlers())
        val result = invoker.invokeCommand("test.Ping", "[1,2]".toByteArray(), ctx("test.Ping"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.INVALID_REQUEST, failure.code)
        assertEquals("Cannot decode payload as io.damoreira.kraken.messaging.fixtures.Ping", failure.message)
    }

    @Test fun `handler exception is HANDLER_ERROR without stack trace`() = runBlocking {
        registry.register(BoomHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.HANDLER_ERROR, failure.code)
        assertEquals("IllegalStateException: boom", failure.message)
    }

    @Test fun `unauthenticated exception maps to UNAUTHENTICATED`() = runBlocking {
        registry.register(UnauthHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.UNAUTHENTICATED, failure.code)
        assertEquals("token expired", failure.message)
    }

    @Test fun `event invokes all handlers`() = runBlocking {
        val handlers = InvokerHandlers()
        registry.register(handlers)
        invoker.invokeEvent("test.Happened", codec.encode(Happened("e1", Instant.EPOCH)), ctx("test.Happened"))
        assertEquals(listOf("first:e1", "second:e1"), handlers.seen)
    }

    @Test fun `event handler failure does not stop the others and is rethrown`() = runBlocking<Unit> {
        val handlers = FailingEventHandlers()
        registry.register(handlers)
        val thrown = assertFailsWith<IllegalStateException> {
            invoker.invokeEvent("test.Happened", codec.encode(Happened("e1", Instant.EPOCH)), ctx("test.Happened"))
        }
        assertSame(handlers.failure, thrown)
        assertEquals(listOf("a", "b"), handlers.seen)
    }

    @Test fun `illegal argument maps to HANDLER_ERROR with plain message and empty details`() = runBlocking {
        registry.register(IllegalArgHandlers())
        val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null))
        assertEquals(ErrorCode.HANDLER_ERROR, failure.code)
        assertEquals("bad input", failure.message)
        assertEquals("[]", String(failure.details))
    }

    @Test fun `constraint violations map to HANDLER_ERROR with details array`() = runBlocking {
        registry.register(ViolationHandlers())
        val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null))
        assertEquals(ErrorCode.HANDLER_ERROR, failure.code)
        assertEquals("invalid", failure.message)
        assertEquals(listOf("a: too short", "global problem", "text: must not be blank"), codec.mapper.readValue(failure.details, List::class.java))
    }

    private fun invokerFor(bean: Any): Pair<HandlerInvoker, JsonCodec> {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        registry.register(bean)
        return HandlerInvoker(registry, codec) to codec
    }

    private fun identity(vararg roles: String): Identity {
        val principal = MessagePrincipal("u-1", roles.toSet(), verified = true)
        return Identity(principal, KrakenForwardedAuthentication("tok", principal))
    }

    @Test fun `annotated handler without identity is UNAUTHENTICATED`() = runBlocking {
        val (invoker, codec) = invokerFor(AuthHandlers())
        val result = invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.UNAUTHENTICATED, failure.code)
        assertEquals("handler AuthHandlers.handle requires authentication", failure.message)
    }

    @Test fun `annotated handler with wrong role is PERMISSION_DENIED`() = runBlocking {
        val (invoker, codec) = invokerFor(AuthHandlers())
        val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), identity("kraken-user")))
        assertEquals(ErrorCode.PERMISSION_DENIED, failure.code)
        assertEquals("handler AuthHandlers.handle: access denied", failure.message)
    }

    @Test fun `annotated handler with matching role runs`() = runBlocking {
        val (invoker, codec) = invokerFor(AuthHandlers())
        assertIs<InvocationResult.Success>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), identity("kraken-admin")))
    }

    @Test fun `unannotated handler accepts anonymous`() = runBlocking {
        val (invoker, codec) = invokerFor(InvokerHandlers())
        assertIs<InvocationResult.Success>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null))
    }

    @Test fun `handler throwing AccessDeniedException maps to PERMISSION_DENIED`() = runBlocking {
        for (bean in listOf(DenyingHandlers(), SpringDenyingHandlers())) {
            val (invoker, codec) = invokerFor(bean)
            val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), identity("kraken-user")))
            assertEquals(ErrorCode.PERMISSION_DENIED, failure.code, bean::class.simpleName)
            assertTrue(failure.message.contains("denied"), failure.message)
        }
    }
}
