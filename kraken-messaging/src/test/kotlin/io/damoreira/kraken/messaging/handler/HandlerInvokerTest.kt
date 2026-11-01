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
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.Row
import io.damoreira.kraken.messaging.fixtures.Balance
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlin.test.assertNull
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

class SpringAuthFailingHandlers {
    @CommandHandler fun handle(command: Ping): Pong =
        throw org.springframework.security.authentication.AuthenticationCredentialsNotFoundException("no auth")
}

class RowsHandlers {
    @QueryHandler fun handle(query: ExportRows): Flow<Row> = flow {
        repeat(query.count) { n ->
            if (n == query.failAt) throw IllegalStateException("boom at $n")
            emit(Row(n + 1))
        }
    }
}

class AuthRowsHandlers {
    @QueryHandler @RequiresAuthentication("hasRole('kraken-admin')") fun handle(query: ExportRows): Flow<Row> = flow { emit(Row(1)) }
}

class WatchHandlers {
    @QueryHandler fun handle(query: WatchAccount): Balance = if (query.account < 0) throw IllegalStateException("no such account") else Balance(query.account * 10)
}

class AuthWatchHandlers {
    @QueryHandler @RequiresAuthentication("hasRole('kraken-admin')") fun handle(query: WatchAccount): Balance = Balance(1)
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
        Unit
    }

    @Test fun `unannotated handler accepts anonymous`() = runBlocking {
        val (invoker, codec) = invokerFor(InvokerHandlers())
        assertIs<InvocationResult.Success>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), null))
        Unit
    }

    @Test fun `handler throwing AccessDeniedException maps to PERMISSION_DENIED`() = runBlocking {
        for (bean in listOf(DenyingHandlers(), SpringDenyingHandlers())) {
            val (invoker, codec) = invokerFor(bean)
            val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), identity("kraken-user")))
            assertEquals(ErrorCode.PERMISSION_DENIED, failure.code, bean::class.simpleName)
            assertTrue(failure.message.contains("denied"), failure.message)
        }
    }

    @Test fun `anonymous call to annotated handler is UNAUTHENTICATED before payload decoding`() = runBlocking {
        val (invoker, _) = invokerFor(AuthHandlers())
        val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", "not json".toByteArray(), ctx("test.Ping"), null))
        assertEquals(ErrorCode.UNAUTHENTICATED, failure.code)
    }

    @Test fun `handler throwing Spring AuthenticationException maps to UNAUTHENTICATED`() = runBlocking {
        val (invoker, codec) = invokerFor(SpringAuthFailingHandlers())
        val failure = assertIs<InvocationResult.Failure>(invoker.invokeCommand("test.Ping", codec.encode(Ping("x")), ctx("test.Ping"), identity("kraken-user")))
        assertEquals(ErrorCode.UNAUTHENTICATED, failure.code)
        assertEquals("no auth", failure.message)
    }

    // --- streaming queries (P4b) ---

    @Test fun `invokeStream hands every encoded element to onPart and returns null at the end`() = runBlocking {
        registry.register(RowsHandlers())
        val parts = mutableListOf<String>()
        val failure = invoker.invokeStream("test.ExportRows", codec.encode(ExportRows(3)), ctx("test.ExportRows"), null) { parts += String(it) }
        assertNull(failure)
        assertEquals(listOf("{\"n\":1}", "{\"n\":2}", "{\"n\":3}"), parts)
    }

    @Test fun `a flow that fails maps like a handler exception, after the parts it emitted`() = runBlocking {
        registry.register(RowsHandlers())
        val parts = mutableListOf<String>()
        val failure = invoker.invokeStream("test.ExportRows", codec.encode(ExportRows(3, failAt = 1)), ctx("test.ExportRows"), null) { parts += String(it) }
        assertEquals(ErrorCode.HANDLER_ERROR to "IllegalStateException: boom at 1", assertIs<InvocationResult.Failure>(failure).let { it.code to it.message })
        assertEquals(listOf("{\"n\":1}"), parts)
    }

    @Test fun `invokeStream without a handler, with a unary handler, or with a bad payload fails before any part`() = runBlocking {
        registry.register(InvokerHandlers())
        registry.register(RowsHandlers())
        val none = invoker.invokeStream("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null) { error("no part") }
        assertEquals(ErrorCode.NO_HANDLER to "no handler for type test.GetCount", assertIs<InvocationResult.Failure>(none).let { it.code to it.message })
        val bad = invoker.invokeStream("test.ExportRows", "[1]".toByteArray(), ctx("test.ExportRows"), null) { error("no part") }
        assertEquals(ErrorCode.INVALID_REQUEST, assertIs<InvocationResult.Failure>(bad).code)
        registry.register(object { @QueryHandler fun handle(query: GetCount): Count = Count(1) })
        val unary = invoker.invokeStream("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null) { error("no part") }
        assertEquals(ErrorCode.INVALID_REQUEST, assertIs<InvocationResult.Failure>(unary).code)
        assertTrue(unary.message.contains("not a streaming query handler"), unary.message)
    }

    @Test fun `a unary request for a streaming query type is INVALID_REQUEST`() = runBlocking {
        registry.register(RowsHandlers())
        val result = invoker.invokeQuery("test.ExportRows", codec.encode(ExportRows(3)), ctx("test.ExportRows"), null)
        val failure = assertIs<InvocationResult.Failure>(result)
        assertEquals(ErrorCode.INVALID_REQUEST, failure.code)
        assertEquals("test.ExportRows is a streaming query: send it with QueryBus.stream (mode STREAMING)", failure.message)
    }

    @Test fun `invokeStream checks RequiresAuthentication before the flow starts`() = runBlocking {
        val (invoker, codec) = invokerFor(AuthRowsHandlers())
        val anonymous = invoker.invokeStream("test.ExportRows", codec.encode(ExportRows(1)), ctx("test.ExportRows"), null) { error("no part") }
        assertEquals(ErrorCode.UNAUTHENTICATED, assertIs<InvocationResult.Failure>(anonymous).code)
        val denied = invoker.invokeStream("test.ExportRows", codec.encode(ExportRows(1)), ctx("test.ExportRows"), identity("kraken-user")) { error("no part") }
        assertEquals(ErrorCode.PERMISSION_DENIED, assertIs<InvocationResult.Failure>(denied).code)
        val parts = mutableListOf<String>()
        assertNull(invoker.invokeStream("test.ExportRows", codec.encode(ExportRows(1)), ctx("test.ExportRows"), identity("kraken-admin")) { parts += String(it) })
        assertEquals(1, parts.size)
    }

    // --- subscription queries (P4c) ---

    @Test fun `a unary request for a subscription query type is INVALID_REQUEST, the initial request runs the handler`() = runBlocking {
        registry.register(WatchHandlers())
        val unary = assertIs<InvocationResult.Failure>(invoker.invokeQuery("test.WatchAccount", codec.encode(WatchAccount(7)), ctx("test.WatchAccount"), null))
        assertEquals(ErrorCode.INVALID_REQUEST, unary.code)
        assertEquals("test.WatchAccount is a subscription query: send it with QueryBus.subscribe", unary.message)
        val initial = assertIs<InvocationResult.Success>(invoker.invokeQuery("test.WatchAccount", codec.encode(WatchAccount(7)), ctx("test.WatchAccount"), null, subscriptionInitial = true))
        assertEquals(Balance(70), codec.decode(initial.payload, Balance::class))
        val failed = assertIs<InvocationResult.Failure>(invoker.invokeQuery("test.WatchAccount", codec.encode(WatchAccount(-1)), ctx("test.WatchAccount"), null, subscriptionInitial = true))
        assertEquals(ErrorCode.HANDLER_ERROR to "IllegalStateException: no such account", failed.code to failed.message)
        // The flag only lets a subscription handler answer: a plain query handler is invoked as before, a streaming one refused as before.
        registry.register(InvokerHandlers())
        registry.register(object { @QueryHandler fun handle(query: GetCount): Count = Count(1) })
        assertIs<InvocationResult.Success>(invoker.invokeQuery("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null, subscriptionInitial = true))
        registry.register(RowsHandlers())
        assertEquals(ErrorCode.INVALID_REQUEST, assertIs<InvocationResult.Failure>(invoker.invokeQuery("test.ExportRows", codec.encode(ExportRows(1)), ctx("test.ExportRows"), null, subscriptionInitial = true)).code)
    }

    @Test fun `admitSubscription decodes the query, and refuses a missing or non-subscription handler and a bad payload`() {
        registry.register(WatchHandlers())
        val accepted = assertIs<SubscriptionAdmission.Accepted>(invoker.admitSubscription("test.WatchAccount", codec.encode(WatchAccount(7)), ctx("test.WatchAccount"), null))
        assertEquals(WatchAccount(7), accepted.query)
        val bad = assertIs<SubscriptionAdmission.Rejected>(invoker.admitSubscription("test.WatchAccount", "[1]".toByteArray(), ctx("test.WatchAccount"), null))
        assertEquals(ErrorCode.INVALID_REQUEST, bad.failure.code)
        val none = assertIs<SubscriptionAdmission.Rejected>(invoker.admitSubscription("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null))
        assertEquals(ErrorCode.INVALID_REQUEST to "no subscription handler for test.GetCount on this instance", none.failure.code to none.failure.message)
        registry.register(object { @QueryHandler fun handle(query: GetCount): Count = Count(1) })
        val unary = assertIs<SubscriptionAdmission.Rejected>(invoker.admitSubscription("test.GetCount", codec.encode(GetCount("k")), ctx("test.GetCount"), null))
        assertEquals(ErrorCode.INVALID_REQUEST, unary.failure.code)
        assertTrue(unary.failure.message.contains("not a subscription query handler"), unary.failure.message)
    }

    @Test fun `admitSubscription checks RequiresAuthentication`() {
        val (invoker, codec) = invokerFor(AuthWatchHandlers())
        val anonymous = assertIs<SubscriptionAdmission.Rejected>(invoker.admitSubscription("test.WatchAccount", codec.encode(WatchAccount(1)), ctx("test.WatchAccount"), null))
        assertEquals(ErrorCode.UNAUTHENTICATED, anonymous.failure.code)
        val denied = assertIs<SubscriptionAdmission.Rejected>(invoker.admitSubscription("test.WatchAccount", codec.encode(WatchAccount(1)), ctx("test.WatchAccount"), identity("kraken-user")))
        assertEquals(ErrorCode.PERMISSION_DENIED, denied.failure.code)
        assertIs<SubscriptionAdmission.Accepted>(invoker.admitSubscription("test.WatchAccount", codec.encode(WatchAccount(1)), ctx("test.WatchAccount"), identity("kraken-admin")))
    }
}
