package io.damoreira.kraken.messaging.security

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.InboundRequestDispatcher
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

class PingHandlers {
    @Volatile var calls = 0
    @CommandHandler fun handle(command: Ping): Pong { calls++; return Pong(command.text, Instant.EPOCH) }
}

class ForwardedJwtAuthenticatorTest {
    private val authenticator = ForwardedJwtAuthenticator(TestJwt.decoder(), requireJwt = true)

    @Test fun `valid token yields subject and realm roles`() {
        val principal = authenticator.authenticate("Bearer " + TestJwt.token("user-42", listOf("member", "admin")))
        assertEquals("user-42", principal.userId)
        assertEquals(setOf("member", "admin"), principal.roles)
    }

    @Test fun `expired token is UnauthenticatedException`() {
        val expired = TestJwt.token("user-42", listOf("member"), expiresIn = (-10).minutes)
        val e = assertFailsWith<UnauthenticatedException> { authenticator.authenticate("Bearer $expired") }
        assertTrue(e.message!!.contains("expired", ignoreCase = true), e.message)
    }

    @Test fun `missing header with require-jwt is UnauthenticatedException`() {
        val e = assertFailsWith<UnauthenticatedException> { authenticator.authenticate(null) }
        assertEquals("missing bearer token", e.message)
    }

    @Test fun `missing header without require-jwt is anonymous`() {
        val principal = ForwardedJwtAuthenticator(TestJwt.decoder(), requireJwt = false).authenticate(null)
        assertNull(principal.userId)
        assertEquals(emptySet(), principal.roles)
    }

    @Test fun `token without decoder and require-jwt false is anonymous`() {
        val principal = ForwardedJwtAuthenticator(null, requireJwt = false)
            .authenticate("Bearer " + TestJwt.token("user-42", listOf("member")))
        assertNull(principal.userId)
        assertEquals(emptySet(), principal.roles)
        assertFalse(principal.verified)
    }

    @Test fun `token without decoder and require-jwt true is UnauthenticatedException`() {
        assertFailsWith<UnauthenticatedException> {
            ForwardedJwtAuthenticator(null, requireJwt = true).authenticate("Bearer " + TestJwt.token("user-42", listOf("member")))
        }
    }

    @Test fun `dispatcher answers UNAUTHENTICATED reply for bad token`() = runBlocking {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val handlers = PingHandlers()
        registry.register(handlers)
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), authenticator, codec)
        val request = Request.newBuilder()
            .setId("req-1")
            .setKind(RequestKind.COMMAND)
            .setType("test.Ping")
            .setPayload(ByteString.copyFrom(codec.encode(Ping("x"))))
            .putHeaders("authorization", "Bearer not-a-jwt")
            .build()

        val reply = dispatcher.dispatch(request)

        assertEquals("req-1", reply.requestId)
        assertTrue(reply.hasError())
        assertEquals(ErrorCode.UNAUTHENTICATED, reply.error.code)
        assertFalse(reply.error.message.contains("not-a-jwt"))
        assertEquals(0, handlers.calls)
    }
}
