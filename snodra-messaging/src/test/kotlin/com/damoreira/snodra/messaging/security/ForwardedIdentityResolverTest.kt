package com.damoreira.snodra.messaging.security

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.messaging.UnauthenticatedException
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.Pong
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.hub.InboundRequestDispatcher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/** Records the caller seen through [CallerContext] while handling. */
class CallerRecordingHandlers {
    @Volatile var seenUserId: String? = "unset"
    @Volatile var seenRoles: Set<String>? = null
    @Volatile var seenToken: String? = null
    @CommandHandler fun handle(command: Ping): Pong {
        seenUserId = CallerContext.currentUserId()
        seenRoles = CallerContext.currentRoles()
        seenToken = CallerContext.currentBearerToken()
        return Pong(command.text, Instant.EPOCH)
    }
}

class ForwardedIdentityResolverTest {
    private val withDecoder = ForwardedIdentityResolver(TestJwt.decoder())
    private val withoutDecoder = ForwardedIdentityResolver(null)

    @Test fun `no header is anonymous`() {
        assertNull(withDecoder.resolve(null))
        assertNull(withDecoder.resolve(""))
        assertNull(withoutDecoder.resolve(null))
    }

    @Test fun `valid token with decoder yields verified identity with roles`() {
        val token = TestJwt.token("u-1", listOf("snodra-user"))

        val identity = assertNotNull(withDecoder.resolve("Bearer $token"))

        assertEquals("u-1", identity.userId)
        assertEquals(setOf("snodra-user"), identity.roles)
        assertTrue(identity.principal.verified)
        val auth = assertIs<SnodraForwardedAuthentication>(identity.authentication)
        assertEquals(token, auth.token)
    }

    @Test fun `invalid token with decoder is UnauthenticatedException`() {
        val expired = TestJwt.token("u-1", listOf("snodra-user"), expiresIn = (-5).minutes)

        val e = assertFailsWith<UnauthenticatedException> { withDecoder.resolve("Bearer $expired") }

        assertFalse(e.message!!.contains(expired))
    }

    @Test fun `a token without decoder is no identity - its claims are never read`() {
        val token = TestJwt.foreignToken("u-9", listOf("snodra-admin"))

        assertNull(withoutDecoder.resolve("Bearer $token"))
        assertNull(withoutDecoder.resolve("Bearer not-a-jwt"), "not read, so not refused either")
        assertNull(withoutDecoder.resolve("Bearer ${TestJwt.unsignedToken("u-9", listOf("snodra-admin"))}"))
    }

    @Test fun `malformed header is UnauthenticatedException`() {
        assertFailsWith<UnauthenticatedException> { withDecoder.resolve("Basic abc") }
        assertFailsWith<UnauthenticatedException> { withoutDecoder.resolve("Basic abc") }
    }

    private fun ping(authorization: String) = Request.newBuilder()
        .setId("req-1")
        .setKind(RequestKind.COMMAND)
        .setType("test.Ping")
        .setPayload(ByteString.copyFrom(JsonCodec().encode(Ping("x"))))
        .putHeaders("authorization", authorization)
        .build()

    @Test fun `without decoder the dispatcher runs the handler for an anonymous caller and still forwards the token`() = runBlocking {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val handlers = CallerRecordingHandlers()
        registry.register(handlers)
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), withoutDecoder, codec)
        val token = TestJwt.foreignToken("u-9", listOf("snodra-user"))

        val reply = dispatcher.dispatch(ping("Bearer $token"))

        assertTrue(reply.hasPayload(), "expected a payload, got ${reply.error.message}")
        assertNull(handlers.seenUserId, "no verified identity: anonymous")
        assertEquals(emptySet(), handlers.seenRoles)
        assertEquals(token, handlers.seenToken, "the token is forwarded on by nested sends")
    }

    @Test fun `with decoder the dispatcher installs the verified caller`() = runBlocking {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val handlers = CallerRecordingHandlers()
        registry.register(handlers)
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), withDecoder, codec)
        val token = TestJwt.token("u-1", listOf("snodra-user"))

        val reply = dispatcher.dispatch(ping("Bearer $token"))

        assertTrue(reply.hasPayload(), "expected a payload, got ${reply.error.message}")
        assertEquals("u-1", handlers.seenUserId)
        assertEquals(setOf("snodra-user"), handlers.seenRoles)
        assertEquals(token, handlers.seenToken)
    }
}
