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

    @Test fun `a well-formed token without decoder is no identity - its claims are never presented`() {
        assertNull(withoutDecoder.resolve("Bearer ${TestJwt.foreignToken("u-9", listOf("snodra-admin"))}"))
        // Signed by anyone, even expired: without the keys neither the signature nor the expiry can be judged.
        assertNull(withoutDecoder.resolve("Bearer ${TestJwt.token("u-1", listOf("snodra-user"), expiresIn = (-5).minutes)}"))
    }

    private fun b64(json: String) = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(json.toByteArray())

    @Test fun `without decoder what is not a signed JWS is UNAUTHENTICATED, as with one`() {
        val header = b64("""{"alg":"RS256","typ":"JWT"}""")
        val claims = b64("""{"sub":"u-9"}""")
        val refused = mapOf(
            "not-a-jwt" to "unreadable token",
            "$header.$claims" to "unreadable token", // two segments
            "$header.$claims.sig.extra" to "unreadable token", // four segments
            "a.b.c.d.e" to "unsupported token", // five segments: an encrypted token (JWE)
            "%%%.$claims.sig" to "unreadable token", // a header that is not base64url
            "${b64("[1]")}.$claims.sig" to "unreadable token", // a header that is not a JSON object
            "${b64("""{"alg":1}""")}.$claims.sig" to "unreadable token", // an alg that is not a string
            "$header.${b64("42")}.sig" to "unreadable token", // claims that are not a JSON object
            "${b64("""{"alg":"none"}""")}.$claims.sig" to "unsigned token",
            "${b64("""{"alg":"NONE"}""")}.$claims." to "unsigned token",
            "$header.$claims." to "unsigned token", // an empty signature
            TestJwt.unsignedToken("u-9", listOf("snodra-admin")) to "unsigned token",
        )
        for ((token, message) in refused) {
            val e = assertFailsWith<UnauthenticatedException>(token) { withoutDecoder.resolve("Bearer $token") }
            assertEquals(message, e.message, token)
        }
        // Padding is tolerated, as base64url decoders do.
        assertNull(withoutDecoder.resolve("Bearer ${java.util.Base64.getUrlEncoder().encodeToString("""{"alg":"RS256"}""".toByteArray())}.$claims.sig"))
    }

    @Test fun `without decoder a bad token is UNAUTHENTICATED before the handler lookup, also for a type nobody handles`() = runBlocking {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), withoutDecoder, codec)

        val reply = dispatcher.dispatch(ping("Bearer not-a-jwt").toBuilder().setType("test.Nobody").build())

        assertEquals(com.damoreira.snodra.server.v1.ErrorCode.UNAUTHENTICATED, reply.error.code)
        assertEquals("unreadable token", reply.error.message)
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
