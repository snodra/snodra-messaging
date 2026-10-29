package io.damoreira.kraken.messaging.security

import com.google.protobuf.ByteString
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
    @CommandHandler fun handle(command: Ping): Pong {
        seenUserId = CallerContext.currentUserId()
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
        val token = TestJwt.token("u-1", listOf("kraken-user"))

        val identity = assertNotNull(withDecoder.resolve("Bearer $token"))

        assertEquals("u-1", identity.userId)
        assertEquals(setOf("kraken-user"), identity.roles)
        assertTrue(identity.principal.verified)
        val auth = assertIs<KrakenForwardedAuthentication>(identity.authentication)
        assertEquals(token, auth.token)
    }

    @Test fun `invalid token with decoder is UnauthenticatedException`() {
        val expired = TestJwt.token("u-1", listOf("kraken-user"), expiresIn = (-5).minutes)

        val e = assertFailsWith<UnauthenticatedException> { withDecoder.resolve("Bearer $expired") }

        assertFalse(e.message!!.contains(expired))
    }

    @Test fun `token without decoder is parsed unverified`() {
        val token = TestJwt.foreignToken("u-9", listOf("kraken-admin"))

        val identity = assertNotNull(withoutDecoder.resolve("Bearer $token"))

        assertEquals("u-9", identity.userId)
        assertEquals(setOf("kraken-admin"), identity.roles)
        assertFalse(identity.principal.verified)
        assertEquals(token, assertIs<KrakenForwardedAuthentication>(identity.authentication).token)
    }

    @Test fun `unreadable token without decoder is UnauthenticatedException`() {
        val e = assertFailsWith<UnauthenticatedException> { withoutDecoder.resolve("Bearer not-a-jwt") }

        assertEquals("unreadable token", e.message)
    }

    @Test fun `malformed header is UnauthenticatedException`() {
        assertFailsWith<UnauthenticatedException> { withDecoder.resolve("Basic abc") }
        assertFailsWith<UnauthenticatedException> { withoutDecoder.resolve("Basic abc") }
    }

    @Test fun `missing sub without decoder yields identity with null userId`() {
        val identity = assertNotNull(withoutDecoder.resolve("Bearer ${TestJwt.foreignToken(null, listOf("kraken-user"))}"))

        assertNull(identity.userId)
        assertEquals(setOf("kraken-user"), identity.roles)
    }

    @Test fun `dispatcher installs forwarded authentication for unverified identity`() = runBlocking {
        val codec = JsonCodec()
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val handlers = CallerRecordingHandlers()
        registry.register(handlers)
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), withoutDecoder, codec)
        val request = Request.newBuilder()
            .setId("req-1")
            .setKind(RequestKind.COMMAND)
            .setType("test.Ping")
            .setPayload(ByteString.copyFrom(codec.encode(Ping("x"))))
            .putHeaders("authorization", "Bearer ${TestJwt.foreignToken("u-9", listOf("kraken-user"))}")
            .build()

        val reply = dispatcher.dispatch(request)

        assertTrue(reply.hasPayload(), "expected a payload, got ${reply.error.message}")
        assertEquals("u-9", handlers.seenUserId)
    }

    @Test fun `unsigned token without decoder is UnauthenticatedException`() {
        val e = assertFailsWith<UnauthenticatedException> {
            withoutDecoder.resolve("Bearer ${TestJwt.unsignedToken("u-9", listOf("kraken-admin"))}")
        }

        assertEquals("unsigned token", e.message)
    }
}
