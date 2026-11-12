package io.damoreira.kraken.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.messaging.fixtures.TestOAuth2Server
import io.damoreira.kraken.messaging.fixtures.TokenResponse
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** The process-wide access token of a service (P5b, ADR 0019): cached, fetched once for everybody, renewed in the background. */
class HubTokenProviderTest {
    private val server = TestOAuth2Server()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val wait = Duration.ofSeconds(10)

    private val log = LoggerFactory.getLogger(HubTokenProvider::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun captureLog() {
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() {
        log.detachAppender(appender)
        scope.cancel()
        server.close()
    }

    private fun provider() = HubTokenProvider(
        TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", { "secret" })), scope,
        retryMin = Duration.ofMillis(50), retryMax = Duration.ofMillis(200),
    )

    // --- the refresh rule ---

    @Test fun `the refresh margin is 20 percent of the lifetime, at least 5 seconds, at most half of it`() {
        fun margin(seconds: Long) = AccessToken("t", Instant.EPOCH, Duration.ofSeconds(seconds)).refreshMargin.toMillis() / 1000.0
        assertEquals(120.0, margin(600))
        assertEquals(20.0, margin(100))
        assertEquals(5.0, margin(25))
        assertEquals(5.0, margin(10))
        assertEquals(3.0, margin(6), "a 6 s token is renewed after 3 s, not after 1 s")
        assertEquals(1.0, margin(2))
        assertEquals(Instant.EPOCH.plusSeconds(80), AccessToken("t", Instant.EPOCH, Duration.ofSeconds(100)).refreshAt)
    }

    // --- the cache ---

    @Test fun `a fresh token is served from the cache, one fetch for any number of callers`() = runBlocking {
        val provider = provider()
        val first = provider.token()
        assertSame(first, provider.token())
        assertEquals(1, server.requests.size)
        assertEquals(first, provider.current.value)
        assertEquals(1, provider.fetches(TokenFetchResult.OK))
    }

    @Test fun `concurrent callers share one fetch`() = runBlocking {
        server.delayMs = 300
        val provider = provider()
        val tokens = (1..20).map { async(Dispatchers.Default) { provider.token() } }.awaitAll()
        assertEquals(1, server.requests.size)
        assertEquals(1, tokens.map { it.value }.distinct().size)
    }

    @Test fun `concurrent callers of a failing endpoint share its one failure`() {
        server.delayMs = 200
        server.responder = { TokenResponse(401, """{"error":"invalid_client"}""") }
        val provider = provider()
        runBlocking {
            val results = (1..10).map { async(Dispatchers.Default) { runCatching { provider.token() } } }.awaitAll()
            assertTrue(results.all { it.exceptionOrNull() is HubTokenException })
        }
        assertEquals(1, server.requests.size)
        assertEquals("invalid_client", provider.lastError.value)
        assertEquals(1, provider.fetches(TokenFetchResult.OAUTH_ERROR))
    }

    @Test fun `an endpoint that fails is an exception for the caller and recovers when it does`() = runBlocking {
        server.responder = { TokenResponse(401, """{"error":"invalid_client","error_description":"bad credentials"}""") }
        val provider = provider()
        val e = assertFailsWith<HubTokenException> { provider.token() }
        assertEquals(TokenFetchResult.OAUTH_ERROR, e.result)
        assertNull(provider.current.value)
        assertEquals("invalid_client: bad credentials", provider.lastError.value)
        server.responder = null
        provider.token()
        assertNull(provider.lastError.value)
        assertEquals(1, provider.fetches(TokenFetchResult.OK))
    }

    // --- the background renewal ---

    @Test fun `a token is renewed in the background before it expires, nobody asking`() {
        server.expiresIn = 2 // margin 1 s: renewed after 1 s
        val provider = provider()
        val first = runBlocking { provider.token() }
        await atMost wait until { provider.current.value?.value != first.value }
        assertTrue(server.requests.size >= 2)
        assertTrue(provider.current.value!!.expiresAt > first.expiresAt)
    }

    /** A wall clock that can jump forward, as after the machine slept (the monotonic delay of the renewer does not see it). */
    private class JumpingClock : Clock() {
        @Volatile var offset: Duration = Duration.ZERO
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = Instant.now().plus(offset)
    }

    @Test fun `the renewer follows the wall clock, so a jump after the machine slept renews the token at once`() {
        server.expiresIn = 3600 // due after 48 minutes: a monotonic sleep of that length would not notice the jump
        val clock = JumpingClock()
        val provider = HubTokenProvider(
            TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", { "secret" }), clock), scope, clock,
            retryMin = Duration.ofMillis(50), retryMax = Duration.ofMillis(200),
        )
        val first = runBlocking { provider.token() }
        clock.offset = Duration.ofSeconds(3500) // the laptop slept: past the refresh point, 100 s from expiry
        await atMost Duration.ofSeconds(8) until { provider.current.value?.value != first.value }
        assertEquals(2, server.requests.size)
    }

    @Test fun `freshness follows the monotonic clock too, so a wall clock set back does not keep a token past its refresh point`() {
        server.expiresIn = 100 // refreshed 80 s after it was fetched
        val clock = JumpingClock()
        var nanos = 0L
        val provider = HubTokenProvider(
            TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", { "secret" }), clock), scope, clock,
            retryMin = Duration.ofMillis(50), retryMax = Duration.ofMillis(200), nanoTime = { nanos },
        )
        val first = runBlocking { provider.token() }
        clock.offset = Duration.ofHours(-1) // an NTP step back: by the wall clock the token is younger than ever
        nanos = Duration.ofSeconds(90).toNanos() // but 90 s went by

        val next = runBlocking { provider.token() }

        assertNotEquals(first.value, next.value, "a token past its refresh point by the monotonic clock is not served")
        assertTrue(server.requests.size >= 2)
    }

    @Test fun `a token the hub refused is renewed in the background at its next look and no longer reported`() {
        server.expiresIn = 3600 // its own refresh point is 48 minutes away
        val provider = provider()
        val first = runBlocking { provider.token() }

        provider.invalidate(first.value)

        assertNotEquals(first.value, provider.usable()?.value, "a refused token is not reported as the current one")
        await atMost Duration.ofSeconds(8) until { provider.current.value?.value != first.value }
        assertEquals(provider.current.value, provider.usable())
        assertEquals(2, server.requests.size)
    }

    @Test fun `a failing renewal keeps the current token, retries with a back-off and recovers`() {
        server.expiresIn = 2
        val provider = provider()
        val first = runBlocking { provider.token() }
        server.responder = { TokenResponse(503, "down") }
        await atMost wait until { provider.fetches(TokenFetchResult.HTTP_ERROR) >= 3 }
        assertEquals(first, provider.current.value, "the old token stays while the endpoint is down")
        assertEquals("HTTP 503", provider.lastError.value)
        server.responder = null
        await atMost wait until { provider.current.value?.value != first.value }
        await atMost wait until { provider.lastError.value == null }
        // One WARN for the streak, not one per attempt.
        assertEquals(1, appender.list.count { it.level == Level.WARN }, appender.list.joinToString { it.formattedMessage })
    }

    @Test fun `the token never appears in the provider's toString or its logs`() = runBlocking {
        val provider = provider()
        val token = provider.token()
        assertTrue(token.value !in provider.toString())
        assertTrue(token.value !in appender.list.joinToString { it.formattedMessage })
        assertNotEquals("", provider.toString())
    }
}
