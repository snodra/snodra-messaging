package io.damoreira.kraken.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.messaging.fixtures.TestOAuth2Server
import io.damoreira.kraken.messaging.fixtures.TokenResponse
import io.damoreira.kraken.messaging.test.TestCertificates
import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsServer
import java.net.InetSocketAddress
import java.security.KeyFactory
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** One exchange with the OAuth2 token endpoint (client credentials, `client_secret_basic`): the request it makes and every way the answer can be wrong. */
class TokenEndpointTest {
    private val server = TestOAuth2Server()
    private val secret = "s3cr3t-" + UUID.randomUUID()

    private val log = LoggerFactory.getLogger(TokenEndpoint::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun captureLog() {
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() {
        log.detachAppender(appender)
        server.close()
    }

    private fun endpoint(
        clientId: String = "billing-service",
        clientSecret: () -> String = { secret },
        scope: String? = null,
        tokenUri: URI = server.tokenUri,
    ) = TokenEndpoint(HubOAuth2(tokenUri, clientId, clientSecret, scope))

    private fun failure(result: TokenFetchResult, endpoint: TokenEndpoint = endpoint()): HubTokenException =
        assertFailsWith<HubTokenException> { runBlocking { endpoint.fetch() } }.also { assertEquals(result, it.result, it.message) }

    @Test fun `posts the client credentials grant as a form with the client authenticated by Basic, and returns the token and its lifetime`() = runBlocking {
        val token = endpoint(scope = "openid kraken").fetch()
        val request = server.requests.single()
        assertEquals("POST", request.method)
        assertEquals("application/x-www-form-urlencoded", request.contentType?.substringBefore(';'))
        assertEquals("client_credentials", request.form["grant_type"])
        assertEquals("openid kraken", request.form["scope"])
        assertEquals("billing-service", request.user)
        assertEquals(secret, request.password)
        assertEquals(server.tokens.single(), token.value)
        assertEquals(Duration.ofSeconds(300), token.lifetime)
        assertFalse(request.form.containsKey("client_secret"), "the secret goes in the Authorization header, not the body")
    }

    @Test fun `no scope is sent when none is configured`() = runBlocking {
        endpoint().fetch()
        assertEquals(setOf("grant_type"), server.requests.single().form.keys)
    }

    @Test fun `the client id and secret are form-urlencoded before the Basic encoding, special characters survive`() = runBlocking {
        val id = "svc:a b/ü"
        val odd = "p@ss:w+rd%&=é /"
        endpoint(clientId = id, clientSecret = { odd }).fetch()
        val request = server.requests.single()
        assertEquals(id, request.user)
        assertEquals(odd, request.password)
        // RFC 6749 2.3.1: the raw header carries the urlencoded forms (a ':' in the id never splits the pair wrongly).
        val raw = String(java.util.Base64.getDecoder().decode(request.rawAuthorization!!.removePrefix("Basic ")))
        assertEquals(1, raw.count { it == ':' }, raw)
    }

    @Test fun `a missing expires_in is 60 seconds and warns once`() = runBlocking {
        server.expiresIn = null
        val endpoint = endpoint()
        assertEquals(Duration.ofSeconds(60), endpoint.fetch().lifetime)
        endpoint.fetch()
        assertEquals(1, appender.list.count { it.level == Level.WARN && "expires_in" in it.formattedMessage }, appender.list.toString())
    }

    @Test fun `a huge expires_in is capped at 10 years, so expiresAt and refreshAt never overflow`() = runBlocking {
        val tenYears = Duration.ofSeconds(10 * 365 * 24 * 3600L)
        for (huge in listOf("9223372036854775807", "32000000000000000", "1e30", "123456789012345678901234567890")) {
            server.responder = { TokenResponse(200, """{"access_token":"a.b.c","token_type":"Bearer","expires_in":$huge}""") }
            val token = endpoint().fetch()
            assertEquals(tenYears, token.lifetime, huge)
            assertTrue(token.expiresAt > token.issuedAt, huge)
            assertTrue(token.refreshAt < token.expiresAt, huge)
        }
        val direct = AccessToken("t", Instant.now(), Duration.ofSeconds(Long.MAX_VALUE))
        assertEquals(tenYears, direct.lifetime)
        assertTrue(direct.expiresAt > direct.issuedAt)
    }

    @Test fun `a token_type other than Bearer is a bad response, bearer in any case is fine, a missing one too`() = runBlocking {
        server.tokenType = "mac"
        failure(TokenFetchResult.BAD_RESPONSE)
        server.tokenType = "bEaReR"
        endpoint().fetch()
        server.tokenType = null
        endpoint().fetch()
    }

    @Test fun `a 401 with an OAuth2 error body is an oauth error carrying error and description`() {
        server.responder = { TokenResponse(401, """{"error":"invalid_client","error_description":"Invalid client or Invalid client credentials"}""") }
        val e = failure(TokenFetchResult.OAUTH_ERROR)
        assertEquals("invalid_client: Invalid client or Invalid client credentials", e.message)
    }

    @Test fun `an error body that is not OAuth2 is an http error naming the status`() {
        server.responder = { TokenResponse(503, "<html>down</html>", "text/html") }
        assertEquals("HTTP 503", failure(TokenFetchResult.HTTP_ERROR).message)
    }

    @Test fun `an error description is clipped to 200 characters`() {
        server.responder = { TokenResponse(400, """{"error":"invalid_request","error_description":"${"x".repeat(1000)}"}""") }
        val message = failure(TokenFetchResult.OAUTH_ERROR).message!!
        assertTrue(message.length <= 200 + "invalid_request: ".length, message.length.toString())
    }

    @Test fun `a 200 that is not JSON, has no access_token or a non-positive lifetime is a bad response`() {
        for (body in listOf("not json", "{}", """{"access_token":"","token_type":"Bearer","expires_in":60}""", """{"access_token":"a.b.c","expires_in":0}""", """{"access_token":"a.b.c","expires_in":"soon"}""")) {
            server.responder = { TokenResponse(200, body) }
            failure(TokenFetchResult.BAD_RESPONSE)
        }
    }

    @Test fun `a response over 1 MiB is a bad response`() {
        server.responder = { TokenResponse(200, """{"access_token":"${"a".repeat(2 * 1024 * 1024)}","expires_in":60}""") }
        failure(TokenFetchResult.BAD_RESPONSE)
    }

    @Test fun `a redirect is not followed and is an http error`() {
        server.responder = { TokenResponse(302, "", headers = mapOf("Location" to "http://localhost:1/elsewhere")) }
        assertEquals("HTTP 302", failure(TokenFetchResult.HTTP_ERROR).message)
    }

    @Test fun `an endpoint that is not listening, or that does not answer within the timeout, is an io error`() {
        failure(TokenFetchResult.IO_ERROR, endpoint(tokenUri = URI.create("http://localhost:1/token")))
    }

    @Test fun `the secret is read for every fetch, so a rotated file applies without a restart`(@TempDir dir: Path) = runBlocking {
        val file = dir.resolve("secret")
        Files.writeString(file, "first\n")
        val endpoint = TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", HubOAuth2.secretSource(null, file)))
        endpoint.fetch()
        Files.writeString(file, "second\n")
        endpoint.fetch()
        assertEquals(listOf("first", "second"), server.requests.map { it.password })
    }

    @Test fun `a secret file that cannot be read is an io error naming the file, not its content`(@TempDir dir: Path) {
        val file = dir.resolve("secret").also { Files.writeString(it, secret) }
        val endpoint = TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", HubOAuth2.secretSource(null, file)))
        Files.delete(file)
        val e = failure(TokenFetchResult.IO_ERROR, endpoint)
        assertTrue(file.toString() in e.message!!, e.message)
        assertFalse(secret in e.message!!)
    }

    @Test fun `neither the secret nor a token appears in an exception, a log line or toString`() = runBlocking {
        server.responder = { TokenResponse(401, """{"error":"invalid_client","error_description":"bad secret $secret"}""") }
        val e = failure(TokenFetchResult.OAUTH_ERROR)
        assertFalse(secret in e.message!!, "an endpoint that echoes the secret back must not leak it through us")
        assertFalse(secret in e.toString())
        server.responder = null
        val endpoint = endpoint()
        val token = endpoint.fetch()
        assertFalse(token.value in token.toString(), token.toString())
        assertFalse(secret in endpoint.toString())
        assertFalse(secret in appender.list.joinToString { it.formattedMessage })
        assertFalse(token.value in appender.list.joinToString { it.formattedMessage })
    }

    // --- https: the platform roots plus an optional CA file ---

    /** An HTTPS token endpoint on `localhost` with a throwaway certificate; answers every POST with a fixed token. */
    private fun httpsServer(certificates: TestCertificates): HttpsServer {
        val certificate = Files.newInputStream(certificates.certificate).use { CertificateFactory.getInstance("X.509").generateCertificate(it) }
        val der = Base64.getMimeDecoder().decode(Files.readAllLines(certificates.privateKey).filterNot { it.startsWith("-----") }.joinToString(""))
        val key = KeyFactory.getInstance("RSA").generatePrivate(PKCS8EncodedKeySpec(der))
        val store = KeyStore.getInstance("PKCS12").apply {
            load(null, null)
            setKeyEntry("k", key, "pw".toCharArray(), arrayOf(certificate))
        }
        val context = SSLContext.getInstance("TLS").apply {
            init(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply { init(store, "pw".toCharArray()) }.keyManagers, null, null)
        }
        return HttpsServer.create(InetSocketAddress(0), 0).apply {
            httpsConfigurator = HttpsConfigurator(context)
            createContext("/token") { exchange ->
                exchange.requestBody.readAllBytes()
                val body = """{"access_token":"a.b.c","token_type":"Bearer","expires_in":60}""".toByteArray()
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            start()
        }
    }

    @Test fun `https trusts a CA file on top of the platform roots, and without it refuses the certificate`(@TempDir dir: Path) = runBlocking {
        val certificates = TestCertificates.generate(dir)
        val https = httpsServer(certificates)
        try {
            val uri = URI.create("https://localhost:${https.address.port}/token")
            assertEquals("a.b.c", TokenEndpoint(HubOAuth2(uri, "billing-service", { secret }, caFile = certificates.certificate)).fetch().value)
            val e = assertFailsWith<HubTokenException> { TokenEndpoint(HubOAuth2(uri, "billing-service", { secret })).fetch() }
            assertEquals(TokenFetchResult.IO_ERROR, e.result, e.message)
        } finally {
            https.stop(0)
        }
    }

    @Test fun `a CA file that cannot be used fails the construction naming the setting`(@TempDir dir: Path) {
        val bad = dir.resolve("ca.pem").also { Files.writeString(it, "not a certificate") }
        val e = assertFailsWith<IllegalStateException> { TokenEndpoint(HubOAuth2(server.tokenUri, "billing-service", { secret }, caFile = bad)) }
        assertTrue("kraken.messaging.hub.oauth2.ca-file" in e.message!!, e.message)
    }
}
