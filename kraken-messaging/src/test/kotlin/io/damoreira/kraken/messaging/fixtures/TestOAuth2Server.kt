package io.damoreira.kraken.messaging.fixtures

import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.JWKSet
import com.nimbusds.jose.jwk.RSAKey
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.URLDecoder
import java.util.Base64
import java.util.Date
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/** What the token endpoint received: the Basic credentials (decoded from their form-urlencoding) and the form fields. */
data class TokenRequest(val method: String, val contentType: String?, val user: String?, val password: String?, val form: Map<String, String>, val rawAuthorization: String?)

/** What the token endpoint answers. */
data class TokenResponse(val status: Int, val body: String, val contentType: String = "application/json", val headers: Map<String, String> = emptyMap())

/**
 * A stand-in for Keycloak in tests: `POST /token` (the client-credentials grant, `client_secret_basic`) issues RS256 access
 * tokens whose `azp` is the Basic user, `GET /certs` serves the JWKS of the signing key. The key pair is made once per JVM at test
 * time; nothing is stored. Listens on every interface (a hub container reaches it through `host.testcontainers.internal`).
 */
class TestOAuth2Server : AutoCloseable {
    private val server = HttpServer.create(InetSocketAddress(0), 0).apply { executor = Executors.newCachedThreadPool() }
    val port: Int get() = server.address.port

    /** The issuer the tokens carry as `iss`; any string, the hub compares it exactly. */
    val issuer = "http://oauth.test/realms/kraken"
    val tokenUri: URI get() = URI.create("http://localhost:$port/token")

    /** `expires_in` of the default answer; null leaves the field out. */
    @Volatile var expiresIn: Int? = 300
    @Volatile var tokenType: String? = "Bearer"
    @Volatile var scopeInToken: Boolean = false

    /** Replaces the answer; null keeps the default issuing one. Called on a server thread. */
    @Volatile var responder: ((TokenRequest) -> TokenResponse?)? = null

    /** Milliseconds to wait before answering a token request. */
    @Volatile var delayMs: Long = 0

    /** The `azp` of tokens issued for the next requests, whatever the Basic user (null: the user). */
    @Volatile var azpOverride: String? = null

    /** The audience put in tokens; null leaves `aud` out. */
    @Volatile var audience: String? = null

    private val received = CopyOnWriteArrayList<TokenRequest>()
    private val issued = CopyOnWriteArrayList<String>()
    private val counter = AtomicInteger()
    val requests: List<TokenRequest> get() = received.toList()

    /** Every token this server issued, in order. */
    val tokens: List<String> get() = issued.toList()

    init {
        server.createContext("/token") { exchange -> exchange.use { token(it) } }
        server.createContext("/certs") { exchange -> exchange.use { respond(it, TokenResponse(200, JWKSet(publicKey).toString())) } }
        server.start()
    }

    private fun token(exchange: HttpExchange) {
        val auth = exchange.requestHeaders.getFirst("Authorization")
        val (user, password) = auth?.takeIf { it.startsWith("Basic ") }?.let {
            val decoded = String(Base64.getDecoder().decode(it.removePrefix("Basic ")), Charsets.UTF_8)
            decoded.substringBefore(':').urlDecoded() to decoded.substringAfter(':').urlDecoded()
        } ?: (null to null)
        val form = String(exchange.requestBody.readAllBytes(), Charsets.UTF_8).split('&').filter { it.isNotEmpty() }
            .associate { it.substringBefore('=').urlDecoded() to it.substringAfter('=', "").urlDecoded() }
        val request = TokenRequest(exchange.requestMethod, exchange.requestHeaders.getFirst("Content-Type"), user, password, form, auth)
        received += request
        if (delayMs > 0) Thread.sleep(delayMs)
        respond(exchange, responder?.invoke(request) ?: issue(request))
    }

    private fun issue(request: TokenRequest): TokenResponse {
        val token = sign(azpOverride ?: request.user ?: "unknown", expiresIn ?: 60)
        issued += token
        val fields = listOfNotNull(
            "\"access_token\":\"$token\"",
            tokenType?.let { "\"token_type\":\"$it\"" },
            expiresIn?.let { "\"expires_in\":$it" },
            request.form["scope"]?.takeIf { scopeInToken }?.let { "\"scope\":\"$it\"" },
        )
        return TokenResponse(200, fields.joinToString(",", "{", "}"))
    }

    /** An access token shaped like Keycloak's client-credentials one (`typ` Bearer, `azp`), signed by this server's key. */
    fun sign(azp: String, expiresInSeconds: Int, issuer: String = this.issuer, typ: String? = "Bearer"): String {
        val now = System.currentTimeMillis()
        val claims = JWTClaimsSet.Builder()
            .issuer(issuer).subject("service-account-$azp").claim("azp", azp)
            .jwtID(UUID.randomUUID().toString())
            .issueTime(Date(now - 10_000)).expirationTime(Date(now + expiresInSeconds * 1000L))
            .apply {
                typ?.let { claim("typ", it) }
                this@TestOAuth2Server.audience?.let { audience(it) }
            }
            .build()
        return SignedJWT(JWSHeader.Builder(JWSAlgorithm.RS256).keyID(publicKey.keyID).build(), claims)
            .apply { sign(RSASSASigner(privateKey)) }.serialize()
    }

    private fun respond(exchange: HttpExchange, response: TokenResponse) {
        val bytes = response.body.toByteArray()
        exchange.responseHeaders.add("Content-Type", response.contentType)
        response.headers.forEach { (k, v) -> exchange.responseHeaders.add(k, v) }
        exchange.sendResponseHeaders(response.status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
        if (bytes.isNotEmpty()) exchange.responseBody.write(bytes)
    }

    private fun String.urlDecoded() = URLDecoder.decode(this, Charsets.UTF_8)

    override fun close() = server.stop(0)

    companion object {
        private val privateKey: RSAKey = RSAKeyGenerator(2048).keyID("test-key").generate()
        private val publicKey: RSAKey = privateKey.toPublicJWK()
    }
}
