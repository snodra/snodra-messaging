package io.damoreira.kraken.messaging.hub

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.IOException
import java.math.BigDecimal
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

/**
 * Where and as whom a service gets its hub access token (P5b, ADR 0019): the OAuth2 client credentials grant against [tokenUri]
 * (Keycloak: `<realm>/protocol/openid-connect/token`), the client authenticated with [clientId] and the secret [clientSecret]
 * returns (HTTP Basic, `client_secret_basic`). [clientSecret] is called for every fetch, so a rotated secret file applies without a
 * restart; it throws [IOException] when the secret cannot be read. [scope] is sent as it is (space-separated). [caFile] adds CA
 * certificates (PEM) to the platform roots for an `https` [tokenUri]. The secret never appears in [toString].
 */
class HubOAuth2(
    val tokenUri: URI,
    val clientId: String,
    val clientSecret: () -> String,
    val scope: String? = null,
    val caFile: Path? = null,
) {
    init {
        require(tokenUri.scheme == "http" || tokenUri.scheme == "https") { "the token endpoint must be an http(s) URI, was $tokenUri" }
        require(!tokenUri.host.isNullOrEmpty()) { "the token endpoint has no host: $tokenUri" }
        require(clientId.isNotBlank()) { "the OAuth2 client id must not be blank" }
    }

    override fun toString() = "HubOAuth2(tokenUri=$tokenUri, clientId=$clientId, clientSecret=****, scope=$scope, caFile=$caFile)"

    companion object {
        /**
         * The secret of `hub.oauth2.client-secret-file` / `client-secret`: [file] when set (read on every call, surrounding
         * whitespace dropped; wins over [inline]), else [inline]. A file that cannot be read or is empty is an [IOException] that
         * names the file and never its content.
         */
        fun secretSource(inline: String?, file: Path?): () -> String {
            if (file != null) return {
                val text = try {
                    Files.readString(file).trim()
                } catch (e: IOException) {
                    throw IOException("client secret file $file cannot be read: ${e.javaClass.simpleName}", e)
                }
                if (text.isEmpty()) throw IOException("client secret file $file is empty")
                text
            }
            val value = inline?.trim()
            require(!value.isNullOrEmpty()) { "no OAuth2 client secret" }
            return { value }
        }
    }
}

/** What one token fetch came to; [label] is the value of the `result` label of `kraken_hub_client_token_fetches_total`. */
enum class TokenFetchResult(val label: String) {
    OK("ok"),
    /** The endpoint answered with an error status and no OAuth2 error body (or could not be asked: see [IO_ERROR]). */
    HTTP_ERROR("http_error"),
    /** The endpoint answered with an OAuth2 `error` (for example `invalid_client`). */
    OAUTH_ERROR("oauth_error"),
    /** No answer: connection, TLS, timeout, or the secret file could not be read. */
    IO_ERROR("io_error"),
    /** A 200 that is not a usable token response. */
    BAD_RESPONSE("bad_response"),
}

/** A token fetch that failed. [message] is short, clipped and free of the client secret; it never holds a token. */
class HubTokenException(val result: TokenFetchResult, message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * An access token and when it was issued. It is fresh for most of its life: [refreshAt] is [refreshMargin] before [expiresAt], where
 * the margin is 20 % of [lifetime] (capped at 10 years), at least 5 s, and never more than half of the lifetime (a 6 s token is renewed after 3 s,
 * not after 1 s, which would renew it every second). [toString] never shows [value].
 */
class AccessToken(val value: String, val issuedAt: Instant, lifetime: Duration) {
    /** The lifetime the endpoint announced, capped at [MAX_LIFETIME] (10 years): a larger one overflows [Instant] arithmetic. */
    val lifetime: Duration = if (lifetime > MAX_LIFETIME) MAX_LIFETIME else lifetime

    val expiresAt: Instant get() = issuedAt.plus(lifetime)
    val refreshMargin: Duration = lifetime.dividedBy(5).coerceAtLeast(Duration.ofSeconds(5)).coerceAtMost(lifetime.dividedBy(2))
    val refreshAt: Instant get() = expiresAt.minus(refreshMargin)

    private fun Duration.coerceAtLeast(min: Duration) = if (this < min) min else this
    private fun Duration.coerceAtMost(max: Duration) = if (this > max) max else this

    override fun toString() = "AccessToken(****, expiresAt=$expiresAt)"

    companion object {
        /** The longest lifetime a token is given, whatever `expires_in` says. */
        val MAX_LIFETIME: Duration = Duration.ofSeconds(10 * 365 * 24 * 3600L)
    }
}

/**
 * One exchange with the token endpoint of [oauth2] (RFC 6749 4.4): `POST grant_type=client_credentials[&scope=...]`, the client
 * authenticated by HTTP Basic with its id and secret form-urlencoded first (2.3.1). Never follows a redirect, 10 s for the whole
 * exchange, answers over 1 MiB are refused, `https` trusts the platform roots plus [HubOAuth2.caFile]. A failure is a
 * [HubTokenException] whose message holds the endpoint's `error` and `error_description` or the HTTP status (clipped to 200
 * characters, the secret masked).
 *
 * `expires_in` missing: 60 s and one WARN. `token_type` present but not `Bearer` (any case): a bad response.
 */
class TokenEndpoint(private val oauth2: HubOAuth2, private val clock: Clock = Clock.systemUTC()) {
    private val log = LoggerFactory.getLogger(TokenEndpoint::class.java)
    private val expiresInWarned = AtomicBoolean()

    private val http: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NEVER)
        .connectTimeout(TIMEOUT)
        .apply { trust(oauth2.caFile)?.let { sslContext(it) } }
        .build()

    suspend fun fetch(): AccessToken {
        val secret = try {
            oauth2.clientSecret()
        } catch (e: IOException) {
            throw HubTokenException(TokenFetchResult.IO_ERROR, clip(e.message ?: "client secret cannot be read"))
        }
        val form = listOfNotNull("grant_type" to "client_credentials", oauth2.scope?.let { "scope" to it })
            .joinToString("&") { (k, v) -> form(k) + "=" + form(v) }
        val basic = Base64.getEncoder().encodeToString("${form(oauth2.clientId)}:${form(secret)}".toByteArray(StandardCharsets.UTF_8))
        val request = HttpRequest.newBuilder(oauth2.tokenUri)
            .timeout(TIMEOUT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .header("Authorization", "Basic $basic")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build()
        val response = try {
            withTimeoutOrNull(TIMEOUT.toMillis()) {
                http.sendAsync(request, HttpResponse.BodyHandlers.limiting(HttpResponse.BodyHandlers.ofByteArray(), MAX_BODY)).await()
            } ?: throw HubTokenException(TokenFetchResult.IO_ERROR, "no answer from ${oauth2.tokenUri.host} within ${TIMEOUT.seconds} s")
        } catch (e: CancellationException) {
            throw e
        } catch (e: HubTokenException) {
            throw e
        } catch (e: IOException) {
            // The limiting handler fails a body over its capacity; that is the endpoint's fault, not the network's.
            if (e.message?.startsWith("body exceeds capacity") == true) throw HubTokenException(TokenFetchResult.BAD_RESPONSE, "response over $MAX_BODY bytes", e)
            throw HubTokenException(TokenFetchResult.IO_ERROR, clip(mask(e.javaClass.simpleName + (e.message?.let { ": $it" } ?: ""), secret)), e)
        } catch (e: Exception) {
            throw HubTokenException(TokenFetchResult.IO_ERROR, clip(mask(e.javaClass.simpleName + (e.message?.let { ": $it" } ?: ""), secret)), e)
        }
        val body = response.body()
        return if (response.statusCode() == 200) parse(body) else throw failure(response.statusCode(), body, secret)
    }

    private fun failure(status: Int, body: ByteArray, secret: String): HubTokenException {
        val error = try {
            MAPPER.readTree(body)?.takeIf { it.isObject }?.get("error")?.takeIf { it.isString }?.asString()
        } catch (_: Exception) {
            null
        }
        if (error == null) return HubTokenException(TokenFetchResult.HTTP_ERROR, "HTTP $status")
        val description = try {
            MAPPER.readTree(body).get("error_description")?.takeIf { it.isString }?.asString()
        } catch (_: Exception) {
            null
        }
        return HubTokenException(TokenFetchResult.OAUTH_ERROR, clip(mask(error + (description?.let { ": $it" } ?: ""), secret)))
    }

    private fun parse(body: ByteArray): AccessToken {
        fun bad(why: String): Nothing = throw HubTokenException(TokenFetchResult.BAD_RESPONSE, why)
        val json: JsonNode = try {
            MAPPER.readTree(body)
        } catch (_: Exception) {
            null
        }?.takeIf { it.isObject } ?: bad("the response is not a JSON object")
        val token = json.get("access_token")?.takeIf { it.isString }?.asString()?.takeIf { it.isNotBlank() } ?: bad("the response has no access_token")
        val type = json.get("token_type")
        if (type != null && !type.isNull && !(type.isString && type.asString().equals("Bearer", ignoreCase = true))) bad("token_type is not Bearer")
        val expiresIn = json.get("expires_in")
        val lifetime = when {
            expiresIn == null || expiresIn.isNull -> {
                if (expiresInWarned.compareAndSet(false, true)) log.warn("The token endpoint {} sent no expires_in: assuming {} s", oauth2.tokenUri.host, DEFAULT_LIFETIME.seconds)
                DEFAULT_LIFETIME
            }
            // Any number the endpoint sends: one beyond the cap (even past Long) is the cap, never an overflow later on.
            expiresIn.isNumber && expiresIn.decimalValue() >= BigDecimal(AccessToken.MAX_LIFETIME.seconds) -> AccessToken.MAX_LIFETIME
            expiresIn.isNumber && expiresIn.asLong() > 0 -> Duration.ofSeconds(expiresIn.asLong())
            else -> bad("expires_in is not a positive number")
        }
        return AccessToken(token, clock.instant(), lifetime)
    }

    override fun toString() = "TokenEndpoint($oauth2)"

    companion object {
        private val TIMEOUT: Duration = Duration.ofSeconds(10)
        private const val MAX_BODY = 1024L * 1024
        private val DEFAULT_LIFETIME: Duration = Duration.ofSeconds(60)
        private const val MAX_MESSAGE = 200
        private val MAPPER = JsonMapper.builder().build()

        private fun form(value: String) = URLEncoder.encode(value, StandardCharsets.UTF_8)

        /** Clips to 200 characters and replaces control characters (a hostile endpoint's description goes into logs). */
        private fun clip(text: String) = text.map { if (it.isISOControl()) '?' else it }.joinToString("").take(MAX_MESSAGE)

        private fun mask(text: String, secret: String) = if (secret.isNotEmpty()) text.replace(secret, "****") else text

        /** The platform roots plus the PEM certificates of [caFile]; null (nothing to add) leaves the HTTP client's defaults. */
        private fun trust(caFile: Path?): SSLContext? {
            if (caFile == null) return null
            val extra = try {
                val store = KeyStore.getInstance(KeyStore.getDefaultType()).apply { load(null, null) }
                Files.newInputStream(caFile).use { input ->
                    CertificateFactory.getInstance("X.509").generateCertificates(input).forEachIndexed { i, cert -> store.setCertificateEntry("ca-$i", cert) }
                }
                if (store.size() == 0) throw IllegalStateException("no certificate found")
                x509(store)
            } catch (e: Exception) {
                throw IllegalStateException("kraken.messaging.hub.oauth2.ca-file $caFile cannot be used: ${e.message ?: e.javaClass.simpleName}", e)
            }
            val platform = x509(null)
            val combined = object : X509TrustManager {
                override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) = platform.checkClientTrusted(chain, authType)
                override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {
                    try {
                        platform.checkServerTrusted(chain, authType)
                    } catch (_: CertificateException) {
                        extra.checkServerTrusted(chain, authType)
                    }
                }
                override fun getAcceptedIssuers(): Array<X509Certificate> = platform.acceptedIssuers + extra.acceptedIssuers
            }
            return SSLContext.getInstance("TLS").apply { init(null, arrayOf(combined), null) }
        }

        private fun x509(store: KeyStore?): X509TrustManager = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            .apply { init(store) }.trustManagers.filterIsInstance<X509TrustManager>().first()
    }
}
