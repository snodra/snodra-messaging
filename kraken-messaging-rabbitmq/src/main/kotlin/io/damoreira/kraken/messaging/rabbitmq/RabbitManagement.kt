package io.damoreira.kraken.messaging.rabbitmq

import io.damoreira.kraken.messaging.eventstore.EventStoreException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.Duration
import java.util.Base64
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

/**
 * The management HTTP API (plugin `rabbitmq_stream_management`): the stream consumers connected now, which the stream protocol
 * cannot list. It tells who reads a subscription (the reset's refusal). It fails closed: an answer of another shape, a consumer
 * without its stream or properties, a non-string property, or metrics switched off (`management.disable_stats`) is an error,
 * never "nobody reads". No redirect is followed; an answer is read up to [MAX_ANSWER] bytes.
 */
internal class RabbitManagement(settings: RabbitManagementSettings, streamUser: String, streamPassword: String, private val virtualHost: String) {
    private val root = settings.url.trim().trimEnd('/')
    private val authorization: String
    private val http: HttpClient

    init {
        val (user, password) = settings.login(streamUser, streamPassword)
        authorization = "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray())
        val builder = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).connectTimeout(REQUEST_TIMEOUT)
        settings.caFile?.let { builder.sslContext(sslContext(it)) }
        http = builder.build()
    }

    /** One stream consumer the API lists: its name (the subscription's; null for one without), its stream, its member. */
    data class Consumer(val name: String?, val stream: String, val member: String)

    /** Whether the broker collects the metrics the consumer list comes from (with `management.disable_stats` the list is always empty). */
    fun statsEnabled(): Boolean {
        val overview = get("overview")
        if (!overview.isObject) throw failed("the overview is not an object")
        val disabled = overview.get("disable_stats") ?: return true
        if (!disabled.isBoolean) throw failed("the overview's disable_stats is $disabled")
        return !disabled.booleanValue()
    }

    /** Every stream consumer of the virtual host; fails, never "nobody", on an answer it cannot trust. */
    fun consumers(): List<Consumer> {
        if (!statsEnabled()) {
            throw failed("it collects no metrics (management.disable_stats): it cannot tell who reads a subscription")
        }
        return parseConsumers(get("stream/consumers/" + URLEncoder.encode(virtualHost, Charsets.UTF_8).replace("+", "%20")))
    }

    /** The members reading the subscription [name] on any of [streams], sorted. */
    fun members(name: String, streams: Collection<String>): List<String> =
        consumers().filter { it.name == name && it.stream in streams }.map { it.member }.toSortedSet().toList()

    private fun get(path: String): JsonNode {
        val request = HttpRequest.newBuilder(URI.create("$root/api/$path")).timeout(REQUEST_TIMEOUT)
            .header("Authorization", authorization).header("Accept", "application/json").GET().build()
        val response = try {
            http.send(request, HttpResponse.BodyHandlers.ofInputStream())
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw failed("interrupted")
        } catch (e: Exception) {
            throw failed("$path: ${e::class.java.simpleName}: ${e.message}")
        }
        response.body().use { body ->
            if (response.statusCode() !in 200..299) throw failed("$path: HTTP ${response.statusCode()}")
            val out = ByteArrayOutputStream()
            val buffer = ByteArray(64 * 1024)
            while (true) {
                val n = body.read(buffer)
                if (n < 0) break
                if (out.size() + n > MAX_ANSWER) throw failed("$path: an answer over $MAX_ANSWER bytes")
                out.write(buffer, 0, n)
            }
            return try {
                mapper.readTree(out.toByteArray())
            } catch (e: Exception) {
                throw failed("$path: not JSON")
            }
        }
    }

    private fun failed(why: String) = EventStoreException.Failed("the RabbitMQ management API ($root): $why")

    companion object {
        /** The longest one request to the management API may take. */
        val REQUEST_TIMEOUT: Duration = Duration.ofSeconds(5)

        /** The largest answer read from the management API. */
        const val MAX_ANSWER = 16 * 1024 * 1024

        private val mapper = JsonMapper.builder().build()

        /**
         * The consumers of the API's answer: an array of consumers (or a paged answer's `items`), each with `queue.name` and a
         * `properties` object; a consumer without `properties.name` is an unnamed reader (the event browser's), never a
         * subscription's member; the member is the `kraken-member` property, else the consumer's connection and subscription id.
         */
        fun parseConsumers(body: JsonNode): List<Consumer> {
            fun shape(what: String) = EventStoreException.Failed("the RabbitMQ management API's consumer list has an unexpected shape: $what")
            val items = when {
                body.isArray -> body
                body.isObject && body.get("items")?.isArray == true -> body.get("items")
                else -> throw shape("not an array of consumers")
            }
            return items.toList().map { c ->
                val stream = c.get("queue")?.get("name")?.takeIf { it.isString }?.stringValue() ?: throw shape("a consumer without queue.name")
                val properties = c.get("properties")?.takeIf { it.isObject } ?: throw shape("a consumer without properties")
                fun text(name: String): String? = when (val v = properties.get(name)) {
                    null -> null
                    else -> if (v.isString) v.stringValue() else throw shape("properties.$name is $v")
                }
                val member = text(RabbitSubscription.MEMBER_PROPERTY)
                    ?: "${c.get("connection_details")?.get("name")?.takeIf { it.isString }?.stringValue() ?: "?"}:${c.get("subscription_id") ?: "null"}"
                Consumer(text("name"), stream, member)
            }
        }

        private fun sslContext(caFile: java.nio.file.Path): SSLContext {
            val certificates = Files.newInputStream(caFile).use { CertificateFactory.getInstance("X.509").generateCertificates(it) }
            require(certificates.isNotEmpty()) { "kraken.messaging.rabbitmq.management.ca-file $caFile holds no certificate" }
            val store = KeyStore.getInstance(KeyStore.getDefaultType()).apply { load(null, null) }
            certificates.forEachIndexed { i, c -> store.setCertificateEntry("ca-$i", c) }
            val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(store) }
            return SSLContext.getInstance("TLS").apply { init(null, trust.trustManagers, null) }
        }
    }
}
