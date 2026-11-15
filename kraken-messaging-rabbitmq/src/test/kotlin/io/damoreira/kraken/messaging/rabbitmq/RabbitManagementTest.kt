package io.damoreira.kraken.messaging.rabbitmq

import com.sun.net.httpserver.HttpServer
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.Base64

/** The management API client fails closed: whatever it cannot trust is an error, never "nobody reads". */
class RabbitManagementTest {
    private val answers = HashMap<String, Pair<Int, String>>()
    private val seenAuth = mutableListOf<String>()
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            synchronized(seenAuth) { seenAuth += exchange.requestHeaders.getFirst("Authorization").orEmpty() }
            if (exchange.requestURI.rawPath == "/api/slow") {
                exchange.sendResponseHeaders(200, 0) // the headers at once, then a body that never ends in time
                exchange.responseBody.write("[".toByteArray())
                exchange.responseBody.flush()
                runCatching { Thread.sleep(10_000) }
                runCatching { exchange.responseBody.close() }
                return@createContext
            }
            val (status, body) = answers[exchange.requestURI.rawPath] ?: (404 to "{}")
            if (status == 302) exchange.responseHeaders.add("Location", "/api/overview")
            val bytes = body.toByteArray()
            exchange.sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        start()
    }
    private val url = "http://127.0.0.1:${server.address.port}"

    @AfterEach
    fun stop() = server.stop(0)

    private fun api(username: String? = null, password: String? = null) =
        RabbitManagement(RabbitManagementSettings(url, username, password), "stream-user", "stream-pass", "/")

    private val consumers = "/api/stream/consumers/%2F"

    @Test
    fun `members are the kraken-member ids of the consumers of the name on the source's streams`() {
        answers["/api/overview"] = 200 to """{"disable_stats": false}"""
        answers[consumers] = 200 to """[
            {"properties": {"name": "svc.p", "single-active-consumer": "true", "kraken-member": "m-2", "super-stream": "s"}, "queue": {"name": "s-0", "vhost": "/"}, "active": true},
            {"properties": {"name": "svc.p", "kraken-member": "m-1"}, "queue": {"name": "s-1"}},
            {"properties": {"name": "svc.p", "kraken-member": "m-9"}, "queue": {"name": "other-0"}},
            {"properties": {"name": "svc.q", "kraken-member": "m-3"}, "queue": {"name": "s-0"}},
            {"properties": {"name": "svc.p"}, "queue": {"name": "s-0"}, "connection_details": {"name": "c-7"}, "subscription_id": 4},
            {"properties": {}, "queue": {"name": "s-0"}}
        ]"""
        assertEquals(listOf("c-7:4", "m-1", "m-2"), api().members("svc.p", listOf("s-0", "s-1")))
        assertEquals("Basic " + Base64.getEncoder().encodeToString("stream-user:stream-pass".toByteArray()), seenAuth.first(), "the stream user's by default")
        answers[consumers] = 200 to """{"items": []}"""
        assertEquals(emptyList<String>(), api(username = "admin", password = "x").members("svc.p", listOf("s-0")))
        assertEquals("Basic " + Base64.getEncoder().encodeToString("admin:x".toByteArray()), seenAuth.last())
    }

    @Test
    fun `an answer it cannot trust is an error`() {
        answers["/api/overview"] = 200 to """{"disable_stats": false}"""
        for (bad in listOf(
            200 to "{}",
            200 to "<html>login</html>",
            200 to """[{"queue": {"name": "s-0"}}]""",
            200 to """[{"properties": {"name": "svc.p"}}]""",
            200 to """[{"properties": {"name": 7}, "queue": {"name": "s-0"}}]""",
            500 to "[]",
            302 to "",
        )) {
            answers[consumers] = bad
            assertThrows(EventStoreException.Failed::class.java, { api().members("svc.p", listOf("s-0")) }, "$bad")
        }
    }

    @Test
    fun `a body that does not end within the request deadline fails`() {
        val started = System.nanoTime()
        val api = RabbitManagement(RabbitManagementSettings(url), "u", "p", "/")
        val method = RabbitManagement::class.java.getDeclaredMethod("get", String::class.java).apply { isAccessible = true }
        val failure = runCatching { method.invoke(api, "slow") }.exceptionOrNull()?.cause
        assertTrue(failure is EventStoreException.Failed, "$failure")
        assertTrue((System.nanoTime() - started) / 1_000_000 < 8_000, "within the 5 s deadline, not the server's 10 s")
    }

    @Test
    fun `metrics switched off cannot tell who reads`() {
        answers["/api/overview"] = 200 to """{"disable_stats": true}"""
        answers[consumers] = 200 to "[]"
        assertFalse(api().statsEnabled())
        assertThrows(EventStoreException.Failed::class.java) { api().members("svc.p", listOf("s-0")) }
        answers["/api/overview"] = 200 to """{"disable_stats": "no"}"""
        assertThrows(EventStoreException.Failed::class.java) { api().statsEnabled() }
    }

    @Test
    fun `open declares replay only when the API answers with its metrics on`() {
        val settings = RabbitEventStoreSettings(hosts = listOf("127.0.0.1:1"), management = RabbitManagementSettings(url))
        answers["/api/overview"] = 200 to """{"disable_stats": false}"""
        RabbitEventStore.open(settings).use { assertTrue(Capability.REPLAY in it.capabilities) }
        answers["/api/overview"] = 200 to """{"disable_stats": true}"""
        RabbitEventStore.open(settings).use { assertFalse(Capability.REPLAY in it.capabilities) }
        val unreachable = settings.copy(management = RabbitManagementSettings("http://127.0.0.1:1"))
        assertThrows(EventStoreException.Failed::class.java) { RabbitEventStore.open(unreachable) }
        // Without the API: no replay and no admin, no request at all.
        RabbitEventStore.open(settings.copy(management = null)).use { store ->
            assertEquals(setOf(Capability.BROWSE, Capability.LAG, Capability.NATIVE_DEAD_LETTER), store.capabilities)
            val refused = runCatching { store.admin(io.damoreira.kraken.messaging.eventstore.SubscriptionSpec("svc.p", "s")) }.exceptionOrNull()
            assertEquals(Capability.REPLAY, (refused as EventStoreException.Unsupported).capability)
        }
    }
}
