package io.damoreira.kraken.messaging.e2e

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.control.ResetTarget
import io.damoreira.kraken.messaging.e2e.control.Catalog
import io.damoreira.kraken.messaging.e2e.control.ControlService
import io.damoreira.kraken.messaging.e2e.control.ItemAdded
import io.damoreira.kraken.messaging.e2e.control.Ledger
import io.damoreira.kraken.messaging.e2e.control.Notifier
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.test.KrakenContainers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Processor control end to end (P6b, ADR 0023): a Spring Boot service against the real hub image under test, driven through
 * the hub's HTTP API with the write token: pause (records held, PAUSED in the hub), resume, a reset to `earliest` that
 * replays the named processor (its `@ResetHandler` ran once, its `@DisallowReplay` handler did not run again), and the dead
 * letters listed, retried and discarded through the hub. The dashboard token is refused on a write. Skips when the image is
 * not built.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcessorControlEndToEndTest {
    private lateinit var kafka: KafkaContainer
    private lateinit var mongo: MongoDBContainer
    private lateinit var hub: GenericContainer<*>
    private lateinit var ctx: ConfigurableApplicationContext
    private val topic = "kraken.events.p6b-" + UUID.randomUUID()
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    private val json = JsonMapper.builder().build()
    private val codec = JsonCodec()

    @BeforeAll fun startAll() {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image under test not built")
        kafka = KrakenContainers.kafka().also { it.start() }
        mongo = KrakenContainers.mongo().also { it.start() }
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers)).use {
            it.createTopics(listOf(NewTopic(topic, 2, 1.toShort()))).all().get()
        }
        hub = KrakenContainers.hub()
            .withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
            .withEnv("KRAKEN_HUB_DASHBOARD_TOKEN", READ_TOKEN)
            .withEnv("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN", WRITE_TOKEN)
            .also { it.start() }
        ctx = SpringApplicationBuilder(ControlService::class.java)
            .web(WebApplicationType.NONE)
            .properties(
                "kraken.messaging.service-name=svc-p6b",
                "kraken.messaging.type-prefix=p6b",
                "kraken.messaging.hub.address=static://${hub.host}:${hub.getMappedPort(7000)}",
                "kraken.messaging.hub.status-interval=500ms",
                "kraken.messaging.kafka.bootstrap-servers=${kafka.bootstrapServers}",
                "kraken.messaging.kafka.topic=$topic",
                "kraken.messaging.kafka.consumer.max-attempts=2",
                "kraken.messaging.outbox.enabled=false",
                "spring.mongodb.uri=mongodb://${mongo.host}:${mongo.firstMappedPort}/svc-p6b?directConnection=true",
                "spring.main.banner-mode=off",
            )
            .run()
        check(runBlocking { ctx.getBean(HubConnections::class.java).awaitRegistered(Duration.ofSeconds(20)) })
    }

    @AfterAll fun stopAll() {
        if (::ctx.isInitialized) runCatching { ctx.close() }
        if (::hub.isInitialized) runCatching { hub.stop() }
        if (::kafka.isInitialized) kafka.stop()
        if (::mongo.isInitialized) mongo.stop()
    }

    private fun publish(item: ItemAdded, partition: Int) {
        KafkaProducer(mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers), StringSerializer(), ByteArraySerializer()).use { p ->
            val record = ProducerRecord<String, ByteArray>(topic, partition, item.itemId, codec.encode(item))
            record.headers().add(RecordHeader("type", "p6b.ItemAdded".toByteArray()))
            record.headers().add(RecordHeader("id", "evt-${item.itemId}".toByteArray()))
            p.send(record).get()
        }
    }

    private class Answer(val status: Int, val body: JsonNode)

    private fun call(method: String, path: String, token: String, body: String? = null): Answer {
        val request = HttpRequest.newBuilder(URI.create("http://${hub.host}:${hub.getMappedPort(7001)}$path"))
            .timeout(Duration.ofSeconds(30))
            .header("Authorization", "Bearer $token")
            .apply { if (body != null) header("Content-Type", "application/json") }
            .method(method, if (body != null) HttpRequest.BodyPublishers.ofString(body) else HttpRequest.BodyPublishers.noBody())
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        return Answer(response.statusCode(), if (response.body().isBlank()) json.createObjectNode() else json.readTree(response.body()))
    }

    private val base = "/api/v1/services/svc-p6b/processors"

    private fun JsonNode.nodes(): List<JsonNode> = (0 until size()).map { get(it) }

    private fun processor(name: String): JsonNode? {
        val services = call("GET", "/api/v1/processors", READ_TOKEN).body["services"].nodes()
        return services.singleOrNull { it["serviceName"].asString() == "svc-p6b" }?.get("processors")?.nodes()?.singleOrNull { it["name"].asString() == name }
    }

    @Test fun `pause, resume, reset and dead letters through the hub's HTTP API`() {
        val connection = ctx.getBean(HubConnections::class.java).connections().single()
        val catalog = ctx.getBean(Catalog::class.java)
        val notifier = ctx.getBean(Notifier::class.java)
        val ledger = ctx.getBean(Ledger::class.java)
        for (i in 0..3) publish(ItemAdded("i-$i"), partition = i % 2)
        await().atMost(Duration.ofSeconds(30)).until { catalog.seen.size == 4 && ledger.seen.size == 4 }
        await().atMost(Duration.ofSeconds(30)).until { processor("catalog")?.get("state")?.asString() == "RUNNING" }

        // The dashboard token reads but never writes.
        assertEquals(403, call("POST", "$base/catalog/pause", READ_TOKEN).status)

        val paused = call("POST", "$base/catalog/pause", WRITE_TOKEN)
        assertEquals(200, paused.status, paused.body.toString())
        assertEquals(1, paused.body["acknowledged"].asInt())
        assertTrue(runBlocking { ctx.getBean(MessageStore::class.java).processorControl.get("catalog") }!!.paused)
        await().atMost(Duration.ofSeconds(15)).until { processor("catalog")?.get("state")?.asString() == "PAUSED" }
        publish(ItemAdded("i-4"), partition = 0)
        await().atMost(Duration.ofSeconds(20)).until { "i-4" in ledger.seen }
        Thread.sleep(1500)
        assertFalse("i-4" in catalog.seen, "the paused processor holds its records; default does not")
        assertEquals(200, call("POST", "$base/catalog/resume", WRITE_TOKEN).status)
        await().atMost(Duration.ofSeconds(20)).until { "i-4" in catalog.seen }

        val reset = call("POST", "$base/catalog/reset", WRITE_TOKEN, """{"to":"earliest"}""")
        assertEquals(200, reset.status, reset.body.toString())
        assertEquals("ok", reset.body["outcome"].asString())
        assertEquals(1, reset.body["resetHandlers"].asInt())
        assertEquals(5, reset.body["markersDeleted"].asInt())
        assertEquals(listOf(0 to 3L, 1 to 2L), reset.body["ranges"].nodes().map { it["partition"].asInt() to it["until"].asLong() })
        assertEquals(ResetTarget.Earliest, catalog.resets.single().target)
        await().atMost(Duration.ofSeconds(30)).until { catalog.seen.size == 10 }
        assertEquals(setOf(2), catalog.seen.groupingBy { it }.eachCount().values.toSet(), "every record of the range once more")
        assertEquals(5, notifier.seen.size, "@DisallowReplay did not run again")
        assertEquals(5, ledger.seen.size, "default was not reset")
        await().atMost(Duration.ofSeconds(20)).until { runBlocking { ctx.getBean(MessageStore::class.java).processorControl.get("catalog") }!!.replay.isEmpty() }

        // Dead letters of default, through the hub.
        publish(ItemAdded("bad-1", poison = true), partition = 1)
        await().atMost(Duration.ofSeconds(30)).until { call("GET", "$base/default/dead-letters", WRITE_TOKEN).body["total"]?.asInt() == 1 }
        val letters = call("GET", "$base/default/dead-letters?limit=10", READ_TOKEN)
        assertEquals(200, letters.status, letters.body.toString())
        val letter = letters.body["letters"].nodes().single()
        assertEquals("evt-bad-1", letter["eventId"].asString())
        assertEquals("HANDLER_FAILED", letter["reason"].asString())
        assertFalse(letter.has("payload"), "no payload leaves the instance")
        val id = letter["id"].asString()
        val retried = call("POST", "$base/default/dead-letters/$id/retry", WRITE_TOKEN)
        assertEquals(200, retried.status, retried.body.toString())
        assertEquals("Failed", retried.body["result"].asString())
        assertEquals(200, call("DELETE", "$base/default/dead-letters/$id", WRITE_TOKEN).status)
        assertEquals(404, call("DELETE", "$base/default/dead-letters/$id", WRITE_TOKEN).status)
    }

    private companion object {
        const val READ_TOKEN = "p6b-dashboard-read"
        const val WRITE_TOKEN = "p6b-dashboard-write"
    }
}
