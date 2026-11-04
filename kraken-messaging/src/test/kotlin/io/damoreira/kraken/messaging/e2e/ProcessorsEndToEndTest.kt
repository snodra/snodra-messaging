package io.damoreira.kraken.messaging.e2e

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.deadletter.RetryResult
import io.damoreira.kraken.messaging.e2e.processors.Audit
import io.damoreira.kraken.messaging.e2e.processors.OrderPlaced
import io.damoreira.kraken.messaging.e2e.processors.OrdersProjection
import io.damoreira.kraken.messaging.e2e.processors.ProcessorsService
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
import kotlin.test.assertTrue

/**
 * Event processors end to end (ADR 0022): a Spring Boot service with a `default` handler and a named processor, its status
 * reported to the real hub image under test (`GET /api/v1/processors`, `/api/v1/topology`), a poisoned event parked and
 * retried. Skips when the image is not built.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProcessorsEndToEndTest {
    private var topic = ""
    private lateinit var kafka: KafkaContainer
    private lateinit var mongo: MongoDBContainer
    private val hubs = mutableListOf<GenericContainer<*>>()
    private val contexts = mutableListOf<ConfigurableApplicationContext>()
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    private val json = JsonMapper.builder().build()
    private val codec = JsonCodec()

    @BeforeAll fun startAll() {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image under test not built")
        kafka = KrakenContainers.kafka().also { it.start() }
        mongo = KrakenContainers.mongo().also { it.start() }
    }

    /** A fresh events topic for the next service (each test reads only its own events). */
    private fun newTopic() {
        topic = "kraken.events.p6a-" + UUID.randomUUID()
        AdminClient.create(mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers)).use {
            it.createTopics(listOf(NewTopic(topic, 2, 1.toShort()))).all().get()
        }
    }

    @AfterAll fun stopAll() {
        contexts.forEach { runCatching { it.close() } }
        hubs.forEach { runCatching { it.stop() } }
        if (::kafka.isInitialized) kafka.stop()
        if (::mongo.isInitialized) mongo.stop()
    }

    private fun startHub(container: GenericContainer<*>) = container.withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").also { it.start(); hubs += it }

    private fun service(hub: GenericContainer<*>, name: String): ConfigurableApplicationContext {
        newTopic()
        val mongoUri = "mongodb://${mongo.host}:${mongo.firstMappedPort}/$name?directConnection=true"
        return SpringApplicationBuilder(ProcessorsService::class.java)
            .web(WebApplicationType.NONE)
            .properties(
                "kraken.messaging.service-name=$name",
                "kraken.messaging.type-prefix=p6a",
                "kraken.messaging.hub.address=static://${hub.host}:${hub.getMappedPort(7000)}",
                "kraken.messaging.hub.status-interval=500ms",
                "kraken.messaging.kafka.bootstrap-servers=${kafka.bootstrapServers}",
                "kraken.messaging.kafka.topic=$topic",
                "kraken.messaging.kafka.consumer.max-attempts=2",
                "kraken.messaging.outbox.enabled=false",
                "spring.mongodb.uri=$mongoUri",
                "spring.main.banner-mode=off",
            )
            .run().also { contexts += it }
            .also { ctx -> check(runBlocking { ctx.getBean(HubConnections::class.java).awaitRegistered(Duration.ofSeconds(20)) }) }
    }

    private fun publish(order: OrderPlaced, key: String) {
        KafkaProducer(mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers), StringSerializer(), ByteArraySerializer()).use { p ->
            val record = ProducerRecord<String, ByteArray>(topic, key, codec.encode(order))
            record.headers().add(RecordHeader("type", "p6a.OrderPlaced".toByteArray()))
            record.headers().add(RecordHeader("id", "evt-${order.orderId}".toByteArray()))
            p.send(record).get()
        }
    }

    private fun get(hub: GenericContainer<*>, path: String): JsonNode {
        val response = http.send(
            HttpRequest.newBuilder(URI.create("http://${hub.host}:${hub.getMappedPort(7001)}$path")).timeout(Duration.ofSeconds(5)).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(response.statusCode() == 200) { "$path answered ${response.statusCode()}: ${response.body()}" }
        return json.readTree(response.body())
    }

    private fun JsonNode.nodes(): List<JsonNode> = (0 until size()).map { get(it) }

    @Test fun `the hub shows both processors with their group, state, partitions and dead letters`() {
        val hub = startHub(KrakenContainers.hub())
        val ctx = service(hub, "svc-p6a")
        val connection = ctx.getBean(HubConnections::class.java).connections().single()

        publish(OrderPlaced("o-1"), "k1")
        publish(OrderPlaced("o-2", poison = true), "k2")
        publish(OrderPlaced("o-3"), "k2") // same key as the poisoned one: sequenced behind it in `orders`
        await().atMost(Duration.ofSeconds(30)).until { ctx.getBean(Audit::class.java).seen.size == 3 }

        await().atMost(Duration.ofSeconds(40)).untilAsserted {
            val service = get(hub, "/api/v1/processors")["services"].nodes().single { it["serviceName"].asString() == "svc-p6a" }
            val processors = service["processors"].nodes().associateBy { it["name"].asString() }
            assertEquals(setOf("default", "orders"), processors.keys)
            val orders = processors.getValue("orders")
            assertEquals("svc-p6a.orders", orders["groupId"].asString())
            assertEquals(topic, orders["topic"].asString())
            assertEquals("FAILING", orders["state"].asString())
            assertEquals("collection", orders["deadLetterMode"].asString())
            assertEquals(2, orders["deadLetters"].asInt())
            assertEquals(1, orders["failedTotal"].asInt())
            assertEquals(2, orders["assignedPartitions"].asInt())
            assertEquals(listOf("p6a.OrderPlaced"), orders["eventTypes"].nodes().map { it.asString() })
            assertTrue(orders["lastError"]["message"].asString().contains("cannot project o-2"))
            val default = processors.getValue("default")
            assertEquals("svc-p6a", default["groupId"].asString())
            assertEquals("RUNNING", default["state"].asString())
            assertEquals(3, default["processedTotal"].asInt())
            assertEquals(0, default["lag"].asInt())
            val partitions = default["instances"].nodes().single()["partitions"]
            assertEquals(listOf(0, 1), partitions.nodes().map { it["partition"].asInt() })
            // A partition without any record of this test has no committed offset (-1).
            assertEquals(3L, partitions.nodes().sumOf { it["committed"].asLong().coerceAtLeast(0) })
            assertEquals(3L, partitions.nodes().sumOf { it["highWatermark"].asLong() })
        }
        // The raw report is in the topology as well.
        val instance = get(hub, "/api/v1/topology")["services"].nodes().single { it["serviceName"].asString() == "svc-p6a" }["instances"].nodes().single()
        assertEquals(setOf("default", "orders"), instance["processors"].nodes().map { it["name"].asString() }.toSet())

        // Fix nothing, retry the head: still failing; the follower stays blocked; discard the head, retry the rest.
        val deadLetters = ctx.getBean(DeadLetters::class.java)
        val head = deadLetters.list("orders").first { it.eventId == "evt-o-2" }
        assertTrue(deadLetters.retry(head.id) is RetryResult.Failed)
        val follower = deadLetters.list("orders").first { it.eventId == "evt-o-3" }
        assertEquals(RetryResult.Blocked(head.id), deadLetters.retry(follower.id))
        assertTrue(deadLetters.discard(head.id))
        assertEquals(RetryResult.Retried, deadLetters.retry(follower.id))
        assertEquals(listOf("o-1", "o-2", "o-2", "o-2", "o-3"), ctx.getBean(OrdersProjection::class.java).seen.sorted())
    }
}
