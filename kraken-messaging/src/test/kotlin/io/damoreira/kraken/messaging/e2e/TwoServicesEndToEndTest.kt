package io.damoreira.kraken.messaging.e2e

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.e2e.a.ServiceA
import io.damoreira.kraken.messaging.e2e.a.ServiceAEvents
import io.damoreira.kraken.messaging.e2e.b.ServiceB
import io.damoreira.kraken.messaging.e2e.b.ServiceBHandlers
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.test.KrakenContainers
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.awaitility.Awaitility.await
import org.slf4j.MDC
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.security.core.context.SecurityContextHolder
import com.nimbusds.jwt.SignedJWT
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.UUID
import java.util.concurrent.ExecutionException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

@MessageType("b.Ping")
data class BPing(val text: String) : Command<BPong>

data class BPong(val echoed: String, val at: Instant)

@MessageType("b.Happened")
data class BHappened(val id: String, val at: Instant) : Event

/** Nobody registers this type at the hub. */
@MessageType("a.Unowned")
data class Unowned(val key: String) : Query<BCount>

data class BCount(val value: Int)

/** Two Spring Boot services talking through the real `kraken-hub:dev` image, Kafka and MongoDB. */
class TwoServicesEndToEndTest {

    @AfterEach
    fun clearSecurityContext() = SecurityContextHolder.clearContext()

    @Test
    fun `command from A is handled by B through the hub with the caller identity`() {
        val pong = asUser(TestJwt.token("u-42", listOf("user"))) {
            serviceA.getBean(CommandBus::class.java).sendBlocking(BPing("hi-" + UUID.randomUUID()))
        }
        assertEquals("u-42", pong.echoed)
    }

    @Test
    fun `event published by B inside its handler reaches A exactly once`() {
        val id = "evt-" + UUID.randomUUID()
        val correlationId = "corr-" + UUID.randomUUID()
        MDC.put("correlationId", correlationId) // the caller's correlation id, e.g. set by an HTTP filter
        try {
            asUser(TestJwt.token("u-42", listOf("user"))) {
                serviceA.getBean(CommandBus::class.java).sendBlocking(BPing(id))
            }
        } finally {
            MDC.remove("correlationId")
        }
        val events = serviceA.getBean(ServiceAEvents::class.java)
        await().atMost(Duration.ofSeconds(20)).until { events.received.count { it.id == id } == 1 }
        Thread.sleep(2000)
        assertEquals(1, events.received.count { it.id == id })

        // The correlation id travelled A -> hub -> B (command) -> outbox -> Kafka -> A (event), with the acting user.
        assertEquals(correlationId, contextB!!.getBean(ServiceBHandlers::class.java).correlationIds[id])
        val ctx = events.contexts.single { it.first.id == id }.second
        assertEquals(correlationId, ctx.correlationId)
        assertEquals(correlationId, ctx.headers["correlationId"])
        assertEquals("u-42", ctx.userId)
        assertEquals("u-42", ctx.headers["userId"])
    }

    @Test
    fun `query to a type nobody owns is NoHandlerException`() {
        val e = asUser(TestJwt.token("u-42", listOf("user"))) {
            assertFailsWith<NoHandlerException> { serviceA.getBean(QueryBus::class.java).sendBlocking(Unowned("x")) }
        }
        assertEquals("a.Unowned", e.type)
    }

    @Test
    fun `expired token is UnauthenticatedException`() {
        asUser(TestJwt.token("u-42", listOf("user"), expiresIn = (-5).minutes)) {
            assertFailsWith<UnauthenticatedException> {
                serviceA.getBean(CommandBus::class.java).sendBlocking(BPing("expired"))
            }
        }
    }

    /** Runs [block] with [token] as the caller, the way an HTTP request authenticated by a resource server would. */
    private fun <T> asUser(token: String, block: () -> T): T {
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(unvalidatedJwt(token))
        try {
            return block()
        } finally {
            SecurityContextHolder.clearContext()
        }
    }

    /**
     * The [Jwt] of [token] without validation: `TestJwt.decoder()` would reject the expired token before it could be
     * sent, and the receiving service is the one that must reject it.
     */
    private fun unvalidatedJwt(token: String): Jwt {
        val claims = SignedJWT.parse(token).jwtClaimsSet.claims.mapValues { (_, v) -> if (v is Date) v.toInstant() else v }
        return Jwt.withTokenValue(token).header("alg", "RS256").claims { it.putAll(claims) }.build()
    }

    companion object {
        private const val TOPIC = "kraken.events"
        private var kafka: KafkaContainer? = null
        private var mongo: MongoDBContainer? = null
        private var hub: GenericContainer<*>? = null
        private var contextA: ConfigurableApplicationContext? = null
        private var contextB: ConfigurableApplicationContext? = null
        private val serviceA: ConfigurableApplicationContext get() = contextA!!

        @JvmStatic
        @BeforeAll
        fun startAll() {
            Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "kraken-hub image not built")
            kafka = KrakenContainers.kafka().also { it.start() }
            mongo = KrakenContainers.mongo().also { it.start() }
            hub = KrakenContainers.hub().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").also { it.start() }
            createTopic(kafka!!.bootstrapServers)
            contextB = start(ServiceB::class.java, "svc-b", "b", "e2e_b")
            contextA = start(ServiceA::class.java, "svc-a", "a", "e2e_a")
            // The hub connects asynchronously after refresh: wait until it acknowledged both registrations (b.Ping).
            for (context in listOf(contextA!!, contextB!!)) {
                val registered = runBlocking { context.getBean(HubConnection::class.java).awaitRegistered(Duration.ofSeconds(20)) }
                check(registered) { "${context.id} was not registered at the hub within 20s" }
            }
        }

        @JvmStatic
        @AfterAll
        fun stopAll() {
            contextA?.close()
            contextB?.close()
            hub?.stop()
            kafka?.stop()
            mongo?.stop()
        }

        private fun createTopic(bootstrap: String) =
            AdminClient.create(mapOf<String, Any>("bootstrap.servers" to bootstrap)).use {
                try {
                    it.createTopics(listOf(NewTopic(TOPIC, 6, 1.toShort()))).all().get()
                } catch (e: ExecutionException) {
                    if (e.cause !is TopicExistsException) throw e
                }
            }

        private fun start(app: Class<*>, serviceName: String, prefix: String, database: String): ConfigurableApplicationContext {
            val mongoUri = "mongodb://${mongo!!.host}:${mongo!!.firstMappedPort}/$database?directConnection=true"
            return SpringApplicationBuilder(app)
                .web(WebApplicationType.NONE)
                .properties(
                    "kraken.messaging.service-name=$serviceName",
                    "kraken.messaging.type-prefix=$prefix",
                    "kraken.messaging.hub.address=static://${hub!!.host}:${hub!!.getMappedPort(7000)}",
                    "kraken.messaging.hub.request-timeout=10s",
                    "kraken.messaging.kafka.bootstrap-servers=${kafka!!.bootstrapServers}",
                    "kraken.messaging.kafka.topic=$TOPIC",
                    "kraken.messaging.security.require-jwt=true",
                    "kraken.messaging.outbox.poll-interval=100ms",
                    "spring.mongodb.uri=$mongoUri", // Boot 4 key; spring.data.mongodb.uri is no longer bound
                    "spring.main.banner-mode=off",
                    "logging.level.io.damoreira=INFO",
                )
                .run()
        }
    }
}
