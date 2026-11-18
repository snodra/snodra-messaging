package com.damoreira.snodra.messaging.e2e

import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.CommandBus
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.QueryBus
import com.damoreira.snodra.messaging.UnauthenticatedException
import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.api.Query
import com.damoreira.snodra.messaging.e2e.a.ServiceA
import com.damoreira.snodra.messaging.e2e.a.ServiceAEvents
import com.damoreira.snodra.messaging.e2e.b.ServiceB
import com.damoreira.snodra.messaging.e2e.b.ServiceBHandlers
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.test.SnodraContainers
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

/** Admin-only command owned by B (`@RequiresAuthentication("hasRole('snodra-admin')")`). */
@MessageType("b.AdminOnly")
data class BAdminOnly(val text: String) : Command<BPong>

/** Sent by A's event handler (no caller token) to an unannotated handler on B. */
@MessageType("b.Record")
data class BRecord(val id: String) : Command<Unit>

/** Two Spring Boot services talking through the real `snodra-server:dev` image, Kafka and MongoDB. */
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


    @Test
    fun `command from event handler reaches unannotated handler without token`() {
        val id = "rec-" + UUID.randomUUID()
        asUser(TestJwt.token("u-42", listOf("user"))) {
            serviceA.getBean(CommandBus::class.java).sendBlocking(BPing(id))
        }
        // A's @EventHandler for b.Happened sends b.Record with no token; B's unannotated handler must accept it.
        val handlers = contextB!!.getBean(ServiceBHandlers::class.java)
        await().atMost(Duration.ofSeconds(20)).until { handlers.recorded.contains(id) }
    }

    @Test
    fun `annotated handler denies user without role end to end`() {
        asUser(TestJwt.token("u-42", listOf("user"))) {
            val e = assertFailsWith<AccessDeniedException> {
                serviceA.getBean(CommandBus::class.java).sendBlocking(BAdminOnly("x"))
            }
            assertEquals("handler ServiceBHandlers.admin: access denied", e.message)
        }
        asUser(TestJwt.token("u-1", listOf("snodra-admin"))) {
            assertEquals("u-1", serviceA.getBean(CommandBus::class.java).sendBlocking(BAdminOnly("x")).echoed)
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
        private const val TOPIC = "snodra.events"
        private var kafka: KafkaContainer? = null
        private var mongo: MongoDBContainer? = null
        private var hub: GenericContainer<*>? = null
        private var contextA: ConfigurableApplicationContext? = null
        private var contextB: ConfigurableApplicationContext? = null
        private val serviceA: ConfigurableApplicationContext get() = contextA!!

        @JvmStatic
        @BeforeAll
        fun startAll() {
            Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "snodra-server image not built")
            kafka = SnodraContainers.kafka().also { it.start() }
            mongo = SnodraContainers.mongo().also { it.start() }
            hub = SnodraContainers.hub().withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "").also { it.start() }
            createTopic(kafka!!.bootstrapServers)
            contextB = start(ServiceB::class.java, "svc-b", "b", "e2e_b")
            contextA = start(ServiceA::class.java, "svc-a", "a", "e2e_a")
            // The hub connects asynchronously after refresh: wait until it acknowledged both registrations (b.Ping).
            for (context in listOf(contextA!!, contextB!!)) {
                val registered = runBlocking { context.getBean(HubConnections::class.java).awaitRegistered(Duration.ofSeconds(20)) }
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
                    "snodra.messaging.service-name=$serviceName",
                    "snodra.messaging.type-prefix=$prefix",
                    "snodra.messaging.hub.address=static://${hub!!.host}:${hub!!.getMappedPort(7000)}",
                    "snodra.messaging.hub.request-timeout=10s",
                    "snodra.messaging.kafka.bootstrap-servers=${kafka!!.bootstrapServers}",
                    "snodra.messaging.events.source=$TOPIC",
                    "snodra.messaging.outbox.poll-interval=100ms",
                    "spring.mongodb.uri=$mongoUri", // Boot 4 key; spring.data.mongodb.uri is no longer bound
                    "spring.main.banner-mode=off",
                    "logging.level.com.damoreira=INFO",
                )
                .run()
        }
    }
}
