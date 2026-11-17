package com.damoreira.snodra.examples.orders

import com.damoreira.snodra.messaging.CommandBus
import com.damoreira.snodra.messaging.HubUnavailableException
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.RemoteHandlerException
import com.damoreira.snodra.messaging.test.SnodraContainers
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import java.nio.file.Files
import java.time.Duration

/**
 * The orders service end to end on PostgreSQL and the hub image under test (`SNODRA_SERVER_IMAGE`): a command sent through the
 * hub stores the order row and the outbox entry of `OrderPlaced` in one transaction; the relay publishes the event, the
 * service's event handler confirms the order, and its processed marker is in `snodra_processed_events`. A command that fails
 * after its writes leaves neither. Skips when the hub image is not built.
 */
@SpringBootTest
class OrdersEndToEndTest {
    @Autowired lateinit var commands: CommandBus
    @Autowired lateinit var jdbc: JdbcClient

    @Test
    fun `a placed order is stored, its event relayed, handled and marked processed`() {
        val reply = send(PlaceOrder("o-1", "book"))
        assertEquals("o-1", reply.orderId)

        await().atMost(Duration.ofSeconds(15)).until { status("o-1") == "CONFIRMED" }
        val (eventId, sent) = jdbc.sql("SELECT id, sent_at IS NOT NULL AS sent FROM snodra_outbox WHERE type = 'examples.orders.OrderPlaced' AND event_key = 'o-1'")
            .query { rs, _ -> rs.getString("id") to rs.getBoolean("sent") }.single()
        assertEquals(true, sent, "the relay marked the entry sent")
        await().atMost(Duration.ofSeconds(5)).until {
            jdbc.sql("SELECT COUNT(*) FROM snodra_processed_events WHERE id = :id").param("id", "processor:default:$eventId").query(Long::class.javaObjectType).single() == 1L
        }
    }

    @Test
    fun `a command that fails after its writes leaves neither the order nor its event`() {
        assertThrows(RemoteHandlerException::class.java) { send(PlaceOrder("o-2", "")) }
        assertEquals(null, status("o-2"))
        assertEquals(0L, jdbc.sql("SELECT COUNT(*) FROM snodra_outbox WHERE event_key = 'o-2'").query(Long::class.javaObjectType).single())
    }

    /** Sends [command] once the service's types are registered with the hub. */
    private fun send(command: PlaceOrder): OrderAccepted {
        var reply: Result<OrderAccepted> = Result.failure(IllegalStateException("not sent"))
        await().atMost(Duration.ofSeconds(15)).until {
            reply = runCatching { runBlocking { commands.send(command) } }
            reply.exceptionOrNull().let { it !is NoHandlerException && it !is HubUnavailableException }
        }
        return reply.getOrThrow()
    }

    private fun status(id: String): String? =
        jdbc.sql("SELECT status FROM orders WHERE id = :id").param("id", id).query(String::class.java).optional().orElse(null)

    companion object {
        private val postgres: PostgreSQLContainer = SnodraContainers.postgres()
        private var hub: GenericContainer<*>? = null

        @BeforeAll
        @JvmStatic
        fun start() {
            assumeTrue(SnodraContainers.hubImageAvailable(), "hub image under test not built")
        }

        @AfterAll
        @JvmStatic
        fun stop() {
            hub?.stop()
        }

        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            postgres.start()
            val started = SnodraContainers.hubAcl(Files.createTempDirectory("orders-hub"), authMode = "off").also { it.start() }
            hub = started
            registry.add("spring.datasource.url") { postgres.jdbcUrl }
            registry.add("spring.datasource.username") { postgres.username }
            registry.add("spring.datasource.password") { postgres.password }
            registry.add("snodra.messaging.hub.address") { "static://localhost:${started.getMappedPort(7000)}" }
        }
    }
}
