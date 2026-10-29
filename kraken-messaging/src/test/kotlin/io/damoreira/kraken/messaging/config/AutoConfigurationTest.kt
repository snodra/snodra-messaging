package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.kafka.EventConsumer
import io.damoreira.kraken.messaging.kafka.MessagingIndexes
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.kafka.OutboxPublisher
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannel
import io.mockk.mockk
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.config.TransactionManagementConfigUtils
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PingHandlers {
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

@Configuration(proxyBeanMethods = false)
class HandlersConfig {
    @Bean fun pingHandlers() = PingHandlers()
}

@Configuration(proxyBeanMethods = false)
class JwtDecoderConfig {
    @Bean fun jwtDecoder(): JwtDecoder = TestJwt.decoder()
}

class UserCommandBus : CommandBus {
    override suspend fun <R> send(command: Command<R>, routingKey: String?): R = throw UnsupportedOperationException("user bus")
}

@Configuration(proxyBeanMethods = false)
class UserCommandBusConfig {
    @Bean fun userCommandBus(): CommandBus = UserCommandBus()
}

/** Autowires both facades the way application code does. */
@org.springframework.stereotype.Component
class BusClient(val commandBus: CommandBus, val queryBus: QueryBus)

@Configuration(proxyBeanMethods = false)
class MongoMockConfig {
    @Bean fun mongoTemplate(): MongoTemplate = mockk(relaxed = true)
    @Bean fun mongoDatabaseFactory(): MongoDatabaseFactory = mockk(relaxed = true)
}

class AutoConfigurationTest {
    private val hub = InProcessHub().start()

    @AfterEach fun tearDown() = hub.close()

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                KafkaEventsAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
            "kraken.messaging.kafka.consumer.enabled=false",
            "kraken.messaging.outbox.enabled=false",
        )

    /** Hub enabled and pointed at the in-process hub through the channel factory override. */
    private val inProcessChannel: () -> ManagedChannel = { hub.channel() }

    private fun withHub() = runner
        .withPropertyValues("kraken.messaging.hub.enabled=true")
        .withBean("krakenHubChannelFactory", Function0::class.java, { inProcessChannel })

    @Test fun `core beans present`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertEquals(1, context.getBeansOfType(HandlerRegistry::class.java).size)
            assertEquals(1, context.getBeansOfType(HandlerInvoker::class.java).size)
            assertEquals(1, context.getBeansOfType(JsonCodec::class.java).size)
            assertEquals(1, context.getBeansOfType(ForwardedIdentityResolver::class.java).size)
            assertEquals(1, context.getBeansOfType(MessageTypeResolver::class.java).size)
            assertEquals(1, context.getBeansOfType(TraceHeaders::class.java).size)
            // no MongoTemplate in this context: the Kafka/outbox configuration backs off
            assertTrue(context.getBeansOfType(EventBus::class.java).isEmpty())
        }
    }

    @Test fun `user CommandBus bean overrides HubBus`() {
        withHub().withUserConfiguration(UserCommandBusConfig::class.java, BusClient::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            assertIs<UserCommandBus>(context.getBean(CommandBus::class.java))
            assertTrue(context.containsBean("queryBus"))
            assertTrue(context.getBean(QueryBus::class.java) !is CommandBus)
            assertIs<UserCommandBus>(context.getBean(BusClient::class.java).commandBus)
        }
        withHub().withUserConfiguration(BusClient::class.java).run { context ->
            assertEquals(setOf("commandBus"), context.getBeansOfType(CommandBus::class.java).keys - "hubBus")
            assertEquals(setOf("queryBus"), context.getBeansOfType(QueryBus::class.java).keys - "hubBus")
            assertTrue(context.getBean(BusClient::class.java).commandBus !is UserCommandBus)
        }
    }

    @Test fun `hub disabled leaves no HubConnection`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(HubConnection::class.java).isEmpty())
            assertTrue(context.getBeansOfType(HubBus::class.java).isEmpty())
            assertTrue(context.getBeansOfType(CommandBus::class.java).isEmpty())
            assertTrue(context.getBeansOfType(QueryBus::class.java).isEmpty())
        }
    }

    @Test fun `handlers in user beans are registered`() {
        runner.withUserConfiguration(HandlersConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue("test.Ping" in context.getBean(HandlerRegistry::class.java).commandTypes())
        }
    }

    @Test fun `hub lifecycle registers handler types with the hub and stops on close`() {
        withHub().withUserConfiguration(HandlersConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            val connection = context.getBean(HubConnection::class.java)
            await atMost Duration.ofSeconds(5) until { connection.connected.value && "test.Ping" in hub.registeredTypes() }
            val registrations = hub.registrationsFrom(connection.instanceId)
            context.close()
            await atMost Duration.ofSeconds(5) until { hub.connectedInstances().isEmpty() }
            // stop() announced an empty registration (no more routing) before closing the stream
            assertEquals(registrations + 1, hub.registrationsFrom(connection.instanceId))
        }
    }

    @Test fun `lifecycle phases stop after the web server drain`() {
        val gracefulShutdown = SmartLifecycle.DEFAULT_PHASE - 1024 // WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE
        val webServerStartStop = gracefulShutdown - 1024 // WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE
        assertEquals(SmartLifecycle.DEFAULT_PHASE - 1900, EventConsumer.PHASE)
        assertEquals(SmartLifecycle.DEFAULT_PHASE - 1950, OutboxPublisher.PHASE)
        assertTrue(HubLifecycle.PHASE < webServerStartStop) // starts before the server, stops after it
        // stop order (highest phase first): drain, consumer, publisher, web server, hub
        assertTrue(gracefulShutdown > EventConsumer.PHASE && EventConsumer.PHASE > OutboxPublisher.PHASE)
        withHub().run { context ->
            assertEquals(HubLifecycle.PHASE, context.getBean(HubLifecycle::class.java).phase)
        }
    }

    @Test fun `kafka events wiring with an application MongoTemplate`() {
        runner.withUserConfiguration(MongoMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9")
            .run { context ->
            assertEquals(null, context.startupFailure)
            assertIs<OutboxEventBus>(context.getBean(EventBus::class.java))
            // The producer settings are accepted by the Kafka client (it validates timeouts when the producer is built).
            @Suppress("UNCHECKED_CAST")
            val producerFactory = context.getBean("krakenKafkaProducerFactory") as ProducerFactory<String, ByteArray>
            assertEquals(10_000, producerFactory.configurationProperties["max.block.ms"])
            assertEquals(30_000, producerFactory.configurationProperties["delivery.timeout.ms"])
            producerFactory.createProducer().close(Duration.ZERO)
            assertIs<KafkaTemplate<*, *>>(context.getBean("krakenKafkaTemplate"))
            assertEquals(1, context.getBeansOfType(MessagingIndexes::class.java).size)
            assertEquals(1, context.getBeansOfType(TransactionTemplate::class.java).size)
            assertTrue(context.getBeansOfType(OutboxPublisher::class.java).isEmpty()) // outbox.enabled=false
            assertTrue(context.getBeansOfType(EventConsumer::class.java).isEmpty()) // kafka.consumer.enabled=false
            // ordered before Boot's TransactionAutoConfiguration: @Transactional support sees the MongoTransactionManager
            assertIs<MongoTransactionManager>(context.getBean(PlatformTransactionManager::class.java))
            assertTrue(context.containsBean(TransactionManagementConfigUtils.TRANSACTION_ADVISOR_BEAN_NAME))
        }
    }

    @Test fun `starts without JwtDecoder`() {
        runner.run { context ->
            assertNotNull(context.getBean(ForwardedIdentityResolver::class.java))
        }
    }

    @Test fun `require-jwt property fails startup with removal hint`() {
        runner.withPropertyValues("kraken.messaging.security.require-jwt=true").run { context ->
            assertTrue(context.startupFailure != null, "expected startup failure")
            val message = generateSequence(context.startupFailure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
            assertTrue(message.contains("kraken.messaging.security.require-jwt was removed"), message)
        }
        runner
            .withInitializer { ctx ->
                ctx.environment.propertySources.addFirst(
                    org.springframework.core.env.SystemEnvironmentPropertySource(
                        "test-systemEnvironment",
                        mapOf("KRAKEN_MESSAGING_SECURITY_REQUIRE_JWT" to "false"),
                    ),
                )
            }
            .run { context ->
                assertTrue(context.startupFailure != null, "expected startup failure from the env-style property")
                val message = generateSequence(context.startupFailure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
                assertTrue(message.contains("kraken.messaging.security.require-jwt was removed"), message)
            }
    }

    @Test fun `require-jwt check also runs with a user supplied identity resolver bean`() {
        runner
            .withPropertyValues("kraken.messaging.security.require-jwt=true")
            .withBean(ForwardedIdentityResolver::class.java, { ForwardedIdentityResolver(null) })
            .run { context -> assertTrue(context.startupFailure != null, "expected startup failure despite the user bean") }
    }
}
