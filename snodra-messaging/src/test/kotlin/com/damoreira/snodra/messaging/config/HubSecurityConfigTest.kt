package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.hub.DefaultHubChannelFactory
import com.damoreira.snodra.messaging.hub.HubChannelFactory
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.test.AclEntry
import com.damoreira.snodra.messaging.test.HubAuth
import com.damoreira.snodra.messaging.test.InProcessHub
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.boot.actuate.context.properties.ConfigurationPropertiesReportEndpoint
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.actuate.endpoint.Show
import org.springframework.boot.actuate.env.EnvironmentEndpoint
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.Environment
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.env.SystemEnvironmentPropertySource
import tools.jackson.databind.json.JsonMapper
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.reflect.full.memberProperties
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Client configuration: `hub.token`, `hub.token-file`, `hub.tls.*`, and that the token never leaks. */
class HubSecurityConfigTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(SnodraMessagingProperties::class)
    class PropertiesOnly

    private val hub = InProcessHub().start()
    private val token = "tok-" + UUID.randomUUID()
    private val acl = mapOf(token to AclEntry("orders", handle = listOf("test.*"), send = listOf("*")))

    @AfterEach fun tearDown() = hub.close()

    private val required = arrayOf("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test")
    private val propertiesRunner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java).withPropertyValues(*required)

    /** The environment-variable source of the real environment, so Boot applies its name mapping. */
    private fun ApplicationContextRunner.withEnv(vararg pairs: Pair<String, Any>) = withInitializer { context ->
        context.environment.propertySources.addFirst(
            SystemEnvironmentPropertySource("test-" + StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, mapOf(*pairs)),
        )
    }

    private fun starter() = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, EventsAutoConfiguration::class.java),
        )
        .withPropertyValues(*required, "snodra.messaging.hub.enabled=true", "snodra.messaging.events.consumer.enabled=false", "snodra.messaging.outbox.enabled=false")
        .withBean("snodraHubChannelFactory", HubChannelFactory::class.java, { HubChannelFactory { hub.channel() } })

    // --- binding ---

    @Test fun `the security settings default to no token and no TLS`() {
        propertiesRunner.run { context ->
            val h = context.getBean(SnodraMessagingProperties::class.java).hub
            assertNull(h.token)
            assertNull(h.tokenFile)
            assertEquals(false, h.tls.enabled)
            assertNull(h.tls.caFile)
            assertNull(h.tls.serverName)
        }
    }

    @Test fun `token, token-file and tls bind from properties`() {
        propertiesRunner.withPropertyValues(
            "snodra.messaging.hub.token=abc", "snodra.messaging.hub.token-file=/run/secrets/hub-token",
            "snodra.messaging.hub.tls.enabled=true", "snodra.messaging.hub.tls.ca-file=/etc/snodra/ca.pem",
            "snodra.messaging.hub.tls.server-name=hub.internal",
        ).run { context ->
            val h = context.getBean(SnodraMessagingProperties::class.java).hub
            assertEquals("abc", h.token)
            assertEquals(Path.of("/run/secrets/hub-token"), h.tokenFile)
            assertEquals(true, h.tls.enabled)
            assertEquals(Path.of("/etc/snodra/ca.pem"), h.tls.caFile)
            assertEquals("hub.internal", h.tls.serverName)
        }
    }

    @Test fun `relaxed environment binding SNODRA_MESSAGING_HUB_TOKEN, _TOKEN_FILE and _TLS_ENABLED`() {
        propertiesRunner.withEnv(
            "SNODRA_MESSAGING_HUB_TOKEN" to "from-env", "SNODRA_MESSAGING_HUB_TOKEN_FILE" to "/run/secrets/t", "SNODRA_MESSAGING_HUB_TLS_ENABLED" to "true",
        ).run { context ->
            val h = context.getBean(SnodraMessagingProperties::class.java).hub
            assertEquals("from-env", h.token)
            assertEquals(Path.of("/run/secrets/t"), h.tokenFile)
            assertTrue(h.tls.enabled)
        }
    }

    // --- redaction ---

    @Test fun `toString of the properties never prints the token and still prints every other field`() {
        propertiesRunner.withPropertyValues("snodra.messaging.hub.token=$token").run { context ->
            val properties = context.getBean(SnodraMessagingProperties::class.java)
            assertFalse(token in properties.toString(), properties.toString())
            assertFalse(token in properties.hub.toString())
            assertTrue("****" in properties.hub.toString())
            // The hand-written toString must not forget a field added later.
            for (field in SnodraMessagingProperties.Hub::class.memberProperties) {
                assertTrue(field.name + "=" in properties.hub.toString(), "Hub.toString misses ${field.name}")
            }
        }
    }

    private val mapper = JsonMapper.builder().build()

    @Test fun `the actuator configprops and env output never show the token, even with show-values always`() {
        val environmentVariableToken = "env-" + UUID.randomUUID()
        starter().withPropertyValues("snodra.messaging.hub.token=$token").withEnv("SNODRA_SERVER_TOKEN" to environmentVariableToken).run { context ->
            assertEquals(null, context.startupFailure)
            val functions = context.getBeansOfType(SanitizingFunction::class.java).values.toList()
            assertEquals(1, functions.size, "the starter contributes one SanitizingFunction")

            val configprops = ConfigurationPropertiesReportEndpoint(functions, Show.ALWAYS).also { it.setApplicationContext(context) }
            val configJson = mapper.writeValueAsString(configprops.configurationProperties())
            assertTrue("snodra.messaging" in configJson && "orders" in configJson, "the report does list the properties")
            assertFalse(token in configJson, "configprops")

            val env = EnvironmentEndpoint(context.environment as Environment, functions, Show.ALWAYS)
            val envJson = mapper.writeValueAsString(env.environment(null))
            assertTrue("snodra.messaging.hub.token" in envJson, "the env report does list the key")
            assertFalse(token in envJson, "env")
            assertFalse(environmentVariableToken in envJson, "env, SNODRA_SERVER_TOKEN")
            val entry = mapper.writeValueAsString(env.environmentEntry("snodra.messaging.hub.token"))
            assertFalse(token in entry, "env entry")
        }
    }

    @Test fun `the sanitizing function leaves other keys alone`() {
        starter().run { context ->
            val function = context.getBean(SanitizingFunction::class.java)
            val data = org.springframework.boot.actuate.endpoint.SanitizableData(null, "snodra.messaging.hub.address", "hub:7000")
            assertEquals("hub:7000", function.apply(data).value)
            assertEquals(org.springframework.boot.actuate.endpoint.SanitizableData.SANITIZED_VALUE, function.apply(org.springframework.boot.actuate.endpoint.SanitizableData(null, "SNODRA_MESSAGING_HUB_TOKEN", "x")).value)
        }
    }

    // --- the token reaches the hub ---

    private fun ApplicationContextRunner.registers(check: (HubConnections) -> Unit) = run { context ->
        assertEquals(null, context.startupFailure)
        val connections = context.getBean(HubConnections::class.java)
        await atMost Duration.ofSeconds(5) until { hub.acksSentTo(connections.instanceId) >= 1 }
        check(connections)
    }

    @Test fun `snodra messaging hub token goes in Register credentials`() {
        hub.auth = HubAuth.enforce(acl)
        starter().withPropertyValues("snodra.messaging.hub.token=$token").registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
    }

    @Test fun `SNODRA_MESSAGING_HUB_TOKEN and the plain SNODRA_SERVER_TOKEN both work, the property wins over SNODRA_SERVER_TOKEN`() {
        hub.auth = HubAuth.enforce(acl)
        starter().withEnv("SNODRA_MESSAGING_HUB_TOKEN" to token).registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
        starter().withEnv("SNODRA_SERVER_TOKEN" to token).registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
        starter().withPropertyValues("snodra.messaging.hub.token=$token").withEnv("SNODRA_SERVER_TOKEN" to "ignored-" + UUID.randomUUID()).registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
    }

    @Test fun `no token configured sends no credentials, an empty token too`() {
        for (extra in listOf(emptyArray(), arrayOf("snodra.messaging.hub.token="))) {
            starter().withPropertyValues(*extra).registers { connections ->
                assertFalse(hub.lastRegisterFrom(connections.instanceId)!!.hasCredentials(), extra.toList().toString())
            }
        }
    }

    @Test fun `the token file wins over the token and is read for the stream, through the property or SNODRA_SERVER_TOKEN_FILE`(@TempDir dir: Path) {
        hub.auth = HubAuth.enforce(acl)
        val file = dir.resolve("hub-token")
        Files.writeString(file, token + "\n")
        starter().withPropertyValues("snodra.messaging.hub.token=wrong-inline", "snodra.messaging.hub.token-file=$file").registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
        starter().withEnv("SNODRA_SERVER_TOKEN_FILE" to file.toString()).registers { connections ->
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom(connections.instanceId).distinct())
        }
    }

    @Test fun `a token file that cannot be read at startup fails the context and names the file, not a token`(@TempDir dir: Path) {
        val missing = dir.resolve("nope")
        starter().withPropertyValues("snodra.messaging.hub.token=$token", "snodra.messaging.hub.token-file=$missing").run { context ->
            val failure = assertNotNull(context.startupFailure)
            val chain = generateSequence(failure as Throwable?) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
            assertTrue(missing.toString() in chain, chain)
            assertFalse(token in chain)
        }
    }

    // --- TLS settings ---

    @Test fun `a CA file that cannot be used fails the startup naming tls ca-file`(@TempDir dir: Path) {
        val bad = dir.resolve("ca.pem").also { Files.writeString(it, "not a certificate") }
        ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, EventsAutoConfiguration::class.java))
            .withPropertyValues(*required, "snodra.messaging.hub.enabled=true", "snodra.messaging.hub.tls.enabled=true", "snodra.messaging.hub.tls.ca-file=$bad",
                "snodra.messaging.events.consumer.enabled=false", "snodra.messaging.outbox.enabled=false")
            .run { context ->
                val chain = generateSequence(context.startupFailure as Throwable?) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
                assertTrue("snodra.messaging.hub.tls.ca-file" in chain, chain)
            }
    }

    @Test fun `the default channel factory is the starter's when no override is defined`() {
        ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, EventsAutoConfiguration::class.java))
            .withPropertyValues(*required, "snodra.messaging.hub.enabled=true", "snodra.messaging.hub.tls.enabled=true",
                "snodra.messaging.events.consumer.enabled=false", "snodra.messaging.outbox.enabled=false")
            .run { context ->
                assertEquals(null, context.startupFailure)
                assertTrue(context.getBean("snodraHubChannelFactory") is DefaultHubChannelFactory)
            }
    }

    @Test fun `the configuration metadata lists the new keys`() {
        val json = checkNotNull(javaClass.getResourceAsStream("/META-INF/spring-configuration-metadata.json")).use { String(it.readAllBytes()) }
        for (key in listOf("hub.token", "hub.token-file", "hub.tls.enabled", "hub.tls.ca-file", "hub.tls.server-name")) {
            assertTrue("\"snodra.messaging.$key\"" in json, key)
        }
    }
}
