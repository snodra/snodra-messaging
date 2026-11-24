package com.damoreira.snodra.messaging.config

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.damoreira.snodra.messaging.fixtures.TestOAuth2Server
import com.damoreira.snodra.messaging.hub.HubAuthKind
import com.damoreira.snodra.messaging.hub.HubChannelFactory
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.test.AclEntry
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import com.damoreira.snodra.messaging.test.HubAuth
import com.damoreira.snodra.messaging.test.InProcessHub
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.context.properties.ConfigurationPropertiesReportEndpoint
import org.springframework.boot.actuate.endpoint.SanitizableData
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.actuate.endpoint.Show
import org.springframework.boot.actuate.env.EnvironmentEndpoint
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.env.SystemEnvironmentPropertySource
import tools.jackson.databind.json.JsonMapper
import java.net.URI
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

/** Client configuration: `hub.oauth2.*` and its environment fallbacks, the startup checks, and that the client secret never leaks. */
class HubOAuth2ConfigTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(SnodraMessagingProperties::class)
    class PropertiesOnly

    private val oauth = TestOAuth2Server()
    private val hub = InProcessHub().start()
    private val secret = "client-secret-" + UUID.randomUUID()

    private val log = LoggerFactory.getLogger(HubAutoConfiguration::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun capture() {
        hub.auth = HubAuth.enforce(emptyMap(), listOf(AclEntry("orders", handle = listOf("test.*"), send = listOf("*"))))
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() {
        log.detachAppender(appender)
        hub.close()
        oauth.close()
    }

    private val required = arrayOf("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test")
    private val propertiesRunner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java).withPropertyValues(*required)

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

    private fun failure(runner: ApplicationContextRunner): String {
        var chain = ""
        runner.run { context -> chain = generateSequence(context.startupFailure as Throwable?) { it.cause }.joinToString(" | ") { it.message.orEmpty() } }
        assertTrue(chain.isNotEmpty(), "the context should have failed to start")
        return chain
    }

    private fun oauth2Properties(vararg extra: String) = arrayOf("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}", "snodra.messaging.hub.oauth2.client-secret=$secret", *extra)

    // --- binding ---

    @Test fun `the oauth2 settings default to none`() {
        propertiesRunner.run { context ->
            val o = context.getBean(SnodraMessagingProperties::class.java).hub.oauth2
            assertNull(o.tokenUri); assertNull(o.clientId); assertNull(o.clientSecret); assertNull(o.clientSecretFile); assertNull(o.scope); assertNull(o.caFile)
        }
    }

    @Test fun `token-uri, client-id, client-secret, client-secret-file, scope and ca-file bind from properties and from SNODRA_MESSAGING_HUB_OAUTH2_ variables`() {
        propertiesRunner.withPropertyValues(
            "snodra.messaging.hub.oauth2.token-uri=https://kc.example/token", "snodra.messaging.hub.oauth2.client-id=orders",
            "snodra.messaging.hub.oauth2.client-secret=abc", "snodra.messaging.hub.oauth2.client-secret-file=/run/secrets/s",
            "snodra.messaging.hub.oauth2.scope=a b", "snodra.messaging.hub.oauth2.ca-file=/etc/ca.pem",
        ).run { context ->
            val o = context.getBean(SnodraMessagingProperties::class.java).hub.oauth2
            assertEquals(URI.create("https://kc.example/token"), o.tokenUri)
            assertEquals("orders", o.clientId)
            assertEquals("abc", o.clientSecret)
            assertEquals(Path.of("/run/secrets/s"), o.clientSecretFile)
            assertEquals("a b", o.scope)
            assertEquals(Path.of("/etc/ca.pem"), o.caFile)
        }
        propertiesRunner.withEnv(
            "SNODRA_MESSAGING_HUB_OAUTH2_TOKEN_URI" to "https://kc.example/token", "SNODRA_MESSAGING_HUB_OAUTH2_CLIENT_SECRET" to "from-env",
            "SNODRA_MESSAGING_HUB_OAUTH2_CLIENT_SECRET_FILE" to "/run/secrets/t", "SNODRA_MESSAGING_HUB_OAUTH2_SCOPE" to "x",
        ).run { context ->
            val o = context.getBean(SnodraMessagingProperties::class.java).hub.oauth2
            assertEquals(URI.create("https://kc.example/token"), o.tokenUri)
            assertEquals("from-env", o.clientSecret)
            assertEquals(Path.of("/run/secrets/t"), o.clientSecretFile)
            assertEquals("x", o.scope)
        }
    }

    // --- the token reaches the hub ---

    private fun ApplicationContextRunner.registers(check: (HubConnections) -> Unit) = run { context ->
        assertEquals(null, context.startupFailure)
        val connections = context.getBean(HubConnections::class.java)
        await atMost Duration.ofSeconds(10) until { hub.acksSentTo(connections.instanceId) >= 1 }
        check(connections)
    }

    @Test fun `the starter registers with an access token fetched for the service name, as the client id by default`() {
        starter().withPropertyValues(*oauth2Properties()).registers { connections ->
            val request = oauth.requests.first()
            assertEquals("orders", request.user, "client-id defaults to snodra.messaging.service-name")
            assertEquals(secret, request.password)
            assertEquals(listOf(oauth.tokens.first()), hub.jwtsSeenFrom(connections.instanceId).distinct())
        }
    }

    @Test fun `client-id, scope and the plain SNODRA_SERVER_OAUTH2_ variables work, the property wins over the variable`() {
        starter().withPropertyValues("snodra.messaging.hub.oauth2.client-id=orders", "snodra.messaging.hub.oauth2.client-secret=$secret")
            .withEnv("SNODRA_SERVER_OAUTH2_TOKEN_URI" to oauth.tokenUri.toString(), "SNODRA_SERVER_OAUTH2_SCOPE" to "snodra openid").registers {
                assertEquals("snodra openid", oauth.requests.first().form["scope"])
            }
        starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}", "snodra.messaging.hub.oauth2.client-secret=$secret")
            .withEnv("SNODRA_SERVER_OAUTH2_CLIENT_ID" to "orders", "SNODRA_SERVER_OAUTH2_TOKEN_URI" to "http://localhost:1/ignored").registers {
                assertEquals("orders", oauth.requests.last().user)
            }
        starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}")
            .withEnv("SNODRA_SERVER_OAUTH2_CLIENT_SECRET" to secret).registers {
                assertEquals(secret, oauth.requests.last().password)
            }
    }

    @Test fun `the secret file wins over the secret and is read for every fetch, so a rotation applies without a restart`(@TempDir dir: Path) {
        oauth.expiresIn = 2 // renewed after 1 s
        val file = dir.resolve("client-secret").also { Files.writeString(it, "first-secret\n") }
        starter().withPropertyValues(*oauth2Properties("snodra.messaging.hub.oauth2.client-secret-file=$file")).registers {
            assertEquals("first-secret", oauth.requests.first().password)
            Files.writeString(file, "second-secret\n")
            await atMost Duration.ofSeconds(10) until { oauth.requests.last().password == "second-secret" }
        }
        val viaEnv = dir.resolve("env-secret").also { Files.writeString(it, "env-secret\n") }
        starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}")
            .withEnv("SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE" to viaEnv.toString()).registers {
                assertEquals("env-secret", oauth.requests.last().password)
            }
    }

    // --- startup checks ---

    @Test fun `oauth2 together with a static hub token fails the startup naming both`(@TempDir dir: Path) {
        val byToken = failure(starter().withPropertyValues(*oauth2Properties("snodra.messaging.hub.token=abc")))
        assertTrue("snodra.messaging.hub.oauth2.token-uri" in byToken && "snodra.messaging.hub.token" in byToken, byToken)
        val tokenFile = dir.resolve("t").also { Files.writeString(it, "abc") }
        val byFile = failure(starter().withPropertyValues(*oauth2Properties("snodra.messaging.hub.token-file=$tokenFile")))
        assertTrue("snodra.messaging.hub.oauth2.token-uri" in byFile && "token-file" in byFile, byFile)
        val byEnv = failure(starter().withPropertyValues(*oauth2Properties()).withEnv("SNODRA_SERVER_TOKEN" to "abc"))
        assertTrue("SNODRA_SERVER_TOKEN" in byEnv || "snodra.messaging.hub.token" in byEnv, byEnv)
        assertFalse("abc" in byToken + byEnv)
        assertFalse(secret in byToken + byFile + byEnv)
    }

    @Test fun `a token uri without a secret fails the startup, an unreadable secret file too, naming the setting`(@TempDir dir: Path) {
        val none = failure(starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}"))
        assertTrue("client-secret" in none, none)
        val missing = dir.resolve("nope")
        val unreadable = failure(starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}", "snodra.messaging.hub.oauth2.client-secret-file=$missing"))
        assertTrue(missing.toString() in unreadable, unreadable)
        val blank = dir.resolve("blank").also { Files.writeString(it, " \t\r\n") }
        val empty = failure(starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=${oauth.tokenUri}", "snodra.messaging.hub.oauth2.client-secret-file=$blank"))
        assertTrue("snodra.messaging.hub.oauth2.client-secret-file" in empty && blank.toString() in empty && "empty" in empty, empty)
    }

    @Test fun `any oauth2 setting without a token uri fails the startup naming it`(@TempDir dir: Path) {
        val file = dir.resolve("s").also { Files.writeString(it, "x") }
        val strays = mapOf(
            "snodra.messaging.hub.oauth2.client-id=orders" to "snodra.messaging.hub.oauth2.client-id",
            "snodra.messaging.hub.oauth2.client-secret=$secret" to "snodra.messaging.hub.oauth2.client-secret",
            "snodra.messaging.hub.oauth2.client-secret-file=$file" to "snodra.messaging.hub.oauth2.client-secret-file",
            "snodra.messaging.hub.oauth2.scope=snodra" to "snodra.messaging.hub.oauth2.scope",
            "snodra.messaging.hub.oauth2.ca-file=$file" to "snodra.messaging.hub.oauth2.ca-file",
        )
        for ((property, name) in strays) {
            val message = failure(starter().withPropertyValues(property))
            assertTrue(name in message && "token-uri" in message, message)
            assertFalse(secret in message, "the secret never goes into the message")
        }
        val envStrays = listOf(
            "SNODRA_SERVER_OAUTH2_CLIENT_ID" to "orders", "SNODRA_SERVER_OAUTH2_CLIENT_SECRET" to secret,
            "SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE" to file.toString(), "SNODRA_SERVER_OAUTH2_SCOPE" to "snodra", "SNODRA_SERVER_OAUTH2_CA" to file.toString(),
        )
        for ((variable, value) in envStrays) {
            val message = failure(starter().withEnv(variable to value))
            assertTrue(variable in message && "SNODRA_SERVER_OAUTH2_TOKEN_URI" in message, message)
            assertFalse(secret in message)
        }
    }

    @Test fun `a token uri that is not http or https fails the startup`() {
        val chain = failure(starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=ftp://kc/token", "snodra.messaging.hub.oauth2.client-secret=$secret"))
        assertTrue("snodra.messaging.hub.oauth2" in chain && "http" in chain, chain)
    }

    @Test fun `http to a routed host warns once, loopback and single-label hosts and https do not`() {
        fun warnings(uri: String): Int {
            appender.list.clear()
            starter().withPropertyValues("snodra.messaging.hub.oauth2.token-uri=$uri", "snodra.messaging.hub.oauth2.client-secret=$secret").run { assertEquals(null, it.startupFailure) }
            return appender.list.count { it.level == Level.WARN && "http://" in it.formattedMessage }
        }
        assertEquals(1, warnings("http://auth.example.com/realms/snodra/protocol/openid-connect/token"))
        assertEquals(1, warnings("http://10.1.2.3:8080/token"))
        assertEquals(0, warnings("http://oauth2:8080/token"))
        assertEquals(0, warnings("http://localhost:8180/token"))
        assertEquals(0, warnings("http://127.0.0.1:8180/token"))
        assertEquals(0, warnings("https://auth.example.com/token"))
    }

    // --- redaction ---

    @Test fun `toString never prints the client secret and still prints every other field`() {
        propertiesRunner.withPropertyValues(*oauth2Properties()).run { context ->
            val properties = context.getBean(SnodraMessagingProperties::class.java)
            assertFalse(secret in properties.toString(), properties.toString())
            assertFalse(secret in properties.hub.oauth2.toString())
            assertTrue("****" in properties.hub.oauth2.toString())
            assertTrue("oauth2=" in properties.hub.toString())
            for (field in SnodraMessagingProperties.OAuth2::class.memberProperties) {
                assertTrue(field.name + "=" in properties.hub.oauth2.toString(), "OAuth2.toString misses ${field.name}")
            }
        }
    }

    private val mapper = JsonMapper.builder().build()

    @Test fun `the actuator configprops and env output never show the client secret, nor the plain variable's, even with show-values always`() {
        val variableSecret = "env-secret-" + UUID.randomUUID()
        starter().withPropertyValues(*oauth2Properties()).withEnv("SNODRA_SERVER_OAUTH2_CLIENT_SECRET" to variableSecret).run { context ->
            assertEquals(null, context.startupFailure)
            val functions = context.getBeansOfType(SanitizingFunction::class.java).values.toList()
            val configprops = ConfigurationPropertiesReportEndpoint(functions, Show.ALWAYS).also { it.setApplicationContext(context) }
            val configJson = mapper.writeValueAsString(configprops.configurationProperties())
            assertTrue("oauth2" in configJson && "token-uri" in configJson || "tokenUri" in configJson, "the report lists the oauth2 properties")
            assertFalse(secret in configJson, "configprops")
            val env = EnvironmentEndpoint(context.environment as Environment, functions, Show.ALWAYS)
            val envJson = mapper.writeValueAsString(env.environment(null))
            assertTrue("snodra.messaging.hub.oauth2.client-secret" in envJson, "the env report lists the key")
            assertFalse(secret in envJson, "env")
            assertFalse(variableSecret in envJson, "env, SNODRA_SERVER_OAUTH2_CLIENT_SECRET")
        }
    }

    @Test fun `the sanitizing function leaves the token uri and the secret file path visible`() {
        starter().run { context ->
            val function = context.getBean(SanitizingFunction::class.java)
            for (key in listOf("snodra.messaging.hub.oauth2.token-uri", "snodra.messaging.hub.oauth2.client-secret-file", "SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE", "snodra.messaging.hub.oauth2.client-id")) {
                assertEquals("v", function.apply(SanitizableData(null, key, "v")).value, key)
            }
            for (key in listOf("snodra.messaging.hub.oauth2.client-secret", "SNODRA_MESSAGING_HUB_OAUTH2_CLIENT_SECRET", "SNODRA_SERVER_OAUTH2_CLIENT_SECRET")) {
                assertEquals(SanitizableData.SANITIZED_VALUE, function.apply(SanitizableData(null, key, "v")).value, key)
            }
        }
    }

    // --- what the library reports ---

    @Test fun `the auth status says none, token or jwt, with the token's expiry and the last fetch error`() {
        starter().run { context -> assertEquals(HubAuthKind.NONE, context.getBean(HubConnections::class.java).authStatus().kind) }
        hub.auth = HubAuth.OFF
        starter().withPropertyValues("snodra.messaging.hub.token=abc").run { context ->
            assertEquals(HubAuthKind.TOKEN, context.getBean(HubConnections::class.java).authStatus().kind)
        }
        hub.auth = HubAuth.enforce(emptyMap(), listOf(AclEntry("orders", handle = listOf("test.*"), send = listOf("*"))))
        starter().withPropertyValues(*oauth2Properties()).registers { connections ->
            val status = connections.authStatus()
            assertEquals(HubAuthKind.JWT, status.kind)
            assertNotNull(status.tokenExpiresAt)
            assertNull(status.lastTokenError)
            assertEquals(1L, status.tokenFetches["ok"])
        }
    }

    @Test fun `the token fetches and the credential refreshes are bound as Micrometer counters named after the documented metrics`() {
        starter().withPropertyValues(*oauth2Properties()).run { context ->
            val connections = context.getBean(HubConnections::class.java)
            await atMost Duration.ofSeconds(10) until { hub.acksSentTo(connections.instanceId) >= 1 }
            val registry = SimpleMeterRegistry()
            context.getBean(MeterBinder::class.java).bindTo(registry) // Boot's registry post-processor does this in an application
            assertEquals(1.0, registry.get("snodra.hub.client.token.fetches").tag("result", "ok").functionCounter().count())
            for (result in listOf("http_error", "oauth_error", "io_error", "bad_response")) {
                assertEquals(0.0, registry.get("snodra.hub.client.token.fetches").tag("result", result).functionCounter().count(), result)
            }
            assertEquals(0.0, registry.get("snodra.hub.client.credentials.refreshes.sent").functionCounter().count())
            assertEquals(1.0, registry.get("snodra.hub.client.connected.hubs").gauge().value())
            assertEquals(0.0, registry.get("snodra.hub.client.handlers.in.flight").gauge().value())
        }
    }

    @Test fun `the configuration metadata lists the oauth2 keys`() {
        val json = checkNotNull(javaClass.getResourceAsStream("/META-INF/spring-configuration-metadata.json")).use { String(it.readAllBytes()) }
        for (key in listOf("token-uri", "client-id", "client-secret", "client-secret-file", "scope", "ca-file")) {
            assertTrue("\"snodra.messaging.hub.oauth2.$key\"" in json, key)
        }
    }
}
