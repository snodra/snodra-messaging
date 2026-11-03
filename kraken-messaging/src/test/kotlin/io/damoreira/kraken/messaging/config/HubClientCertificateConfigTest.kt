package io.damoreira.kraken.messaging.config

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.messaging.fixtures.TestPki
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubClientCertificate
import io.damoreira.kraken.messaging.test.InProcessHub
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.env.SystemEnvironmentPropertySource
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** P5c client configuration: `hub.tls.client-cert-file` / `client-key-file`, their environment names and the startup checks (ADR 0020). */
class HubClientCertificateConfigTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KrakenMessagingProperties::class)
    class PropertiesOnly

    @TempDir lateinit var dir: Path
    private val hub = InProcessHub().start()
    private val ca by lazy { TestPki.ca(dir.resolve("ca")) }
    private val certFile get() = dir.resolve("client.pem")
    private val keyFile get() = dir.resolve("client.key")

    private val log = LoggerFactory.getLogger(HubAutoConfiguration::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun capture() {
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() {
        log.detachAppender(appender)
        hub.close()
    }

    private val required = arrayOf("kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=test")
    private val propertiesRunner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java).withPropertyValues(*required)

    private fun ApplicationContextRunner.withEnv(vararg pairs: Pair<String, Any>) = withInitializer { context ->
        context.environment.propertySources.addFirst(
            SystemEnvironmentPropertySource("test-" + StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, mapOf(*pairs)),
        )
    }

    private fun starter() = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KrakenMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, KafkaEventsAutoConfiguration::class.java))
        .withPropertyValues(*required, "kraken.messaging.hub.enabled=true", "kraken.messaging.kafka.consumer.enabled=false", "kraken.messaging.outbox.enabled=false")
        .withBean("krakenHubChannelFactory", HubChannelFactory::class.java, { HubChannelFactory { hub.channel() } })

    private fun pair(service: String = "orders") = ca.issue(service).also { it.writeTo(certFile, keyFile) }

    private fun files() = arrayOf("kraken.messaging.hub.tls.client-cert-file=$certFile", "kraken.messaging.hub.tls.client-key-file=$keyFile")

    private fun failure(runner: ApplicationContextRunner): String {
        var chain = ""
        runner.run { context ->
            chain = generateSequence(assertNotNull(context.startupFailure, "the context should not start") as Throwable?) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
        }
        return chain
    }

    // --- binding ---

    @Test fun `no client certificate by default`() {
        propertiesRunner.run { context ->
            val tls = context.getBean(KrakenMessagingProperties::class.java).hub.tls
            assertNull(tls.clientCertFile)
            assertNull(tls.clientKeyFile)
        }
    }

    @Test fun `client-cert-file and client-key-file bind from properties and from the relaxed environment names`() {
        propertiesRunner.withPropertyValues(*files()).run { context ->
            val tls = context.getBean(KrakenMessagingProperties::class.java).hub.tls
            assertEquals(certFile, tls.clientCertFile)
            assertEquals(keyFile, tls.clientKeyFile)
        }
        propertiesRunner.withEnv("KRAKEN_MESSAGING_HUB_TLS_CLIENT_CERT_FILE" to "/certs/c.pem", "KRAKEN_MESSAGING_HUB_TLS_CLIENT_KEY_FILE" to "/certs/c.key").run { context ->
            val tls = context.getBean(KrakenMessagingProperties::class.java).hub.tls
            assertEquals(Path.of("/certs/c.pem"), tls.clientCertFile)
            assertEquals(Path.of("/certs/c.key"), tls.clientKeyFile)
        }
    }

    @Test fun `the certificate without the key, or the key without the certificate, fails the startup naming the missing one`() {
        val noKey = failure(propertiesRunner.withPropertyValues("kraken.messaging.hub.tls.client-cert-file=$certFile"))
        assertTrue("kraken.messaging.hub.tls.client-key-file" in noKey, noKey)
        val noCert = failure(propertiesRunner.withPropertyValues("kraken.messaging.hub.tls.client-key-file=$keyFile"))
        assertTrue("kraken.messaging.hub.tls.client-cert-file" in noCert, noCert)
    }

    @Test fun `the configuration metadata lists the new keys`() {
        val json = checkNotNull(javaClass.getResourceAsStream("/META-INF/spring-configuration-metadata.json")).use { String(it.readAllBytes()) }
        for (key in listOf("hub.tls.client-cert-file", "hub.tls.client-key-file")) assertTrue("\"kraken.messaging.$key\"" in json, key)
    }

    // --- startup checks ---

    @Test fun `a good pair starts the context and is shared as a bean, with no TLS flag needed`() {
        val issued = pair()
        starter().withPropertyValues(*files()).run { context ->
            assertEquals(null, context.startupFailure)
            val certificate = context.getBean(HubClientCertificate::class.java)
            assertEquals(issued.fingerprint, certificate.current.fingerprint)
            assertEquals("orders", certificate.current.service)
        }
    }

    @Test fun `without the settings there is no certificate bean`() {
        starter().run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(HubClientCertificate::class.java).isEmpty())
        }
    }

    @Test fun `a client certificate with the tls flag off is TLS, not a plain-text warning`() {
        pair()
        starter().withPropertyValues(*files(), "kraken.messaging.hub.tls.ca-file=${ca.pem}").run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(appender.list.none { it.level == Level.WARN && "plain text" in it.formattedMessage }, appender.list.map { it.formattedMessage }.toString())
        }
    }

    @Test fun `a client certificate together with a static token names both settings`() {
        pair()
        for (setting in listOf("kraken.messaging.hub.token=abc", "kraken.messaging.hub.token-file=${dir.resolve("t")}")) {
            val message = failure(starter().withPropertyValues(*files(), setting))
            assertTrue("kraken.messaging.hub.tls.client-cert-file" in message && setting.substringBefore('=') in message, message)
        }
        val env = failure(starter().withPropertyValues(*files()).withEnv("KRAKEN_HUB_TOKEN" to "abc"))
        assertTrue("client-cert-file" in env && "KRAKEN_HUB_TOKEN" in env, env)
        val envFile = failure(starter().withPropertyValues(*files()).withEnv("KRAKEN_HUB_TOKEN_FILE" to "/run/secrets/t"))
        assertTrue("client-cert-file" in envFile && "KRAKEN_HUB_TOKEN_FILE" in envFile, envFile)
    }

    @Test fun `a client certificate together with OAuth2 names both settings`() {
        pair()
        val property = failure(starter().withPropertyValues(*files(), "kraken.messaging.hub.oauth2.token-uri=http://idp/token", "kraken.messaging.hub.oauth2.client-secret=s"))
        assertTrue("client-cert-file" in property && "kraken.messaging.hub.oauth2" in property, property)
        val env = failure(starter().withPropertyValues(*files()).withEnv("KRAKEN_HUB_OAUTH2_TOKEN_URI" to "http://idp/token", "KRAKEN_HUB_OAUTH2_CLIENT_SECRET" to "s"))
        assertTrue("client-cert-file" in env && "KRAKEN_HUB_OAUTH2_TOKEN_URI" in env, env)
    }

    @Test fun `an unreadable certificate file, a mismatching key, no service SAN and another service's name each fail the startup`() {
        pair()
        Files.delete(certFile)
        assertTrue("client-cert-file" in failure(starter().withPropertyValues(*files())))

        pair("orders")
        Files.writeString(keyFile, ca.issue("orders").keyPem)
        assertTrue("does not match" in failure(starter().withPropertyValues(*files())))

        ca.issue(service = null).writeTo(certFile, keyFile)
        assertTrue("kraken://service/" in failure(starter().withPropertyValues(*files())))

        pair("billing")
        val other = failure(starter().withPropertyValues(*files()))
        assertTrue("billing" in other && "orders" in other, other)
    }

    @Test fun `an unreadable client key file fails the startup naming it, no key content in the failure`() {
        val issued = pair()
        Files.writeString(keyFile, issued.keyPem.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN RSA PRIVATE KEY-----").replace("-----END PRIVATE KEY-----", "-----END RSA PRIVATE KEY-----"))
        val message = failure(starter().withPropertyValues(*files()))
        assertTrue("client-key-file" in message && "PKCS#8" in message, message)
        val body = issued.keyPem.lines().filter { !it.startsWith("-----") }.joinToString("")
        assertTrue(body.take(30) !in message)
    }
}
