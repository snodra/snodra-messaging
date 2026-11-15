package io.damoreira.kraken.messaging.rabbitmq

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.endpoint.SanitizableData
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.actuate.endpoint.Show
import org.springframework.boot.actuate.env.EnvironmentEndpoint
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.Environment
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

/** The add-on's secrets stay out of the actuator's output and off the network in clear. */
class RabbitSecretsTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KrakenMessagingAutoConfiguration::class.java, RabbitEventStoreAutoConfiguration::class.java))
        .withPropertyValues("kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=orders", "kraken.messaging.rabbitmq.hosts=127.0.0.1:1")

    private fun function(context: org.springframework.context.ApplicationContext): SanitizingFunction =
        context.getBeansOfType(SanitizingFunction::class.java).values.single { it.apply(SanitizableData(null, "kraken.messaging.rabbitmq.password", "x")).value != "x" }

    @Test
    fun `the passwords are masked in every form, the files and other settings are not`() {
        runner.run { context ->
            val function = function(context)
            for (key in listOf(
                "kraken.messaging.rabbitmq.password", "KRAKEN_MESSAGING_RABBITMQ_PASSWORD",
                "kraken.messaging.rabbitmq.management.password", "KRAKEN_MESSAGING_RABBITMQ_MANAGEMENT_PASSWORD",
            )) assertEquals(SanitizableData.SANITIZED_VALUE, function.apply(SanitizableData(null, key, "v")).value, key)
            for (key in listOf(
                "kraken.messaging.rabbitmq.password-file", "kraken.messaging.rabbitmq.management.password-file",
                "kraken.messaging.rabbitmq.username", "kraken.messaging.rabbitmq.management.url", "kraken.messaging.hub.address",
            )) assertEquals("v", function.apply(SanitizableData(null, key, "v")).value, key)
        }
    }

    @Test
    fun `the env report never shows them, even with show-values always`() {
        val password = "stream-" + UUID.randomUUID()
        val api = "api-" + UUID.randomUUID()
        runner.withPropertyValues("kraken.messaging.rabbitmq.password=$password", "kraken.messaging.rabbitmq.management.password=$api").run { context ->
            val functions = context.getBeansOfType(SanitizingFunction::class.java).values.toList()
            val env = EnvironmentEndpoint(context.environment as Environment, functions, Show.ALWAYS)
            val json = JsonMapper.builder().build().writeValueAsString(env.environment(null))
            assertTrue("kraken.messaging.rabbitmq.password" in json, "the report lists the key")
            assertFalse(password in json || api in json, json)
        }
    }

    @Test
    fun `a management API on plain http warns that its credentials cross in clear`() {
        val logger = LoggerFactory.getLogger(RabbitManagement::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        try {
            RabbitManagement(RabbitManagementSettings("http://rabbit:15672"), "u", "secret-p", "/").close()
            RabbitManagement(RabbitManagementSettings("https://rabbit:15671"), "u", "secret-p", "/").close()
            val warns = appender.list.filter { it.level == Level.WARN }
            assertEquals(1, warns.size, warns.map { it.formattedMessage }.toString())
            assertTrue("http://rabbit:15672" in warns.single().formattedMessage && "https" in warns.single().formattedMessage, warns.single().formattedMessage)
            assertFalse("secret-p" in warns.single().formattedMessage)
        } finally {
            logger.detachAppender(appender)
        }
    }
}
