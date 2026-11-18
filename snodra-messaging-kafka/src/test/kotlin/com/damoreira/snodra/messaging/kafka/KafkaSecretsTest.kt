package com.damoreira.snodra.messaging.kafka

import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.actuate.endpoint.SanitizableData
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.actuate.endpoint.Show
import org.springframework.boot.actuate.env.EnvironmentEndpoint
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.env.Environment
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

/** Kafka client properties (a `sasl.jaas.config` holds a secret) stay out of the actuator's output and of the settings' text. */
class KafkaSecretsTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java, KafkaEventStoreAutoConfiguration::class.java))
        .withPropertyValues("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=orders", "snodra.messaging.kafka.bootstrap-servers=localhost:9")

    private fun function(context: org.springframework.context.ApplicationContext): SanitizingFunction =
        context.getBeansOfType(SanitizingFunction::class.java).values
            .single { it.apply(SanitizableData(null, "snodra.messaging.kafka.client-properties.sasl.jaas.config", "x")).value != "x" }

    @Test
    fun `client properties and partner source properties are masked in every form, other settings are not`() {
        runner.run { context ->
            val function = function(context)
            for (key in listOf(
                "snodra.messaging.kafka.client-properties.sasl.jaas.config", "snodra.messaging.kafka.clientProperties.sasl.jaas.config",
                "snodra.messaging.kafka.client-properties[sasl.jaas.config]", "SNODRA_MESSAGING_KAFKA_CLIENTPROPERTIES_SASL_JAAS_CONFIG",
                "snodra.messaging.kafka.client-properties", "snodra.messaging.external.kafka.sources.printer.properties.sasl.jaas.config",
            )) assertEquals(SanitizableData.SANITIZED_VALUE, function.apply(SanitizableData(null, key, "v")).value, key)
            for (key in listOf(
                "snodra.messaging.kafka.bootstrap-servers", "snodra.messaging.external.kafka.sources.printer.topic", "snodra.messaging.hub.address",
            )) assertEquals("v", function.apply(SanitizableData(null, key, "v")).value, key)
        }
    }

    @Test
    fun `the env report never shows a client property's value, even with show-values always`() {
        val jaas = "jaas-" + UUID.randomUUID()
        runner.withPropertyValues("snodra.messaging.kafka.client-properties.sasl.jaas.config=$jaas").run { context ->
            val functions = context.getBeansOfType(SanitizingFunction::class.java).values.toList()
            val env = EnvironmentEndpoint(context.environment as Environment, functions, Show.ALWAYS)
            val json = JsonMapper.builder().build().writeValueAsString(env.environment(null))
            assertTrue("sasl.jaas.config" in json, "the report lists the key")
            assertFalse(jaas in json, json)
        }
    }

    @Test
    fun `a partner source prints its property names only`() {
        val source = KafkaExternalProperties.Source(topic = "printer.events", properties = mapOf("sasl.jaas.config" to "secret-jaas"))
        val text = source.toString()
        assertTrue("sasl.jaas.config" in text && "printer.events" in text, text)
        assertFalse("secret-jaas" in text, text)
        assertFalse("secret-jaas" in KafkaExternalProperties(mapOf("printer" to source)).toString())
    }
}
