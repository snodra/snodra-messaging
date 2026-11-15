package io.damoreira.kraken.messaging.rabbitmq

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** The client the add-on requires: exactly 1.13.1, with every member it uses beyond the public API. */
class RabbitClientCheckTest {
    @Test
    fun `the client on the classpath passes the check`() {
        assertEquals("1.13.1", RabbitClientCheck.found, "the jar's Implementation-Version")
        RabbitClientCheck.verify()
    }

    @Test
    fun `another version fails naming the property that fixes it`() {
        val failure = assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify("1.6.0") }
        for (text in listOf("1.13.1 exactly", "1.6.0", "extra[\"rabbit-stream-client.version\"] = \"1.13.1\"", "<rabbit-stream-client.version>1.13.1</rabbit-stream-client.version>")) {
            assertTrue(text in failure.message!!, failure.message)
        }
        assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify(null) }
    }

    @Test
    fun `a member missing from the client fails the check`() {
        val missing = RabbitClientCheck.Member(com.rabbitmq.stream.impl.Client::class.java, "serverAdvertisedHostname", emptyList(), declared = true)
        val failure = assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify(RabbitClientCheck.REQUIRED, RabbitClientCheck.USED + missing) }
        assertTrue("serverAdvertisedHostname" in failure.message!!, failure.message)
    }
}
