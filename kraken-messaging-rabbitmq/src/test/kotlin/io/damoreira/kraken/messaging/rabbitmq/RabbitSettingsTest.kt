package io.damoreira.kraken.messaging.rabbitmq

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

class RabbitSettingsTest {
    @Test
    fun `the defaults are the Rust add-on's`() {
        val s = RabbitProperties().toSettings()
        assertEquals(listOf("rabbitmq" to 5552), s.nodes())
        assertEquals("/", s.virtualHost)
        assertEquals("guest" to "guest", s.username to s.password)
        assertFalse(s.tls)
        assertFalse(s.loadBalancer)
        assertEquals(10_000L, s.offsetStoreMessages)
        assertEquals(Duration.ofSeconds(5), s.offsetStoreInterval)
        assertEquals(Duration.ofSeconds(30), s.confirmTimeout)
        assertNull(s.management)
    }

    @Test
    fun `hosts with or without a port, the TLS port by default with TLS`(@TempDir dir: Path) {
        val ca = Files.writeString(dir.resolve("ca.pem"), "x")
        assertEquals(listOf("a" to 5552, "b" to 6000), RabbitEventStoreSettings(hosts = listOf("a", " b:6000 ")).nodes())
        assertEquals(listOf("a" to 7000), RabbitEventStoreSettings(hosts = listOf("a"), port = 7000).nodes())
        assertEquals(listOf("a" to 5551), RabbitEventStoreSettings(hosts = listOf("a"), tls = true, tlsCaFile = ca).nodes())
    }

    @Test
    fun `a bad configuration is refused`(@TempDir dir: Path) {
        for (bad in listOf(
            { RabbitEventStoreSettings(hosts = emptyList()) },
            { RabbitEventStoreSettings(hosts = listOf(" ")) },
            { RabbitEventStoreSettings(hosts = listOf("h:port")) },
            { RabbitEventStoreSettings(tls = true) }, // TLS without a CA file: never unverified
            { RabbitEventStoreSettings(offsetStoreMessages = 0) },
            { RabbitEventStoreSettings(offsetStoreInterval = Duration.ZERO) },
            { RabbitEventStoreSettings(confirmTimeout = Duration.ZERO) },
            { RabbitEventStoreSettings(management = RabbitManagementSettings("ftp://h:15672")) },
            { RabbitEventStoreSettings(management = RabbitManagementSettings("http://user:secret@h:15672")) },
        )) assertThrows(IllegalArgumentException::class.java) { bad() }
        val message = runCatching { RabbitManagementSettings("http://user:secret@h:15672") }.exceptionOrNull()!!.message!!
        assertFalse("secret" in message, message)
    }

    @Test
    fun `the password file wins over the setting, its trailing newline removed`(@TempDir dir: Path) {
        val file = Files.writeString(dir.resolve("password"), "from-file\n")
        assertEquals("from-file", RabbitProperties(passwordFile = file).toSettings().password)
        assertEquals("from-file", RabbitProperties(password = "set", passwordFile = file).toSettings().password, "the file wins")
        assertEquals("set", RabbitProperties(password = "set").toSettings().password)
        val both = RabbitProperties(
            management = RabbitProperties.Management(url = "http://h:15672", username = "admin", password = "set", passwordFile = file),
        ).toSettings().management!!
        assertEquals("admin" to "from-file", both.login("guest", "guest"), "the management password file wins too")
        val management = RabbitProperties(
            username = "stream-user",
            password = "stream-secret",
            management = RabbitProperties.Management(url = "http://h:15672"),
        ).toSettings().management!!
        assertEquals("stream-user" to "stream-secret", management.login("stream-user", "stream-secret"), "the stream user's by default")
        val own = RabbitProperties(
            management = RabbitProperties.Management(url = "http://h:15672", username = "admin", passwordFile = file),
        ).toSettings().management!!
        assertEquals("admin" to "from-file", own.login("guest", "guest"))
        assertNull(RabbitProperties(management = RabbitProperties.Management(url = " ")).toSettings().management, "a blank URL: no management API")
    }

    @Test
    fun `a password file is trimmed at both ends, as in the Rust add-on`(@TempDir dir: Path) {
        val file = Files.writeString(dir.resolve("password"), " \tfrom-file\r\n\n\r\n")
        assertEquals("from-file", RabbitProperties(passwordFile = file).toSettings().password)
        val management = RabbitProperties(
            management = RabbitProperties.Management(url = "http://h:15672", username = "admin", passwordFile = file),
        ).toSettings().management!!
        assertEquals("admin" to "from-file", management.login("guest", "guest"))
    }

    @Test
    fun `a password file empty after trimming fails naming the setting and the file`(@TempDir dir: Path) {
        val file = Files.writeString(dir.resolve("password"), " \t\r\n")
        val stream = assertThrows(IllegalStateException::class.java) { RabbitProperties(password = "set", passwordFile = file).toSettings() }
        assertTrue("kraken.messaging.rabbitmq.password-file" in stream.message!! && file.toString() in stream.message!!, stream.message)
        val api = assertThrows(IllegalStateException::class.java) {
            RabbitProperties(management = RabbitProperties.Management(url = "http://h:15672", passwordFile = file)).toSettings()
        }
        assertTrue("kraken.messaging.rabbitmq.management.password-file" in api.message!! && file.toString() in api.message!!, api.message)
    }

    @Test
    fun `secrets never appear in the settings' text`() {
        val s = RabbitProperties(password = "stream-secret", management = RabbitProperties.Management(url = "http://h:15672", password = "api-secret")).toSettings()
        for (text in listOf(s.toString(), s.management.toString(), RabbitProperties(password = "stream-secret").toString())) {
            assertFalse("stream-secret" in text || "api-secret" in text, text)
        }
        assertTrue("http://h:15672" in s.toString())
    }
}
