package com.damoreira.snodra.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.damoreira.snodra.messaging.fixtures.TestPki
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The client certificate pair of `hub.tls.client-cert-file` / `client-key-file`: what is checked at startup, how a change is adopted. */
class HubClientCertificateTest {
    @TempDir lateinit var dir: Path
    private lateinit var ca: TestPki.Ca
    private val certFile get() = dir.resolve("client.pem")
    private val keyFile get() = dir.resolve("client.key")

    private val log = LoggerFactory.getLogger(HubClientCertificate::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private fun lines(level: Level) = appender.list.filter { it.level == level }.map { it.formattedMessage }

    @BeforeEach fun setUp() {
        ca = TestPki.ca(dir.resolve("ca"))
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() { log.detachAppender(appender) }

    private fun issue(service: String? = "billing-service", validFor: Duration = Duration.ofHours(1), write: Boolean = true, notBefore: Instant = Instant.now().minusSeconds(60)) =
        ca.issue(service, validFor = validFor, notBefore = notBefore).also { if (write) it.writeTo(certFile, keyFile) }

    private fun load(service: String = "billing-service", clock: () -> Instant = Instant::now) = HubClientCertificate(certFile, keyFile, service, clock)

    private fun problem(service: String = "billing-service"): String = assertFailsWith<IllegalStateException> { load(service) }.message!!

    @Test fun `a valid pair loads with its service, fingerprint prefix and validity`() {
        val issued = issue()
        val certificate = load().current
        assertEquals("billing-service", certificate.service)
        assertEquals(issued.fingerprint, certificate.fingerprint)
        assertEquals(8, certificate.fingerprint.length)
        assertEquals(issued.certificate.notAfter.toInstant(), certificate.notAfter)
        assertEquals(issued.certificate.notBefore.toInstant(), certificate.notBefore)
    }

    @Test fun `a chain file with the leaf first and a CA after it is read as the chain`() {
        val issued = issue(write = false)
        Files.writeString(certFile, issued.certificatePem + Files.readString(ca.pem))
        Files.writeString(keyFile, issued.keyPem)
        assertEquals(issued.fingerprint, load().current.fingerprint)
    }

    @Test fun `a certificate file that is missing fails the startup naming the setting`() {
        issue().also { Files.delete(certFile) }
        val message = problem()
        assertTrue("snodra.messaging.hub.tls.client-cert-file" in message && certFile.toString() in message, message)
    }

    @Test fun `a key file that is missing fails the startup naming the setting`() {
        issue().also { Files.delete(keyFile) }
        val message = problem()
        assertTrue("snodra.messaging.hub.tls.client-key-file" in message && keyFile.toString() in message, message)
    }

    @Test fun `an empty certificate file or key file fails the startup`() {
        issue().also { Files.writeString(certFile, "") }
        assertTrue("client-cert-file" in problem() && "empty" in problem())
        issue().also { Files.writeString(keyFile, "\n") }
        assertTrue("client-key-file" in problem() && "empty" in problem())
    }

    @Test fun `a file that is not a PEM certificate fails the startup`() {
        issue().also { Files.writeString(certFile, "-----BEGIN CERTIFICATE-----\nnot base64 at all\n-----END CERTIFICATE-----\n") }
        assertTrue("client-cert-file" in problem(), problem())
    }

    @Test fun `a key that does not belong to the certificate fails the startup`() {
        val issued = issue()
        val other = issue("billing-service", write = false)
        Files.writeString(keyFile, other.keyPem)
        val message = problem()
        assertTrue("does not match" in message, message)
        assertEquals(issued.fingerprint.length, 8)
    }

    @Test fun `a garbage key fails the startup naming the key setting`() {
        issue().also { Files.writeString(keyFile, "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n") }
        assertTrue("client-key-file" in problem(), problem())
    }

    @Test fun `PKCS#1, SEC1 and encrypted keys are refused with the supported format named`() {
        for (type in listOf("RSA PRIVATE KEY", "EC PRIVATE KEY", "ENCRYPTED PRIVATE KEY")) {
            issue().also { Files.writeString(keyFile, "-----BEGIN $type-----\nAAAA\n-----END $type-----\n") }
            val message = problem()
            assertTrue("PKCS#8" in message && "client-key-file" in message, "$type: $message")
        }
    }

    @Test fun `a certificate without a service SAN fails the startup, also with only DNS and foreign URIs`() {
        issue(service = null)
        assertTrue("snodra://service/" in problem(), problem())
        ca.issue(uris = listOf("spiffe://example.org/billing"), dns = listOf("billing-service")).writeTo(certFile, keyFile)
        assertTrue("snodra://service/" in problem(), problem())
    }

    @Test fun `a certificate that names two services fails the startup, even when both are the same`() {
        ca.issue(uris = listOf("snodra://service/billing-service", "snodra://service/orders-service")).writeTo(certFile, keyFile)
        assertTrue("more than one" in problem(), problem())
        ca.issue(uris = listOf("snodra://service/billing-service", "snodra://service/billing-service")).writeTo(certFile, keyFile)
        assertTrue("more than one" in problem(), problem())
    }

    @Test fun `a certificate for another service than the configured one fails the startup naming both`() {
        issue("orders-service")
        val message = problem("billing-service")
        assertTrue("orders-service" in message && "billing-service" in message && "service-name" in message, message)
    }

    @Test fun `a service name outside the hub's alphabet is not a service`() {
        ca.issue(uris = listOf("snodra://service/billing/service")).writeTo(certFile, keyFile)
        assertTrue("snodra://service/" in problem(), problem())
    }

    @Test fun `other SANs next to the one service SAN are ignored`() {
        ca.issue(uris = listOf("spiffe://example.org/billing", "snodra://service/billing-service"), dns = listOf("billing.internal")).writeTo(certFile, keyFile)
        assertEquals("billing-service", load().current.service)
    }

    @Test fun `neither the certificate object nor any log line carries key material`() {
        val issued = issue()
        val certificate = load().current
        val keyBody = issued.keyPem.lines().filter { !it.startsWith("-----") }.joinToString("")
        assertFalse(keyBody.take(40) in certificate.toString(), certificate.toString())
        assertFalse(appender.list.any { keyBody.take(40) in it.formattedMessage || keyBody.take(40) in it.throwableProxy?.message.orEmpty() })
        assertTrue(appender.list.any { issued.fingerprint in it.formattedMessage && "billing-service" in it.formattedMessage }, "the load is logged by fingerprint and service")
    }

    @Test fun `refresh keeps the pair when the files did not change`() {
        issue()
        val source = load()
        val first = source.current
        assertEquals(first.fingerprint, source.refresh().fingerprint)
        assertEquals(0, source.generation)
        assertEquals(0, source.rotations)
    }

    @Test fun `refresh adopts a changed valid pair and counts the rotation`() {
        val first = issue()
        val source = load()
        val second = issue()
        assertTrue(first.fingerprint != second.fingerprint)
        assertEquals(second.fingerprint, source.refresh().fingerprint)
        assertEquals(second.fingerprint, source.current.fingerprint)
        assertEquals(1, source.generation)
        assertEquals(1, source.rotations)
        assertEquals(second.fingerprint, source.refresh().fingerprint)
        assertEquals(1, source.generation, "unchanged again")
    }

    @Test fun `refresh keeps the current pair when the new one is invalid, logging at debug first and one ERROR once the problem persists for a second check`() {
        val first = issue()
        val source = load()
        Files.writeString(certFile, "garbage")
        assertEquals(first.fingerprint, source.refresh().fingerprint)
        assertEquals(emptyList(), lines(Level.ERROR), "a pair replaced in two writes is half-written for a moment: the first sighting is not an error")
        repeat(3) { assertEquals(first.fingerprint, source.refresh().fingerprint) }
        assertEquals(1, lines(Level.ERROR).size, lines(Level.ERROR).toString())
        assertEquals(0, source.generation)
        ca.issue("orders-service").writeTo(certFile, keyFile) // a different problem: another service
        repeat(2) { source.refresh() }
        assertEquals(2, lines(Level.ERROR).size, lines(Level.ERROR).toString())
        val renewed = issue()
        assertEquals(renewed.fingerprint, source.refresh().fingerprint) // fixed: adopted
        Files.writeString(certFile, "garbage")
        repeat(2) { source.refresh() }
        assertEquals(3, lines(Level.ERROR).size, "the same problem after a recovery is news again")
    }

    @Test fun `a refresh at connect time that finds an invalid pair falls back to the last good one with one WARN per problem`() {
        val first = issue()
        val source = load()
        Files.writeString(certFile, "garbage")
        assertEquals(first.fingerprint, source.refresh(connecting = true).fingerprint)
        val warnings = lines(Level.WARN)
        assertEquals(1, warnings.size, warnings.toString())
        assertTrue(first.fingerprint in warnings[0], warnings[0])
        assertEquals(emptyList(), lines(Level.ERROR), "the ERROR waits for a second check, as for the tick")
        repeat(2) { assertEquals(first.fingerprint, source.refresh(connecting = true).fingerprint) }
        assertEquals(1, lines(Level.WARN).size, "one WARN per problem")
        assertEquals(1, lines(Level.ERROR).size)
    }

    @Test fun `a half-written replacement (new certificate, old key) is not adopted until the pair matches`() {
        val first = issue()
        val source = load()
        val next = issue(write = false)
        Files.writeString(certFile, next.certificatePem) // the key is still the old one
        assertEquals(first.fingerprint, source.refresh().fingerprint)
        Files.writeString(keyFile, next.keyPem)
        assertEquals(next.fingerprint, source.refresh().fingerprint)
    }

    @Test fun `a replacement that is already expired or not yet valid is not adopted`() {
        val first = issue()
        val source = load()
        issue(validFor = Duration.ofMinutes(1), notBefore = Instant.now().minusSeconds(3600))
        repeat(2) { assertEquals(first.fingerprint, source.refresh().fingerprint) }
        assertTrue(lines(Level.ERROR).any { "expired" in it }, lines(Level.ERROR).toString())
        issue(notBefore = Instant.now().plusSeconds(3600))
        repeat(2) { assertEquals(first.fingerprint, source.refresh().fingerprint) }
        assertTrue(lines(Level.ERROR).any { "not valid before" in it }, lines(Level.ERROR).toString())
    }

    @Test fun `with less than a tenth of the lifetime left and no renewed file it warns once per certificate`() {
        issue(validFor = Duration.ofHours(1))
        var now = Instant.now()
        val source = load { now }
        val notAfter = source.current.notAfter
        now = notAfter.minus(Duration.ofMinutes(7)) // 12 % of the hour left: quiet
        source.refresh()
        assertEquals(emptyList(), lines(Level.WARN))
        now = notAfter.minus(Duration.ofMinutes(5)) // 8 %
        repeat(3) { source.refresh() }
        val warnings = lines(Level.WARN)
        assertEquals(1, warnings.size, warnings.toString())
        assertTrue(source.current.fingerprint in warnings[0] && "billing-service" in warnings[0], warnings[0])
        issue(validFor = Duration.ofHours(3)) // renewed: adopted, no more warning for the old one; the new one has its own lifetime ahead
        source.refresh()
        now = now.plusSeconds(1)
        source.refresh()
        assertEquals(1, lines(Level.WARN).size)
    }

    @Test fun `a long-lived certificate warns 24 hours before its end, not a tenth of its life`() {
        issue(validFor = Duration.ofDays(365))
        var now = Instant.now()
        val source = load { now }
        val notAfter = source.current.notAfter
        now = notAfter.minus(Duration.ofHours(25))
        source.refresh()
        assertEquals(emptyList(), lines(Level.WARN))
        now = notAfter.minus(Duration.ofHours(23))
        source.refresh()
        assertEquals(1, lines(Level.WARN).size)
    }

    @Test fun `a certificate that has ended is reported by expired(), kept, and not logged by the files check`() {
        issue(validFor = Duration.ofHours(1))
        var now = Instant.now()
        val source = load { now }
        assertFalse(source.expired())
        now = source.current.notAfter.plusSeconds(5)
        repeat(3) { source.refresh() }
        assertTrue(source.expired())
        assertEquals(emptyList(), lines(Level.ERROR), "the connection reports it, through the rejection path")
        assertEquals(0, source.generation)
    }

    @Test fun `a pair that has already ended at startup fails the startup`() {
        issue(validFor = Duration.ofMinutes(1), notBefore = Instant.now().minusSeconds(3600))
        val message = problem()
        assertTrue("expired" in message && "client-cert-file" in message, message)
    }
}
