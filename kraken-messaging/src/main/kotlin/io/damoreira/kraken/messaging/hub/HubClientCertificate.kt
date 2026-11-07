package io.damoreira.kraken.messaging.hub

import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.security.GeneralSecurityException
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.Signature
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Duration
import java.time.Instant
import java.util.Base64

/**
 * A client certificate and its key as the hub's TLS handshake needs them (P5c, ADR 0020): [chainPem] (the file's certificates, leaf first)
 * and [keyPem] (PKCS#8) as read, [leaf] and the [service] its one `kraken://service/<name>` URI SAN names. [toString] shows the service,
 * the [fingerprint] and the end of validity, never a key.
 */
class ClientCertificate internal constructor(
    internal val chainPem: ByteArray,
    internal val keyPem: ByteArray,
    val leaf: X509Certificate,
    val service: String,
) {
    /** The first 8 hex characters of the SHA-256 of the leaf's DER: the only form of the certificate that is logged or shown (the hub's `cert:<8 hex>`). */
    val fingerprint: String = MessageDigest.getInstance("SHA-256").digest(leaf.encoded).joinToString("") { "%02x".format(it) }.take(8)
    val notBefore: Instant = leaf.notBefore.toInstant()
    val notAfter: Instant = leaf.notAfter.toInstant()

    override fun toString(): String = "ClientCertificate(service=$service, fingerprint=$fingerprint, notAfter=$notAfter)"
}

/**
 * The client certificate files of `kraken.messaging.hub.tls.client-cert-file` / `client-key-file` (P5c, ADR 0020): checked when this is
 * built (an unreadable or empty file, a key that does not match, a certificate without exactly one `kraken://service/<name>` URI SAN, a
 * name other than [serviceName] fail the startup with the setting named), shared by every hub connection, and watched afterwards.
 *
 * [current] is what a new TLS channel presents. [refresh] reads the files again (every reconnect and every heartbeat tick does): a
 * different pair that is valid (same checks, not expired, already valid) becomes [current], bumps [generation] and counts as a
 * rotation; one that is not valid is kept out and logged at ERROR once per distinct problem, so a half-written replacement is
 * adopted once it is complete. [refresh] also warns once per certificate when less than a tenth of its lifetime (24 hours at most) is
 * left and no renewed file has appeared, and logs once when [current] has ended (the hub refuses it).
 *
 * Only the service name and the first 8 hex characters of the leaf's SHA-256 (the fingerprint prefix) are ever logged.
 */
class HubClientCertificate(
    private val certFile: Path,
    private val keyFile: Path,
    private val serviceName: String,
    private val clock: () -> Instant = Instant::now,
) {
    private val log = LoggerFactory.getLogger(HubClientCertificate::class.java)

    private class Raw(val cert: ByteArray, val key: ByteArray) {
        fun sameAs(other: Raw?) = other != null && cert.contentEquals(other.cert) && key.contentEquals(other.key)
    }

    /** A problem with one of the settings: the message names it. */
    private class Problem(message: String) : Exception(message)

    @Volatile private var presenting: ClientCertificate
    private var presentingRaw: Raw
    /** The problem the latest check found (null when it found none): an error is logged only when the next check finds it again. */
    private var seenProblem: String? = null
    private var errorLoggedFor: String? = null
    private var warnedAtConnect: String? = null
    private var warnedFor: String? = null
    @Volatile private var generationCount = 0L
    @Volatile private var rotationCount = 0L

    init {
        val raw = try {
            read()
        } catch (e: Problem) {
            throw IllegalStateException(e.message, e)
        }
        presentingRaw = raw
        presenting = try {
            parse(raw).also {
                // A pair that has ended can never connect: say so now instead of failing every attempt (a renewal later is adopted as usual).
                if (!it.notAfter.isAfter(clock())) throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile: the certificate ${it.fingerprint} expired at ${it.notAfter}")
            }
        } catch (e: Problem) {
            throw IllegalStateException(e.message, e)
        }
        log.info("Hub client certificate {} loaded for service {}, valid until {}", presenting.fingerprint, presenting.service, presenting.notAfter)
        lifecycle(presenting)
    }

    /** The pair a new channel presents. */
    val current: ClientCertificate get() = presenting

    /** Counts the pairs adopted since start: a connection that began before the latest bump presents an older pair. */
    val generation: Long get() = generationCount

    /** Pairs adopted since start (the `kraken_hub_client_certificate_rotations_total` counter): the sessions are moved over by [HubConnections]. */
    val rotations: Long get() = rotationCount

    /** The clock of the validity checks (the remaining validity of a presented pair is counted with it too). */
    internal fun now(): Instant = clock()

    /** True when [current] has ended: the hub refuses it, so a connection reports it locally instead of opening a stream. */
    fun expired(): Boolean = !presenting.notAfter.isAfter(clock())

    /**
     * Reads the files again and returns [current] (see the class comment). Never throws: a file that cannot be used is kept out. A pair
     * replaced in two writes is half-written for a moment, so a problem is logged at DEBUG when first seen and at ERROR (once) when the next
     * check finds it again; [connecting] (a new attempt is about to present [current]) also logs one WARN per problem saying which
     * certificate is presented instead.
     */
    @Synchronized fun refresh(connecting: Boolean = false): ClientCertificate {
        try {
            val raw = read()
            if (!raw.sameAs(presentingRaw)) {
                val candidate = parse(raw)
                val now = clock()
                if (!candidate.notAfter.isAfter(now)) throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile: the new certificate ${candidate.fingerprint} expired at ${candidate.notAfter}")
                if (candidate.notBefore.isAfter(now)) throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile: the new certificate ${candidate.fingerprint} is not valid before ${candidate.notBefore}")
                val old = presenting
                presenting = candidate
                presentingRaw = raw
                generationCount++
                rotationCount++
                log.info(
                    "Hub client certificate of {} changed from {} to {}, valid until {}",
                    candidate.service, old.fingerprint, candidate.fingerprint, candidate.notAfter,
                )
            }
            seenProblem = null
            errorLoggedFor = null
            warnedAtConnect = null
        } catch (e: Problem) {
            // The current pair stays; a half-written replacement is complete on a later check.
            val problem = e.message
            if (connecting && problem != warnedAtConnect) {
                warnedAtConnect = problem
                log.warn("The hub client certificate files cannot be used, presenting the last good certificate {}: {}", presenting.fingerprint, problem)
            }
            if (problem != seenProblem) {
                seenProblem = problem
                log.debug("The hub client certificate files changed but cannot be used (first sighting), keeping certificate {}: {}", presenting.fingerprint, problem)
            } else if (problem != errorLoggedFor) {
                errorLoggedFor = problem
                log.error("The hub client certificate files cannot be used, keeping certificate {}: {}", presenting.fingerprint, problem)
            }
        }
        lifecycle(presenting)
        return presenting
    }

    private fun lifecycle(certificate: ClientCertificate) {
        val now = clock()
        if (!certificate.notAfter.isAfter(now)) return // reported by the connections, through the rejection path
        val lifetime = Duration.between(certificate.notBefore, certificate.notAfter)
        val warnWithin = minOf(lifetime.dividedBy(10), Duration.ofHours(24))
        if (Duration.between(now, certificate.notAfter) < warnWithin && warnedFor != certificate.fingerprint) {
            warnedFor = certificate.fingerprint
            log.warn(
                "Hub client certificate {} of {} expires at {} ({} left) and no renewed file has appeared at {}: the hub closes the session at that moment",
                certificate.fingerprint, certificate.service, certificate.notAfter, Duration.between(now, certificate.notAfter), certFile,
            )
        }
    }

    private fun read(): Raw = Raw(
        readFile(certFile, "client-cert-file"),
        readFile(keyFile, "client-key-file"),
    )

    private fun readFile(file: Path, setting: String): ByteArray {
        val bytes = try {
            Files.readAllBytes(file)
        } catch (e: IOException) {
            throw Problem("kraken.messaging.hub.tls.$setting $file cannot be read: ${e.javaClass.simpleName}")
        }
        if (String(bytes, Charsets.ISO_8859_1).isBlank()) throw Problem("kraken.messaging.hub.tls.$setting $file is empty")
        return bytes
    }

    private fun parse(raw: Raw): ClientCertificate {
        val certificates = try {
            CertificateFactory.getInstance("X.509").generateCertificates(ByteArrayInputStream(raw.cert)).filterIsInstance<X509Certificate>()
        } catch (e: GeneralSecurityException) {
            throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile is not a PEM certificate chain: ${e.message}")
        }
        val leaf = certificates.firstOrNull() ?: throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile holds no PEM certificate")
        val key = parseKey(raw.key)
        if (!matches(key, leaf)) {
            throw Problem("kraken.messaging.hub.tls.client-key-file $keyFile does not match the first certificate of client-cert-file $certFile")
        }
        val service = serviceOf(leaf)
        if (service != serviceName) {
            throw Problem(
                "kraken.messaging.hub.tls.client-cert-file $certFile names the service $service but kraken.messaging.service-name is $serviceName: " +
                    "the hub refuses a registration under a name the certificate does not carry",
            )
        }
        return ClientCertificate(raw.cert, raw.key, leaf, service)
    }

    /** The one `kraken://service/<name>` URI SAN, as the hub reads it (ADR 0020 ruling 4); other SANs are ignored. */
    private fun serviceOf(leaf: X509Certificate): String {
        val uris = try {
            leaf.subjectAlternativeNames.orEmpty().filter { it[0] == URI_SAN }.map { it[1] as String }
        } catch (e: GeneralSecurityException) {
            throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile has unreadable subject alternative names: ${e.message}")
        }.filter { it.startsWith(SERVICE_PREFIX) }
        if (uris.isEmpty()) throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile has no URI SAN ${SERVICE_PREFIX}<name>: the hub finds no service in it")
        if (uris.size > 1) throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile names more than one service (${uris.size} URI SANs ${SERVICE_PREFIX}...): the hub refuses it")
        val name = uris.single().removePrefix(SERVICE_PREFIX)
        if (!NAME.matches(name)) {
            throw Problem("kraken.messaging.hub.tls.client-cert-file $certFile has a URI SAN ${SERVICE_PREFIX}<name> with a name of 1 to 128 characters outside A-Za-z0-9._-: ${name.take(40)}")
        }
        return name
    }

    private fun parseKey(bytes: ByteArray): PrivateKey {
        val text = String(bytes, Charsets.ISO_8859_1)
        val begin = text.indexOf(PKCS8_BEGIN)
        val end = text.indexOf(PKCS8_END)
        if (begin < 0 || end < begin) {
            throw Problem(
                "kraken.messaging.hub.tls.client-key-file $keyFile must be an unencrypted PKCS#8 PEM key (-----BEGIN PRIVATE KEY-----); " +
                    "convert another format with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem",
            )
        }
        val der = try {
            Base64.getMimeDecoder().decode(text.substring(begin + PKCS8_BEGIN.length, end))
        } catch (e: IllegalArgumentException) {
            throw Problem("kraken.messaging.hub.tls.client-key-file $keyFile is not valid PEM")
        }
        val spec = PKCS8EncodedKeySpec(der)
        for (algorithm in KEY_ALGORITHMS) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec)
            } catch (_: GeneralSecurityException) {
                // another algorithm
            }
        }
        throw Problem("kraken.messaging.hub.tls.client-key-file $keyFile is not a PKCS#8 RSA, EC or Ed25519 private key")
    }

    /** A signature made with [key] that [leaf]'s public key verifies. */
    private fun matches(key: PrivateKey, leaf: X509Certificate): Boolean = try {
        val algorithm = when (key.algorithm) {
            "RSA" -> "SHA256withRSA"
            "EC" -> "SHA256withECDSA"
            else -> "EdDSA"
        }
        val data = "kraken client certificate".toByteArray()
        val signature = Signature.getInstance(algorithm).apply { initSign(key); update(data) }.sign()
        Signature.getInstance(algorithm).apply { initVerify(leaf.publicKey); update(data) }.verify(signature)
    } catch (e: GeneralSecurityException) {
        false
    }

    private companion object {
        const val URI_SAN = 6
        const val SERVICE_PREFIX = "kraken://service/"
        val NAME = Regex("[A-Za-z0-9._-]{1,128}")
        const val PKCS8_BEGIN = "-----BEGIN PRIVATE KEY-----"
        const val PKCS8_END = "-----END PRIVATE KEY-----"
        val KEY_ALGORITHMS = listOf("RSA", "EC", "Ed25519")
    }
}
