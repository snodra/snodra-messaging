package com.damoreira.snodra.messaging.fixtures

import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.security.spec.ECGenParameterSpec
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.Date
import javax.security.auth.x500.X500Principal

/**
 * Throwaway certificate authorities and client certificates for the mTLS tests, generated at test time (BouncyCastle, test scope: the
 * JDK cannot put a URI SAN or a seconds-long validity in a certificate). Nothing is checked in; every file lands in a caller's temp dir.
 */
object TestPki {
    private var serial = System.nanoTime()

    private fun keyPair(rsa: Boolean = false): KeyPair =
        if (rsa) KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()
        else KeyPairGenerator.getInstance("EC").apply { initialize(ECGenParameterSpec("secp256r1")) }.generateKeyPair()

    private fun nextSerial(): BigInteger = BigInteger.valueOf(synchronized(this) { ++serial })

    /** A signing authority: [certificate] is what a hub's `SNODRA_SERVER_TLS_CLIENT_CA` or a client's `tls.ca-file` holds, as PEM in [pem]. */
    class Ca internal constructor(val certificate: X509Certificate, internal val key: PrivateKey, val pem: Path) {
        /**
         * A certificate signed by this CA. [uris] are the URI SANs (default the one `snodra://service/<service>`), [dns] the DNS SANs, [ips] the
         * IP SANs. [rsa] makes an RSA key (the hub's own pair: ring, which the hub's rustls uses, reads a PKCS#8 EC key only with its public key
         * included, which the JDK leaves out; an EC key is fine for a client). [validFor] counts from [notBefore] (default one minute ago, so a clock a little behind still accepts it).
         */
        fun issue(
            service: String? = null,
            uris: List<String> = listOfNotNull(service?.let { "snodra://service/$it" }),
            dns: List<String> = emptyList(),
            ips: List<String> = emptyList(),
            validFor: Duration = Duration.ofHours(1),
            notBefore: Instant = Instant.now().minusSeconds(60),
            clientAuth: Boolean = true,
            serverAuth: Boolean = false,
            rsa: Boolean = false,
        ): Issued {
            val pair = keyPair(rsa)
            val names = uris.map { GeneralName(GeneralName.uniformResourceIdentifier, it) } +
                dns.map { GeneralName(GeneralName.dNSName, it) } + ips.map { GeneralName(GeneralName.iPAddress, it) }
            val builder = JcaX509v3CertificateBuilder(
                certificate, nextSerial(), Date.from(notBefore), Date.from(notBefore.plus(validFor)),
                X500Principal("CN=${service ?: "no-service"}"), pair.public,
            )
            builder.addExtension(Extension.basicConstraints, true, BasicConstraints(false))
            builder.addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.digitalSignature))
            val usages = listOfNotNull(KeyPurposeId.id_kp_clientAuth.takeIf { clientAuth }, KeyPurposeId.id_kp_serverAuth.takeIf { serverAuth })
            if (usages.isNotEmpty()) builder.addExtension(Extension.extendedKeyUsage, false, ExtendedKeyUsage(usages.toTypedArray()))
            if (names.isNotEmpty()) builder.addExtension(Extension.subjectAlternativeName, false, GeneralNames(names.toTypedArray()))
            val signed = builder.build(JcaContentSignerBuilder("SHA256withECDSA").build(key))
            return Issued(JcaX509CertificateConverter().getCertificate(signed), pair.private)
        }
    }

    /** A certificate with its private key. */
    class Issued internal constructor(val certificate: X509Certificate, val key: PrivateKey) {
        /** The PEM of the certificate chain (leaf only here). */
        val certificatePem: String get() = pem("CERTIFICATE", certificate.encoded)

        /** The key as PKCS#8 PEM, the format the library documents. */
        val keyPem: String get() = pem("PRIVATE KEY", key.encoded)

        /** The first 8 hex characters of the SHA-256 of the DER: what the hub and the library show of a certificate. */
        val fingerprint: String
            get() = java.security.MessageDigest.getInstance("SHA-256").digest(certificate.encoded).joinToString("") { "%02x".format(it) }.take(8)

        /** Writes the pair; replaces both files (each by a rename, so a reader never sees half of one). Returns [certFile] to [keyFile]. */
        fun writeTo(certFile: Path, keyFile: Path): Pair<Path, Path> {
            write(keyFile, keyPem) // the key first: a reader that sees the new certificate finds its key
            write(certFile, certificatePem)
            return certFile to keyFile
        }
    }

    /** A new self-signed CA ([validFor] ten years by default), its certificate PEM written to `<directory>/<name>.pem`. */
    fun ca(directory: Path, name: String = "snodra-test-client-ca"): Ca {
        Files.createDirectories(directory)
        val pair = keyPair()
        val subject = X500Principal("CN=$name")
        val now = Instant.now()
        val builder = JcaX509v3CertificateBuilder(subject, nextSerial(), Date.from(now.minusSeconds(60)), Date.from(now.plus(Duration.ofDays(3650))), subject, pair.public)
        builder.addExtension(Extension.basicConstraints, true, BasicConstraints(true))
        builder.addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.keyCertSign or KeyUsage.cRLSign))
        val certificate = JcaX509CertificateConverter().getCertificate(builder.build(JcaContentSignerBuilder("SHA256withECDSA").build(pair.private)))
        return Ca(certificate, pair.private, write(directory.resolve("$name.pem"), pem("CERTIFICATE", certificate.encoded)))
    }

    /** The files of a hub's server pair issued by [ca] with both usages (a `required` hub probes itself with it), valid for `localhost` and `127.0.0.1`. */
    fun hubPair(ca: Ca, directory: Path): Pair<Path, Path> {
        Files.createDirectories(directory)
        val issued = ca.issue(dns = listOf("localhost"), ips = listOf("127.0.0.1"), clientAuth = true, serverAuth = true, validFor = Duration.ofDays(2), rsa = true)
        return issued.writeTo(directory.resolve("hub-cert.pem"), directory.resolve("hub-key.pem"))
    }

    fun pem(type: String, der: ByteArray): String =
        "-----BEGIN $type-----\n" + Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(der) + "\n-----END $type-----\n"

    /** World-readable (a container's `nonroot` reads it) and replaced by a rename. */
    private fun write(path: Path, content: String): Path {
        val temp = Files.createTempFile(path.parent, path.fileName.toString(), ".tmp")
        Files.writeString(temp, content)
        runCatching { Files.setPosixFilePermissions(temp, PosixFilePermissions.fromString("rw-r--r--")) }
        return Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }
}
