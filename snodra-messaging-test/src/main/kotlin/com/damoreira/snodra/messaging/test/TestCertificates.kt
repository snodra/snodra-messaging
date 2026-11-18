package com.damoreira.snodra.messaging.test

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.Certificate
import java.util.Base64
import java.util.concurrent.TimeUnit

/**
 * A throwaway self-signed TLS certificate for tests, generated at test time with the JDK's own `keytool` and written
 * as PEM: [certificate] (also the CA a client trusts: `snodra.messaging.hub.tls.ca-file`) and [privateKey] (PKCS#8), for
 * the hub's `SNODRA_SERVER_TLS_CERT` / `SNODRA_SERVER_TLS_KEY`. Nothing is checked in; [directory] is the caller's temp dir.
 * The certificate is valid for [names] (DNS names and IP addresses; default `localhost` and `127.0.0.1`) for two days.
 */
class TestCertificates private constructor(val certificate: Path, val privateKey: Path) {
    companion object {
        /** Generates a certificate for [names] into [directory] (created when absent), files readable by any user (a container's `nonroot`). */
        fun generate(directory: Path, names: List<String> = listOf("localhost", "127.0.0.1"), commonName: String = "snodra-server-test"): TestCertificates {
            Files.createDirectories(directory)
            val store = directory.resolve("keystore.p12")
            val password = "changeit" // protects only this throwaway keystore, which is deleted with the directory
            val san = names.joinToString(",") { if (it.matches(Regex("[0-9.]+|[0-9a-fA-F:]+:[0-9a-fA-F:]*"))) "ip:$it" else "dns:$it" }
            val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString()
            val process = ProcessBuilder(
                keytool, "-genkeypair", "-alias", "hub", "-keyalg", "RSA", "-keysize", "2048", "-sigalg", "SHA256withRSA",
                "-dname", "CN=$commonName", "-ext", "san=$san", "-validity", "2",
                "-keystore", store.toString(), "-storetype", "PKCS12", "-storepass", password, "-keypass", password,
            ).redirectErrorStream(true).start()
            val output = process.inputStream.readAllBytes().decodeToString()
            check(process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0) { "keytool failed: $output" }

            val keyStore = KeyStore.getInstance("PKCS12").apply { Files.newInputStream(store).use { load(it, password.toCharArray()) } }
            val certificate: Certificate = keyStore.getCertificate("hub")
            val key = keyStore.getKey("hub", password.toCharArray()) as PrivateKey
            Files.delete(store)
            return TestCertificates(
                write(directory.resolve("hub-cert.pem"), pem("CERTIFICATE", certificate.encoded)),
                write(directory.resolve("hub-key.pem"), pem("PRIVATE KEY", key.encoded)),
            )
        }

        private fun pem(type: String, der: ByteArray): String =
            "-----BEGIN $type-----\n" + Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(der) + "\n-----END $type-----\n"

        private fun write(path: Path, content: String): Path {
            Files.writeString(path, content)
            runCatching { Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--")) }
            return path
        }
    }
}
