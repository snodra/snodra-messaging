package io.damoreira.kraken.messaging.test

import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * Shared Testcontainers definitions for kraken integration tests. Containers are returned unstarted;
 * callers start them. Kafka and MongoDB (single-node replica set) are marked reusable.
 */
object KrakenContainers {
    const val KAFKA_IMAGE = "apache/kafka:4.4.0"
    const val MONGO_IMAGE = "mongo:8.3.13"

    private val hubImage: String get() = System.getenv("KRAKEN_HUB_IMAGE") ?: "kraken-hub:dev"

    /** The previous hub release, for compatibility tests of new clients against an old hub (legacy mode). */
    private val hubBaselineImage: String get() = System.getenv("KRAKEN_HUB_BASELINE_IMAGE") ?: "kraken-hub:v1-baseline"

    /** The merged P0 hub (protocol v2 without flow control), for compatibility tests of P1 clients. */
    private val hubP0Image: String get() = System.getenv("KRAKEN_HUB_P0_IMAGE") ?: "kraken-hub:p0"

    /** The merged P2 hub (protocol v2 with flow control, without `instance-status`), for compatibility tests of P3 clients. */
    private val hubP2Image: String get() = System.getenv("KRAKEN_HUB_P2_IMAGE") ?: "kraken-hub:p2"

    /** The P3 hub (multi-hub HA: `instance-status`), for the two-hub tests. */
    private val hubP3Image: String get() = System.getenv("KRAKEN_HUB_P3_IMAGE") ?: "kraken-hub:p3-hub"

    /** The P4a hub (scatter-gather, shared query types), for the scatter-gather tests. */
    private val hubP4aImage: String get() = System.getenv("KRAKEN_HUB_P4A_IMAGE") ?: "kraken-hub:p4a-hub"

    /** The P4b hub (streaming queries), for the streaming tests. */
    private val hubP4bImage: String get() = System.getenv("KRAKEN_HUB_P4B_IMAGE") ?: "kraken-hub:p4b-hub"

    /** The P4c hub (subscription queries), for the subscription tests. */
    private val hubP4cImage: String get() = System.getenv("KRAKEN_HUB_P4C_IMAGE") ?: "kraken-hub:p4c-hub"

    /** The P5a hub (access control and TLS), for the security tests. */
    private val hubP5aImage: String get() = System.getenv("KRAKEN_HUB_P5A_IMAGE") ?: "kraken-hub:p5a-hub"

    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withReuse(true)

    fun mongo(): MongoDBContainer = MongoDBContainer(DockerImageName.parse(MONGO_IMAGE)).withReplicaSet().withReuse(true)

    /** The hub image under test: `KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`. */
    fun hub(): GenericContainer<*> = hub(hubImage)

    /** The baseline (previous) hub image: `KRAKEN_HUB_BASELINE_IMAGE`, default `kraken-hub:v1-baseline`. */
    fun hubBaseline(): GenericContainer<*> = hub(hubBaselineImage)

    /** The P0 hub image: `KRAKEN_HUB_P0_IMAGE`, default `kraken-hub:p0`. */
    fun hubP0(): GenericContainer<*> = hub(hubP0Image)

    /** The P2 hub image: `KRAKEN_HUB_P2_IMAGE`, default `kraken-hub:p2`. */
    fun hubP2(): GenericContainer<*> = hub(hubP2Image)

    /** The P3 hub image: `KRAKEN_HUB_P3_IMAGE`, default `kraken-hub:p3-hub`; gRPC 7000 and HTTP 7001 exposed. */
    fun hubP3(): GenericContainer<*> = hub(hubP3Image).withExposedPorts(7000, 7001)

    /** The P4a hub image: `KRAKEN_HUB_P4A_IMAGE`, default `kraken-hub:p4a-hub`; gRPC 7000 and HTTP 7001 exposed. */
    fun hubP4a(): GenericContainer<*> = hub(hubP4aImage).withExposedPorts(7000, 7001)

    /** The P4b hub image: `KRAKEN_HUB_P4B_IMAGE`, default `kraken-hub:p4b-hub`; gRPC 7000 and HTTP 7001 exposed. */
    fun hubP4b(): GenericContainer<*> = hub(hubP4bImage).withExposedPorts(7000, 7001)

    /** The P4c hub image: `KRAKEN_HUB_P4C_IMAGE`, default `kraken-hub:p4c-hub`; gRPC 7000 and HTTP 7001 exposed. */
    fun hubP4c(): GenericContainer<*> = hub(hubP4cImage).withExposedPorts(7000, 7001)

    /**
     * The P5a hub image: `KRAKEN_HUB_P5A_IMAGE`, default `kraken-hub:p5a-hub`, with access control and optionally TLS.
     *
     * - [acl]: token -> ACL entry. The ACL file (digests only, as the hub wants it) is written into [directory] and copied
     *   to the container; the plain tokens stay in the test. Mode [authMode] (`enforce` by default, `permissive` or `off`).
     * - [tls]: certificate and key (see [TestCertificates]) copied to the container; the gRPC and HTTP listeners then
     *   accept TLS only. Null: plain text.
     * - [env]: further `KRAKEN_HUB_*` settings.
     *
     * Exposes gRPC 7000 and HTTP 7001; OTLP export is disabled.
     */
    fun hubP5a(
        directory: Path,
        acl: Map<String, AclEntry> = emptyMap(),
        authMode: String = "enforce",
        tls: TestCertificates? = null,
        env: Map<String, String> = emptyMap(),
    ): GenericContainer<*> {
        val container = hub(hubP5aImage).withExposedPorts(7000, 7001).withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
        container.withEnv("KRAKEN_HUB_AUTH_MODE", authMode)
        if (authMode != "off") {
            val file = directory.resolve("acl.toml")
            Files.writeString(file, aclToml(acl))
            container.withCopyFileToContainer(MountableFile.forHostPath(file, 0b110_100_100), HUB_ACL_PATH)
            container.withEnv("KRAKEN_HUB_ACL_FILE", HUB_ACL_PATH)
        }
        if (tls != null) {
            container.withCopyFileToContainer(MountableFile.forHostPath(tls.certificate, 0b110_100_100), HUB_TLS_CERT_PATH)
            container.withCopyFileToContainer(MountableFile.forHostPath(tls.privateKey, 0b110_100_100), HUB_TLS_KEY_PATH)
            container.withEnv("KRAKEN_HUB_TLS_CERT", HUB_TLS_CERT_PATH).withEnv("KRAKEN_HUB_TLS_KEY", HUB_TLS_KEY_PATH)
        }
        return container.withEnv(env)
    }

    /** The ACL file of the hub: one `[[token]]` per entry, keyed by the SHA-256 of the token (the hub never sees it). */
    fun aclToml(acl: Map<String, AclEntry>): String = acl.entries.joinToString("\n") { (token, entry) ->
        fun list(items: List<String>) = items.joinToString(", ", "[", "]") { "\"$it\"" }
        "[[token]]\ntoken_sha256 = \"${sha256Hex(token)}\"\nservice = \"${entry.service}\"\nhandle = ${list(entry.handle)}\nsend = ${list(entry.send)}\n"
    }

    private fun sha256Hex(token: String): String =
        MessageDigest.getInstance("SHA-256").digest(token.toByteArray()).joinToString("") { "%02x".format(it) }

    private const val HUB_ACL_PATH = "/etc/kraken-hub/acl.toml"
    private const val HUB_TLS_CERT_PATH = "/etc/kraken-hub/tls/cert.pem"
    private const val HUB_TLS_KEY_PATH = "/etc/kraken-hub/tls/key.pem"

    /**
     * The hub under test with extra `KRAKEN_HUB_*` settings (for example `KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE` or
     * `KRAKEN_HUB_TIMEOUT_OVERRIDES`); OTLP export is disabled.
     */
    fun hub(env: Map<String, String>): GenericContainer<*> = hub().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").withEnv(env)

    fun hubImageAvailable(): Boolean = imageAvailable(hubImage)

    fun hubP0ImageAvailable(): Boolean = imageAvailable(hubP0Image)

    fun hubP2ImageAvailable(): Boolean = imageAvailable(hubP2Image)

    fun hubP3ImageAvailable(): Boolean = imageAvailable(hubP3Image)

    fun hubP4aImageAvailable(): Boolean = imageAvailable(hubP4aImage)

    fun hubP4bImageAvailable(): Boolean = imageAvailable(hubP4bImage)

    fun hubP4cImageAvailable(): Boolean = imageAvailable(hubP4cImage)

    fun hubP5aImageAvailable(): Boolean = imageAvailable(hubP5aImage)

    fun hubBaselineImageAvailable(): Boolean = imageAvailable(hubBaselineImage)

    private fun hub(image: String): GenericContainer<*> = GenericContainer(DockerImageName.parse(image))
        .withExposedPorts(7000)
        .waitingFor(Wait.forLogMessage(".*hub listening.*", 1))

    private fun imageAvailable(image: String): Boolean = try {
        DockerClientFactory.instance().client().inspectImageCmd(image).exec()
        true
    } catch (_: Exception) {
        false
    }
}
