package com.damoreira.snodra.messaging.test

import com.github.dockerjava.api.model.Ulimit
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.wait.strategy.WaitAllStrategy
import java.time.Duration
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.mysql.MySQLContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * Shared Testcontainers definitions for snodra integration tests. Containers are returned unstarted;
 * callers start them. Kafka, MongoDB (single-node replica set), PostgreSQL and MySQL are marked reusable.
 */
object SnodraContainers {
    const val KAFKA_IMAGE = "apache/kafka:4.4.0"
    const val MONGO_IMAGE = "mongo:8.3.17"
    const val POSTGRES_IMAGE = "postgres:17.7"
    const val MYSQL_IMAGE = "mysql:8.4.7"
    private const val MONGO_OPEN_FILES = 64_000L

    private val hubImage: String get() = System.getenv("SNODRA_SERVER_IMAGE") ?: "snodra-server:dev"

    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withReuse(true)

    /**
     * MongoDB ([MONGO_IMAGE]) as a single-node replica set. Its open-file limit is MongoDB's recommended 64000: WiredTiger
     * keeps a file open per collection and index, and the conformance tests create a database per test, which exhausts a
     * Docker daemon's default of 1024 and leaves the server refusing connections.
     */
    fun mongo(): MongoDBContainer = MongoDBContainer(DockerImageName.parse(MONGO_IMAGE)).withReplicaSet().withReuse(true)
        .withStartupTimeout(Duration.ofMinutes(3))
        // When mongod dies at startup the reason only exists in its log; print it so a CI failure is diagnosable.
        .withLogConsumer { frame -> println("[mongo] " + frame.utf8String.trimEnd()) }
        .withCreateContainerCmdModifier { it.hostConfig!!.withUlimits(listOf(Ulimit("nofile", MONGO_OPEN_FILES, MONGO_OPEN_FILES))) }

    /**
     * PostgreSQL ([POSTGRES_IMAGE]) for the SQL message store (`snodra-messaging-jdbc`); test credentials only. Started once the
     * server is ready and its port answers on the host (a VM's port forwarding can lag behind the server's log line).
     */
    fun postgres(): PostgreSQLContainer = PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE)).withReuse(true)
        .withStartupTimeout(Duration.ofMinutes(3))
        .waitingFor(
            WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))
                .withStrategy(Wait.forListeningPort()),
        )

    /** MySQL ([MYSQL_IMAGE], InnoDB) for the SQL message store (`snodra-messaging-jdbc`); test credentials only. */
    fun mysql(): MySQLContainer = MySQLContainer(DockerImageName.parse(MYSQL_IMAGE)).withReuse(true)
        .withStartupTimeout(Duration.ofMinutes(3))

    /** The hub image under test: `SNODRA_SERVER_IMAGE`, default `snodra-server:dev`; gRPC 7000 and HTTP 7001 exposed. */
    fun hub(): GenericContainer<*> = hub(hubImage).withExposedPorts(7000, 7001)

    /**
     * The hub image under test (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`) with token access control and
     * optionally TLS.
     *
     * - [acl]: token -> ACL entry. The ACL file (digests only, as the hub wants it) is written into [directory] and copied
     *   to the container; the plain tokens stay in the test. Mode [authMode] (`enforce` by default, `permissive` or `off`).
     * - [tls]: certificate and key (see [TestCertificates]) copied to the container; the gRPC and HTTP listeners then
     *   accept TLS only. Null: plain text.
     * - [env]: further `SNODRA_SERVER_*` settings.
     *
     * Exposes gRPC 7000 and HTTP 7001; OTLP export is disabled.
     */
    fun hubAcl(
        directory: Path,
        acl: Map<String, AclEntry> = emptyMap(),
        authMode: String = "enforce",
        tls: TestCertificates? = null,
        env: Map<String, String> = emptyMap(),
    ): GenericContainer<*> {
        val container = hub().withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "")
        container.withEnv("SNODRA_SERVER_AUTH_MODE", authMode)
        if (authMode != "off") {
            val file = directory.resolve("acl.toml")
            Files.writeString(file, aclToml(acl))
            container.withCopyFileToContainer(MountableFile.forHostPath(file, 0b110_100_100), HUB_ACL_PATH)
            container.withEnv("SNODRA_SERVER_ACL_FILE", HUB_ACL_PATH)
        }
        if (tls != null) {
            container.withCopyFileToContainer(MountableFile.forHostPath(tls.certificate, 0b110_100_100), HUB_TLS_CERT_PATH)
            container.withCopyFileToContainer(MountableFile.forHostPath(tls.privateKey, 0b110_100_100), HUB_TLS_KEY_PATH)
            container.withEnv("SNODRA_SERVER_TLS_CERT", HUB_TLS_CERT_PATH).withEnv("SNODRA_SERVER_TLS_KEY", HUB_TLS_KEY_PATH)
        }
        return container.withEnv(env)
    }

    /**
     * The hub image under test (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`) with JWT authentication, mode
     * `enforce`: [services] become the ACL file's `[[service]]` tables (the rights of a service whose token's `azp` is its name),
     * [issuer] is the exact `iss` a token must carry and [jwksUri] where the hub fetches the signing keys (a test server on the
     * host is `http://host.testcontainers.internal:<port>/...` after `Testcontainers.exposeHostPorts(port)`). The leeway is 0 s
     * and the expiry grace 2 s unless [env] says otherwise, so a token's `exp` matters to the second. Exposes gRPC 7000 and HTTP
     * 7001; OTLP export is disabled.
     */
    fun hubJwt(
        directory: Path,
        services: List<AclEntry>,
        issuer: String,
        jwksUri: String,
        env: Map<String, String> = emptyMap(),
    ): GenericContainer<*> {
        val file = directory.resolve("acl.toml")
        Files.writeString(file, serviceToml(services))
        return hub().withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "")
            .withCopyFileToContainer(MountableFile.forHostPath(file, 0b110_100_100), HUB_ACL_PATH)
            .withEnv("SNODRA_SERVER_AUTH_MODE", "enforce").withEnv("SNODRA_SERVER_ACL_FILE", HUB_ACL_PATH)
            .withEnv("SNODRA_SERVER_JWT_ISSUER", issuer).withEnv("SNODRA_SERVER_JWT_JWKS_URI", jwksUri)
            .withEnv("SNODRA_SERVER_JWT_LEEWAY", "0s").withEnv("SNODRA_SERVER_JWT_EXPIRY_GRACE", "2s")
            .withEnv(env)
    }

    /**
     * The hub image under test (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`) with client certificates (mutual TLS), mode
     * `enforce`: [services] become the ACL file's `[[service]]` tables (the rights of the service a certificate's `snodra://service/<name>`
     * SAN names), [serverCertificate] and [serverKey] are the hub's own pair (PEM files), [clientCa] the PEM bundle of the CAs whose client
     * certificates it accepts and [clientAuth] `required` or `optional`. Under `required` the image's own health check presents the hub's pair
     * as a client certificate, so issue the server certificate from [clientCa] with both the server and the client usage;
     * the wait strategy is the log line, which does not depend on it. Further `SNODRA_SERVER_*` settings go in [env]. Exposes gRPC 7000 and HTTP
     * 7001; OTLP export is disabled.
     */
    fun hubMtls(
        directory: Path,
        services: List<AclEntry>,
        serverCertificate: Path,
        serverKey: Path,
        clientCa: Path,
        clientAuth: String = "required",
        env: Map<String, String> = emptyMap(),
    ): GenericContainer<*> {
        val file = directory.resolve("acl.toml")
        Files.writeString(file, serviceToml(services))
        return hub().withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "")
            .withCopyFileToContainer(MountableFile.forHostPath(file, 0b110_100_100), HUB_ACL_PATH)
            .withCopyFileToContainer(MountableFile.forHostPath(serverCertificate, 0b110_100_100), HUB_TLS_CERT_PATH)
            .withCopyFileToContainer(MountableFile.forHostPath(serverKey, 0b110_100_100), HUB_TLS_KEY_PATH)
            .withCopyFileToContainer(MountableFile.forHostPath(clientCa, 0b110_100_100), HUB_TLS_CLIENT_CA_PATH)
            .withEnv("SNODRA_SERVER_AUTH_MODE", "enforce").withEnv("SNODRA_SERVER_ACL_FILE", HUB_ACL_PATH)
            .withEnv("SNODRA_SERVER_TLS_CERT", HUB_TLS_CERT_PATH).withEnv("SNODRA_SERVER_TLS_KEY", HUB_TLS_KEY_PATH)
            .withEnv("SNODRA_SERVER_TLS_CLIENT_CA", HUB_TLS_CLIENT_CA_PATH).withEnv("SNODRA_SERVER_TLS_CLIENT_AUTH", clientAuth)
            .withEnv(env)
    }

    /** The ACL file of the hub for JWT services: one `[[service]]` per entry ([AclEntry.service] is the `name`). */
    fun serviceToml(services: List<AclEntry>): String = services.joinToString("\n") { entry ->
        fun list(items: List<String>) = items.joinToString(", ", "[", "]") { "\"$it\"" }
        "[[service]]\nname = \"${entry.service}\"\nhandle = ${list(entry.handle)}\nsend = ${list(entry.send)}\n"
    }

    /** The ACL file of the hub: one `[[token]]` per entry, keyed by the SHA-256 of the token (the hub never sees it). */
    fun aclToml(acl: Map<String, AclEntry>): String = acl.entries.joinToString("\n") { (token, entry) ->
        fun list(items: List<String>) = items.joinToString(", ", "[", "]") { "\"$it\"" }
        "[[token]]\ntoken_sha256 = \"${sha256Hex(token)}\"\nservice = \"${entry.service}\"\nhandle = ${list(entry.handle)}\nsend = ${list(entry.send)}\n"
    }

    private fun sha256Hex(token: String): String =
        MessageDigest.getInstance("SHA-256").digest(token.toByteArray()).joinToString("") { "%02x".format(it) }

    private const val HUB_ACL_PATH = "/etc/snodra-server/acl.toml"
    private const val HUB_TLS_CERT_PATH = "/etc/snodra-server/tls/cert.pem"
    private const val HUB_TLS_KEY_PATH = "/etc/snodra-server/tls/key.pem"
    private const val HUB_TLS_CLIENT_CA_PATH = "/etc/snodra-server/tls/client-ca.pem"

    /**
     * The hub under test with extra `SNODRA_SERVER_*` settings (for example `SNODRA_SERVER_QUEUE_CAPACITY_PER_INSTANCE` or
     * `SNODRA_SERVER_TIMEOUT_OVERRIDES`); OTLP export is disabled.
     */
    fun hub(env: Map<String, String>): GenericContainer<*> = hub().withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "").withEnv(env)

    fun hubImageAvailable(): Boolean = imageAvailable(hubImage)

    private fun hub(image: String): GenericContainer<*> = GenericContainer(DockerImageName.parse(image))
        .withExposedPorts(7000)
        .withStartupTimeout(Duration.ofMinutes(3))
        .waitingFor(Wait.forLogMessage(".*hub listening.*", 1))

    private fun imageAvailable(image: String): Boolean = try {
        DockerClientFactory.instance().client().inspectImageCmd(image).exec()
        true
    } catch (_: Exception) {
        false
    }
}
