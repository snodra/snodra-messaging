package io.damoreira.kraken.messaging.rabbitmq

import org.springframework.boot.context.properties.ConfigurationProperties
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * `kraken.messaging.rabbitmq.*`: how the RabbitMQ Streams event store ([RabbitEventStore]) reaches RabbitMQ, the Rust add-on's
 * `KRAKEN_RABBITMQ_*` settings. The source, the subscriptions' names and the dead-letter mode are the core's
 * `kraken.messaging.events.*`. Passwords never appear in logs.
 */
@ConfigurationProperties("kraken.messaging.rabbitmq")
data class RabbitProperties(
    /** Nodes to connect to first, `host` or `host:port`, comma-separated, tried in order. */
    val hosts: List<String> = listOf("rabbitmq"),
    /** The port of a host given without one (5552, 5551 with TLS). */
    val port: Int? = null,
    val virtualHost: String = "/",
    val username: String = "guest",
    /** The password (default `guest`); wins over [passwordFile]. */
    val password: String? = null,
    /** A file holding the password (its trailing newline removed). */
    val passwordFile: Path? = null,
    val tls: Boolean = false,
    /** PEM CA certificates the nodes are verified against; required with TLS. */
    val tlsCaFile: Path? = null,
    /** The hosts are a load balancer: connect through it until it lands on the node needed. */
    val loadBalancer: Boolean = false,
    val offsetStoreMessages: Long = 10_000,
    val offsetStoreInterval: Duration = Duration.ofSeconds(5),
    val management: Management = Management(),
) {
    /** `kraken.messaging.rabbitmq.management.*`: the management API, which declares `replay`. */
    data class Management(
        /** `http(s)://host:port`, no credentials in it; none: no management API, no `replay`. */
        val url: String? = null,
        val username: String? = null,
        val password: String? = null,
        val passwordFile: Path? = null,
        val caFile: Path? = null,
    ) {
        override fun toString(): String = "Management(url=$url, username=$username, password=<hidden>, passwordFile=$passwordFile, caFile=$caFile)"
    }

    /** The event store's settings: a password from its file when the setting is not given. */
    fun toSettings() = RabbitEventStoreSettings(
        hosts = hosts.flatMap { it.split(',') }.map(String::trim).filter(String::isNotEmpty).ifEmpty { hosts },
        port = port,
        virtualHost = virtualHost,
        username = username,
        password = secret(password, passwordFile) ?: "guest",
        tls = tls,
        tlsCaFile = tlsCaFile,
        loadBalancer = loadBalancer,
        offsetStoreMessages = offsetStoreMessages,
        offsetStoreInterval = offsetStoreInterval,
        management = management.url?.takeIf { it.isNotBlank() }?.let {
            RabbitManagementSettings(it, management.username, secret(management.password, management.passwordFile), management.caFile)
        },
    )

    override fun toString(): String =
        "RabbitProperties(hosts=$hosts, port=$port, virtualHost=$virtualHost, username=$username, password=<hidden>, passwordFile=$passwordFile, " +
            "tls=$tls, tlsCaFile=$tlsCaFile, loadBalancer=$loadBalancer, offsetStoreMessages=$offsetStoreMessages, " +
            "offsetStoreInterval=$offsetStoreInterval, management=$management)"

    private companion object {
        fun secret(value: String?, file: Path?): String? =
            value ?: file?.let { Files.readString(it).removeSuffix("\n").removeSuffix("\r") }
    }
}
