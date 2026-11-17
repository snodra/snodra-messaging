package com.damoreira.snodra.messaging.rabbitmq

import org.springframework.boot.context.properties.ConfigurationProperties
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * `snodra.messaging.rabbitmq.*`: how the RabbitMQ Streams event store ([RabbitEventStore]) reaches RabbitMQ, the Rust add-on's
 * `SNODRA_RABBITMQ_*` settings. The source, the subscriptions' names and the dead-letter mode are the core's
 * `snodra.messaging.events.*`. Passwords never appear in logs.
 */
@ConfigurationProperties("snodra.messaging.rabbitmq")
data class RabbitProperties(
    /** Nodes to connect to first, `host` or `host:port`, comma-separated, tried in order. */
    val hosts: List<String> = listOf("rabbitmq"),
    /** The port of a host given without one (5552, 5551 with TLS). */
    val port: Int? = null,
    val virtualHost: String = "/",
    val username: String = "guest",
    /** The password (default `guest`); [passwordFile] wins over it. */
    val password: String? = null,
    /** A file holding the password (trimmed at both ends; an empty one fails the startup); wins over [password], as the hub token file does. */
    val passwordFile: Path? = null,
    val tls: Boolean = false,
    /** PEM CA certificates the nodes are verified against; required with TLS. */
    val tlsCaFile: Path? = null,
    /** The hosts are a load balancer: connect through it until it lands on the node needed. */
    val loadBalancer: Boolean = false,
    val offsetStoreMessages: Long = 10_000,
    val offsetStoreInterval: Duration = Duration.ofSeconds(5),
    /** The longest a publish waits for the broker's confirm. */
    val confirmTimeout: Duration = Duration.ofSeconds(30),
    val management: Management = Management(),
) {
    /** `snodra.messaging.rabbitmq.management.*`: the management API, which declares `replay`. */
    data class Management(
        /** `http(s)://host:port`, no credentials in it; none: no management API, no `replay`. */
        val url: String? = null,
        val username: String? = null,
        val password: String? = null,
        /** Wins over [password]. */
        val passwordFile: Path? = null,
        val caFile: Path? = null,
    ) {
        override fun toString(): String = "Management(url=$url, username=$username, password=<hidden>, passwordFile=$passwordFile, caFile=$caFile)"
    }

    /** The event store's settings: a password from its file when one is given, else the setting. */
    fun toSettings() = RabbitEventStoreSettings(
        hosts = hosts.flatMap { it.split(',') }.map(String::trim).filter(String::isNotEmpty).ifEmpty { hosts },
        port = port,
        virtualHost = virtualHost,
        username = username,
        password = secret(password, passwordFile, "snodra.messaging.rabbitmq.password-file") ?: "guest",
        tls = tls,
        tlsCaFile = tlsCaFile,
        loadBalancer = loadBalancer,
        offsetStoreMessages = offsetStoreMessages,
        offsetStoreInterval = offsetStoreInterval,
        confirmTimeout = confirmTimeout,
        management = management.url?.takeIf { it.isNotBlank() }?.let {
            RabbitManagementSettings(it, management.username, secret(management.password, management.passwordFile, "snodra.messaging.rabbitmq.management.password-file"), management.caFile)
        },
    )

    override fun toString(): String =
        "RabbitProperties(hosts=$hosts, port=$port, virtualHost=$virtualHost, username=$username, password=<hidden>, passwordFile=$passwordFile, " +
            "tls=$tls, tlsCaFile=$tlsCaFile, loadBalancer=$loadBalancer, offsetStoreMessages=$offsetStoreMessages, " +
            "offsetStoreInterval=$offsetStoreInterval, confirmTimeout=$confirmTimeout, management=$management)"

    private companion object {
        /**
         * [file]'s content trimmed at both ends when set, else [value]. A file that is missing, unreadable, not UTF-8 or empty
         * fails, naming [setting] and the file (never its content).
         */
        fun secret(value: String?, file: Path?, setting: String): String? {
            file ?: return value
            val content = try {
                Files.readString(file).trim()
            } catch (e: IOException) {
                throw IllegalStateException("$setting $file cannot be read: ${e.javaClass.simpleName}", e)
            }
            check(content.isNotEmpty()) { "$setting $file is empty: it must hold the password" }
            return content
        }
    }
}
