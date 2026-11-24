package com.damoreira.snodra.messaging.rabbitmq

import java.net.URI
import java.nio.file.Path
import java.time.Duration

/**
 * How [RabbitEventStore] reaches RabbitMQ (`snodra.messaging.rabbitmq.*` in a Spring application, [RabbitProperties]).
 * The source and its dead-letter stream are the core's
 * (`snodra.messaging.events.*`), as the Kafka add-on's topics.
 *
 * Refuses (an [IllegalArgumentException]) no host, a host that is not `host[:port]`, TLS without a CA file (the store never
 * connects unverified), an offset store policy of zero, and a management URL that is not `http(s)` or that carries credentials.
 * Passwords never appear in [toString].
 */
data class RabbitEventStoreSettings(
    /** Nodes to connect to first, `host` or `host:port`, tried in order; then each stream's leader as the broker advertises it. */
    val hosts: List<String> = listOf("rabbitmq"),
    /** The port of a host given without one: 5552, 5551 with TLS. */
    val port: Int? = null,
    val virtualHost: String = "/",
    val username: String = "guest",
    val password: String = "guest",
    /** TLS to every node, verified against [tlsCaFile]. */
    val tls: Boolean = false,
    /** PEM CA certificates the nodes are verified against; required with TLS. */
    val tlsCaFile: Path? = null,
    /** The hosts are a load balancer: connect through it until it lands on the node needed. */
    val loadBalancer: Boolean = false,
    /** A subscription stores its position every so many acknowledgements... */
    val offsetStoreMessages: Long = 10_000,
    /** ...or this often, whichever first, and always when a partition moves away and at close. */
    val offsetStoreInterval: Duration = Duration.ofSeconds(5),
    /** The longest a publish waits for the broker's confirm (a connection that confirms nothing within it is replaced). */
    val confirmTimeout: Duration = Duration.ofSeconds(30),
    /** The management API: declares `replay` (a reset needs to know who reads); null without it. */
    val management: RabbitManagementSettings? = null,
) {
    init {
        require(hosts.isNotEmpty() && hosts.none { it.isBlank() }) { "snodra.messaging.rabbitmq.hosts: at least one host, none blank" }
        nodes() // every host parses
        require(port == null || port in 1..65535) { "snodra.messaging.rabbitmq.port must be a port, was $port" }
        require(!tls || tlsCaFile != null) {
            "snodra.messaging.rabbitmq.tls needs snodra.messaging.rabbitmq.tls-ca-file: the nodes are verified against it, never trusted unverified"
        }
        require(offsetStoreMessages >= 1) { "snodra.messaging.rabbitmq.offset-store-messages must be at least 1, was $offsetStoreMessages" }
        require(confirmTimeout > Duration.ZERO) { "snodra.messaging.rabbitmq.confirm-timeout must be positive, was $confirmTimeout" }
        require(offsetStoreInterval > Duration.ZERO) { "snodra.messaging.rabbitmq.offset-store-interval must be positive, was $offsetStoreInterval" }
    }

    /** The port of a host given without one. */
    val defaultPort: Int get() = port ?: if (tls) DEFAULT_TLS_PORT else DEFAULT_PORT

    /** The hosts as (host, port), in order. */
    fun nodes(): List<Pair<String, Int>> = hosts.map { raw ->
        val host = raw.trim()
        val colon = host.lastIndexOf(':')
        if (colon < 0 || host.startsWith("[") && !host.contains("]:")) {
            host to defaultPort
        } else {
            val p = host.substring(colon + 1).toIntOrNull()?.takeIf { it in 1..65535 }
                ?: throw IllegalArgumentException("snodra.messaging.rabbitmq.hosts: \"$host\" is not host or host:port")
            require(colon > 0) { "snodra.messaging.rabbitmq.hosts: \"$host\" has no host" }
            host.substring(0, colon) to p
        }
    }

    override fun toString(): String =
        "RabbitEventStoreSettings(hosts=$hosts, port=$port, virtualHost=$virtualHost, username=$username, password=<hidden>, tls=$tls, " +
            "tlsCaFile=$tlsCaFile, loadBalancer=$loadBalancer, offsetStoreMessages=$offsetStoreMessages, offsetStoreInterval=$offsetStoreInterval, " +
            "confirmTimeout=$confirmTimeout, management=$management)"

    companion object {
        const val DEFAULT_PORT = 5552
        const val DEFAULT_TLS_PORT = 5551
    }
}

/**
 * The RabbitMQ management API (plugins `rabbitmq_management` and `rabbitmq_stream_management`, metrics collection on): the only
 * way to tell which instances read a subscription. [url] is `http(s)://host:port`, without credentials; [username] and
 * [password] default to the stream user's.
 */
data class RabbitManagementSettings(
    val url: String,
    val username: String? = null,
    val password: String? = null,
    /** PEM CA certificates of an `https` management API. */
    val caFile: Path? = null,
) {
    init {
        val uri = runCatching { URI(url.trim()) }.getOrNull()
        require(uri != null && (uri.scheme == "http" || uri.scheme == "https") && !uri.host.isNullOrBlank()) {
            "snodra.messaging.rabbitmq.management.url must be http(s)://host:port"
        }
        require(uri.rawUserInfo == null) {
            "snodra.messaging.rabbitmq.management.url must not carry credentials: set snodra.messaging.rabbitmq.management.username and password"
        }
    }

    /** The user and password of the API: these, else the stream user's ([username], [password]). */
    fun login(streamUser: String, streamPassword: String): Pair<String, String> =
        (username ?: streamUser) to (password ?: streamPassword)

    override fun toString(): String = "RabbitManagementSettings(url=$url, username=$username, password=<hidden>, caFile=$caFile)"
}
