package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Constants
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.netty.handler.ssl.SslContextBuilder
import java.time.Duration

/** One partition stream of a source: its segment and its stream's name. */
internal data class Part(val segment: String, val stream: String)

/**
 * Opens the stream protocol connections ([Client]s) of one store, subscription or call: to the configured hosts (the first that
 * answers), or to the node a stream's leader is on, as the broker advertises it (with a load balancer: through the hosts until
 * the connection lands on that node). Every connection carries [connectionName] and every request is bounded by [RPC_TIMEOUT].
 */
internal open class RabbitConnector(private val settings: RabbitEventStoreSettings, private val connectionName: String) {
    private val ssl = if (settings.tls) {
        SslContextBuilder.forClient().trustManager(settings.tlsCaFile!!.toFile()).endpointIdentificationAlgorithm("HTTPS").build()
    } else {
        null
    }

    /** A connection to [host]:[port], with [configure]'s listeners; [EventStoreException.Failed] when it cannot be opened. */
    fun connect(host: String, port: Int, configure: (Client.ClientParameters) -> Unit = {}): Client {
        val parameters = Client.ClientParameters()
            .host(host).port(port)
            .virtualHost(settings.virtualHost)
            .username(settings.username).password(settings.password)
            .clientProperty("connection_name", connectionName)
            .rpcTimeout(RPC_TIMEOUT)
        ssl?.let(parameters::sslContext)
        configure(parameters)
        return try {
            newClient(host, port, parameters)
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: cannot connect to $host:$port: ${e.message ?: e::class.java.simpleName}", e)
        }
    }

    /** Opens the connection (a test double records which node each call goes to). */
    protected open fun newClient(host: String, port: Int, parameters: Client.ClientParameters): Client = Client(parameters)

    /** A connection to the first configured host that answers. */
    fun seed(configure: (Client.ClientParameters) -> Unit = {}): Client {
        var last: Exception? = null
        for ((host, port) in settings.nodes()) {
            try {
                return connect(host, port, configure)
            } catch (e: EventStoreException.Failed) {
                last = e
            }
        }
        throw EventStoreException.Failed("rabbitmq: no host of ${settings.hosts} answers: ${last?.message}", last)
    }

    /** A connection to [node] (a stream's leader as the broker advertises it). */
    fun node(node: Client.Broker, configure: (Client.ClientParameters) -> Unit = {}): Client {
        if (!settings.loadBalancer) return connect(node.host, node.port, configure)
        repeat(LOAD_BALANCER_TRIES) {
            val client = seed(configure)
            val landed = try {
                advertised(client)
            } catch (e: Exception) {
                runCatching { client.close() }
                throw EventStoreException.Failed("rabbitmq: the node a connection landed on cannot be read: ${e.message}", e)
            }
            if (landed == node.host to node.port) return client
            runCatching { client.close() }
        }
        throw EventStoreException.Failed("rabbitmq: the load balancer did not reach the node $node in $LOAD_BALANCER_TRIES tries")
    }

    companion object {
        /** The longest one request on a connection may take. */
        val RPC_TIMEOUT: Duration = Duration.ofSeconds(10)

        /** How many connections through a load balancer are tried to reach one node. */
        const val LOAD_BALANCER_TRIES = 32

        /**
         * The node [client] landed on, as the broker advertises it (the client keeps it package-private: read reflectively, as the
         * client's own `Environment` reads it to go through a load balancer).
         */
        fun advertised(client: Client): Pair<String, Int> {
            fun call(name: String) = Client::class.java.getDeclaredMethod(name).apply { isAccessible = true }.invoke(client)
            return call(ADVERTISED_HOST) as String to (call(ADVERTISED_PORT) as Int)
        }

        /** The client's package-private methods [advertised] calls. */
        const val ADVERTISED_HOST = "serverAdvertisedHost"
        const val ADVERTISED_PORT = "serverAdvertisedPort"

        /**
         * The partitions of [source] on [control]: a super stream's partition streams in the broker's order (segment: the suffix),
         * a plain stream as one segment `0`, null when neither exists.
         */
        fun layout(control: Client, source: String): List<Part>? {
            val partitions = try {
                control.partitions(source)
            } catch (e: Exception) {
                throw EventStoreException.Failed("rabbitmq: the partitions of $source: ${e.message}", e)
            }
            if (partitions.isNotEmpty()) {
                return partitions.map { stream ->
                    val segment = RabbitWire.segmentOf(source, stream)
                    if (!RabbitWire.segmentFits(segment)) {
                        throw EventStoreException.Invalid("the partition $stream of $source makes a segment name over 128 bytes")
                    }
                    Part(segment, stream)
                }
            }
            val metadata = metadata(control, source)
            return if (metadata.isResponseOk) listOf(Part(RabbitWire.SINGLE_SEGMENT, source)) else null
        }

        /** The leader of [stream]; [EventStoreException.Failed] when the stream is missing or has none now. */
        fun leader(control: Client, stream: String): Client.Broker {
            val metadata = metadata(control, stream)
            if (!metadata.isResponseOk) throw EventStoreException.Failed(noLeader(stream, metadata.responseCode))
            return metadata.leader
                ?: throw EventStoreException.Failed("rabbitmq: the stream $stream has no leader now (a leader election?); retried")
        }

        /**
         * What a failed leader lookup of [stream] says. While a stream elects a leader (its leader's node was lost) the broker
         * answers "does not exist" or "not available" for a stream that exists: the message says both, so an operator does not
         * go looking for a deleted stream. The caller's retry is unchanged (a failed publish or subscribe, retried).
         */
        internal fun noLeader(stream: String, code: Short): String = when (code) {
            Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE ->
                "rabbitmq: the stream $stream has no leader the broker can name now (code $code): a leader election " +
                    "after a node was lost, or the stream was deleted; retried"
            else -> "rabbitmq: the stream $stream: ${codeName(code)}"
        }

        private fun metadata(control: Client, stream: String): Client.StreamMetadata = try {
            control.metadata(stream)[stream] ?: throw EventStoreException.Failed("rabbitmq: no metadata for $stream")
        } catch (e: EventStoreException) {
            throw e
        } catch (e: Exception) {
            throw EventStoreException.Failed("rabbitmq: the metadata of $stream: ${e.message}", e)
        }

        /** The offset stored under [reference] on [stream]: null when none is stored; [EventStoreException.Failed] when it cannot be read. */
        fun storedOffset(control: Client, reference: String, stream: String): Long? {
            val response = try {
                control.queryOffset(reference, stream)
            } catch (e: Exception) {
                throw EventStoreException.Failed("rabbitmq: reading the offset of $reference on $stream: ${e.message}", e)
            }
            return when (response.responseCode) {
                Constants.RESPONSE_CODE_OK -> response.offset
                Constants.RESPONSE_CODE_NO_OFFSET -> null
                else -> throw EventStoreException.Failed("rabbitmq: reading the offset of $reference on $stream: ${codeName(response.responseCode)}")
            }
        }

        /**
         * Stores [offset] under [reference] on [stream] through [writer] (unless [store] is false: it was stored already) and waits
         * until [control] reads it back (a store is asynchronous on the broker), at most [wait].
         */
        fun storeAndConfirm(writer: Client, control: Client, reference: String, stream: String, offset: Long, wait: Duration, store: Boolean = true) {
            if (store) {
                try {
                    writer.storeOffset(reference, stream, offset)
                } catch (e: Exception) {
                    throw EventStoreException.Failed("rabbitmq: storing the offset of $reference on $stream: ${e.message}", e)
                }
            }
            val deadline = System.nanoTime() + wait.toNanos()
            while (true) {
                if (storedOffset(control, reference, stream) == offset) return
                if (System.nanoTime() > deadline) {
                    throw EventStoreException.Failed("rabbitmq: the offset $offset of $reference on $stream was not read back within $wait")
                }
                Thread.sleep(20)
            }
        }

        /** A response code's name, for messages. */
        fun codeName(code: Short): String = when (code) {
            // The broker gives both answers while a stream elects a leader, for a stream that exists.
            Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST -> "the stream does not exist, or has no leader now (a leader election)"
            Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE -> "the stream is not available (a leader election?)"
            Constants.RESPONSE_CODE_ACCESS_REFUSED -> "access refused"
            Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS -> "subscription id already exists"
            Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST -> "publisher does not exist"
            else -> "response code $code"
        }
    }
}
