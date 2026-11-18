package com.damoreira.snodra.messaging.rabbitmq

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Ports
import com.rabbitmq.stream.Environment
import com.rabbitmq.stream.impl.Client
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Base64

/**
 * A RabbitMQ broker for the add-on's tests: `rabbitmq:4.4.0-management` with the stream and stream-management plugins, the stream
 * port published on a fixed free host port that the node advertises (the store connects to each stream's leader as the broker
 * advertises it), the management API on Docker's port. One per JVM, started on first use and removed when the JVM ends. The
 * broker's default test credentials (`guest`) only.
 */
object RabbitBroker {
    const val IMAGE = "rabbitmq:4.4.0-management"

    private val started: Started by lazy { start() }

    class Started(val container: GenericContainer<*>, val streamPort: Int, val managementUrl: String)

    val streamPort: Int get() = started.streamPort
    val managementUrl: String get() = started.managementUrl

    /** The store's settings for this broker, with or without the management API. */
    fun settings(management: Boolean = true, offsetStoreMessages: Long = 10_000, offsetStoreInterval: Duration = Duration.ofSeconds(5)) =
        RabbitEventStoreSettings(
            hosts = listOf("127.0.0.1:$streamPort"),
            management = if (management) RabbitManagementSettings(managementUrl) else null,
            offsetStoreMessages = offsetStoreMessages,
            offsetStoreInterval = offsetStoreInterval,
        )

    /** A plain client of the Java stream library (its `Environment`), as any other program on these streams would use it. */
    fun environment(): Environment = Environment.builder().host("127.0.0.1").port(streamPort).build()

    /** A plain protocol connection (the library's `Client`). */
    fun client(parameters: Client.ClientParameters = Client.ClientParameters()): Client =
        Client(parameters.host("127.0.0.1").port(streamPort))

    /** The super stream [name] with [partitions] partitions (`<name>-0` ..., binding keys `0` ...), as `rabbitmq-streams add_super_stream` creates it. */
    fun superStream(name: String, partitions: Int) = environment().use { it.streamCreator().name(name).superStream().partitions(partitions).creator().create() }

    /** The plain stream [name]. */
    fun stream(name: String) = environment().use { it.streamCreator().name(name).create() }

    private fun freePort(): Int = ServerSocket(0).use { it.localPort }

    private fun start(): Started {
        var last: Exception? = null
        // The stream port is picked here (the node must advertise it); a port taken meanwhile ("failed to bind") is picked again.
        repeat(3) {
            val port = freePort()
            val container = GenericContainer(DockerImageName.parse(IMAGE))
                .withExposedPorts(15672)
                .withCopyToContainer(Transferable.of("[rabbitmq_management,rabbitmq_stream,rabbitmq_stream_management].\n"), "/etc/rabbitmq/enabled_plugins")
                .withCopyToContainer(Transferable.of("stream.advertised_host = 127.0.0.1\nstream.advertised_port = $port\n"), "/etc/rabbitmq/conf.d/20-stream.conf")
                .withCreateContainerCmdModifier { cmd ->
                    val stream = ExposedPort.tcp(5552)
                    cmd.withExposedPorts(cmd.exposedPorts.orEmpty().toList() + stream)
                    val bindings = cmd.hostConfig!!.portBindings ?: Ports()
                    bindings.bind(stream, Ports.Binding.bindPort(port))
                    cmd.hostConfig!!.withPortBindings(bindings)
                }
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1))
                .withStartupTimeout(Duration.ofMinutes(2))
            try {
                container.start()
                val management = "http://127.0.0.1:${container.getMappedPort(15672)}"
                Runtime.getRuntime().addShutdownHook(Thread { container.stop() })
                awaitReady(port, management)
                return Started(container, port, management)
            } catch (e: Exception) {
                last = e
                runCatching { container.stop() }
                if (e.toString().contains("bind", ignoreCase = true).not()) throw e
            }
        }
        throw IllegalStateException("the RabbitMQ container did not start", last)
    }

    /** The stream listener and the management API answer a moment after the startup line. */
    private fun awaitReady(port: Int, management: String) {
        val http = HttpClient.newHttpClient()
        val auth = "Basic " + Base64.getEncoder().encodeToString("guest:guest".toByteArray())
        val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos()
        while (true) {
            val stream = runCatching { Client(Client.ClientParameters().host("127.0.0.1").port(port)).close() }.isSuccess
            val api = runCatching {
                http.send(
                    HttpRequest.newBuilder(URI("$management/api/stream/consumers/%2F")).header("Authorization", auth).build(),
                    HttpResponse.BodyHandlers.discarding(),
                ).statusCode() == 200
            }.getOrDefault(false)
            if (stream && api) return
            check(System.nanoTime() < deadline) { "the broker never answered" }
            Thread.sleep(250)
        }
    }
}
