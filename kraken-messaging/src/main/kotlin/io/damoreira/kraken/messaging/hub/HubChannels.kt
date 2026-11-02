package io.damoreira.kraken.messaging.hub

import io.grpc.Grpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.TlsChannelCredentials
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * TLS towards the hub (P5a, ADR 0018). [enabled] false is plain text; [caFile] is the PEM CA that signed the hub's
 * certificate (the JVM's system roots when null); [serverName] is the name the certificate is verified against instead
 * of the address's host. mTLS (a client certificate) is P5c: see [DefaultHubChannelFactory.customizeTls].
 */
data class HubTls(val enabled: Boolean = false, val caFile: Path? = null, val serverName: String? = null)

/**
 * The [HubChannelFactory] of the starter: one channel per connection attempt, plain text or TLS by [tls], for every
 * address form (`host:port`, a comma-separated list, `dns:///name:port`: the target string is handed to gRPC as it is).
 * HTTP/2 pings detect a dead peer at the transport level even while the stream is idle; 10 s / 5 s catches a one-way
 * partition about as fast as the hub's 10 s heartbeat timeout does.
 */
open class DefaultHubChannelFactory(private val tls: HubTls = HubTls()) : HubChannelFactory {
    override fun channel(target: String): ManagedChannel = try {
        val builder: ManagedChannelBuilder<*> =
            if (tls.enabled) Grpc.newChannelBuilder(target, credentials()) else ManagedChannelBuilder.forTarget(target).usePlaintext()
        tls.serverName?.let { builder.overrideAuthority(it) }
        builder
            .keepAliveTime(10, TimeUnit.SECONDS)
            .keepAliveTimeout(5, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .build()
    } catch (e: RuntimeException) {
        // gRPC reads the CA certificates while building: say which setting is wrong instead of a bare parser message.
        if (!tls.enabled || e is IllegalStateException) throw e
        throw IllegalStateException("${if (tls.caFile != null) "kraken.messaging.hub.tls.ca-file ${tls.caFile}" else "kraken.messaging.hub.tls"} cannot be used: ${e.message}", e)
    }

    /** Builds a channel (it does not connect) and drops it: a bad CA file fails the startup, not every attempt. */
    fun validate() {
        if (tls.enabled) channel("localhost:1").shutdownNow()
    }

    private fun credentials(): io.grpc.ChannelCredentials {
        val builder = TlsChannelCredentials.newBuilder()
        try {
            tls.caFile?.let { builder.trustManager(it.toFile()) }
        } catch (e: IOException) {
            throw IllegalStateException("kraken.messaging.hub.tls.ca-file ${tls.caFile} cannot be used: ${e.message}", e)
        } catch (e: java.security.GeneralSecurityException) {
            throw IllegalStateException("kraken.messaging.hub.tls.ca-file ${tls.caFile} cannot be used: ${e.message}", e)
        }
        customizeTls(builder)
        return builder.build()
    }

    /**
     * The hook for mTLS (P5c): called with the credentials builder of every TLS channel, where a client certificate
     * (`keyManager(...)`) will be set. Not implemented in P5a: it does nothing.
     */
    protected open fun customizeTls(credentials: TlsChannelCredentials.Builder) = Unit
}
