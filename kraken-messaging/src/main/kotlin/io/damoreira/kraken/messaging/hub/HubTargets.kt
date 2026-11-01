package io.damoreira.kraken.messaging.hub

import io.grpc.ManagedChannel
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress

/**
 * The hubs named by `kraken.messaging.hub.address` (ADR 0016): either a fixed list of gRPC targets or one DNS name whose
 * addresses are the hubs.
 *
 * - `host:port` or `static://host:port`: one hub, today's behaviour.
 * - `host-1:7000,static://host-2:7000`: one hub per entry (blank entries and repeats are dropped, order kept).
 * - `dns:///name:port`: every address `name` resolves to is a hub (`ip:port`), re-resolved every 30 s; it cannot be
 *   combined with other entries, and the port is required. Only one address family is used ([pickAddresses]).
 */
sealed interface HubTargets {
    data class Static(val targets: List<String>) : HubTargets {
        init {
            require(targets.isNotEmpty()) { "at least one hub target is required" }
        }
    }

    data class Dns(val host: String, val port: Int) : HubTargets

    companion object {
        private const val STATIC = "static://"
        private const val DNS = "dns:"
        private const val DNS_PREFIX = "dns:///"

        /** @throws IllegalArgumentException for an empty, malformed or mixed address */
        fun parse(address: String): HubTargets {
            val entries = address.split(',').map(String::trim).filter(String::isNotEmpty)
            require(entries.isNotEmpty()) { "kraken.messaging.hub.address names no hub: '$address'" }
            if (entries.any { it.startsWith(DNS) }) {
                require(entries.size == 1) { "kraken.messaging.hub.address: a dns:/// target cannot be combined with others: '$address'" }
                return parseDns(entries.single())
            }
            val targets = entries.map { it.removePrefix(STATIC).trim() }
            require(targets.none(String::isEmpty)) { "kraken.messaging.hub.address has an empty target: '$address'" }
            return Static(targets.distinct())
        }

        private fun parseDns(entry: String): Dns {
            require(entry.startsWith(DNS_PREFIX)) { "kraken.messaging.hub.address: expected dns:///name:port, was '$entry'" }
            val rest = entry.removePrefix(DNS_PREFIX)
            val colon = rest.lastIndexOf(':')
            require(colon > 0) { "kraken.messaging.hub.address: expected dns:///name:port (port required), was '$entry'" }
            val host = rest.substring(0, colon)
            val port = rest.substring(colon + 1).toIntOrNull()
            require(host.isNotBlank() && '/' !in host) { "kraken.messaging.hub.address: bad host in '$entry'" }
            require(port != null && port in 1..65_535) { "kraken.messaging.hub.address: bad port in '$entry'" }
            return Dns(host, port)
        }

        /**
         * The hub addresses of one DNS answer (same rule as the Rust library): the IPv4 addresses when there is any, else
         * the IPv6 ones; repeats (an IPv6 address differing only in its scope id included) dropped, answer order kept.
         * A dual-stack name lists every hub under both families: dialing both would open two streams to each hub with
         * one instance id, and the hub keeps only the newer one.
         */
        fun pickAddresses(addresses: List<InetAddress>): List<InetAddress> {
            val v4 = addresses.filterIsInstance<Inet4Address>()
            val family: List<InetAddress> = v4.ifEmpty { addresses.filterIsInstance<Inet6Address>() }
            return family.distinctBy { it.hostAddress.substringBefore('%') }
        }

        /** The gRPC target of one resolved hub address: `ip:port`, an IPv6 address in brackets. */
        fun target(address: InetAddress, port: Int): String =
            if (address is Inet6Address) "[${address.hostAddress.substringBefore('%')}]:$port" else "${address.hostAddress}:$port"
    }
}

/** Builds the channel of one hub target (`host:port`), called once per connection attempt to that hub. */
fun interface HubChannelFactory {
    fun channel(target: String): ManagedChannel
}
