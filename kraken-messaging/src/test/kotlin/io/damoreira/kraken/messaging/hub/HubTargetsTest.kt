package io.damoreira.kraken.messaging.hub

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class HubTargetsTest {

    @Test fun `a single target is today's address, with or without static prefix`() {
        assertEquals(HubTargets.Static(listOf("kraken-hub:7000")), HubTargets.parse("static://kraken-hub:7000"))
        assertEquals(HubTargets.Static(listOf("kraken-hub:7000")), HubTargets.parse("kraken-hub:7000"))
        assertEquals(HubTargets.Static(listOf("10.0.0.1:7000")), HubTargets.parse("  10.0.0.1:7000 "))
    }

    @Test fun `a comma-separated list gives one target per entry, in order, without duplicates`() {
        assertEquals(
            HubTargets.Static(listOf("hub-1:7000", "hub-2:7000", "hub-3:7100")),
            HubTargets.parse("static://hub-1:7000, hub-2:7000,static://hub-3:7100,hub-1:7000"),
        )
        assertEquals(HubTargets.Static(listOf("hub-1:7000", "hub-2:7000")), HubTargets.parse("hub-1:7000,,hub-2:7000,"))
    }

    @Test fun `a dns target is resolved, never combined with other targets`() {
        assertEquals(HubTargets.Dns("kraken-hub.svc", 7000), HubTargets.parse("dns:///kraken-hub.svc:7000"))
        assertEquals(HubTargets.Dns("kraken-hub", 7002), HubTargets.parse(" dns:///kraken-hub:7002 "))
        assertFailsWith<IllegalArgumentException> { HubTargets.parse("dns:///hub-a:7000,hub-b:7000") }
        assertFailsWith<IllegalArgumentException> { HubTargets.parse("dns:///hub-a:7000,dns:///hub-b:7000") }
    }

    @Test fun `malformed addresses are rejected`() {
        for (bad in listOf("", " , ", "static://", "dns:///kraken-hub", "dns:///:7000", "dns:///hub:0", "dns:///hub:x", "dns://authority/hub:7000")) {
            assertFailsWith<IllegalArgumentException>(bad) { HubTargets.parse(bad) }
        }
    }

    @Test fun `resolved addresses become host-port targets`() {
        assertEquals("10.1.2.3:7000", HubTargets.target(java.net.InetAddress.getByName("10.1.2.3"), 7000))
        assertEquals("[0:0:0:0:0:0:0:1]:7000", HubTargets.target(java.net.InetAddress.getByName("::1"), 7000))
    }

    @Test fun `a dns answer keeps one address family, IPv4 when it has any, without repeats`() {
        fun ips(vararg a: String) = a.map(java.net.InetAddress::getByName)
        // Dual stack: the A and AAAA records name the same hubs; dialing both would register every instance twice on each.
        assertEquals(ips("10.0.0.1", "10.0.0.2"), HubTargets.pickAddresses(ips("fd00::1", "10.0.0.1", "fd00::2", "10.0.0.2", "10.0.0.1")))
        assertEquals(ips("fd00::1", "fd00::2"), HubTargets.pickAddresses(ips("fd00::1", "fd00::2", "fd00::1")))
        assertEquals(emptyList(), HubTargets.pickAddresses(emptyList()))
        // Scope ids name the same address: one target.
        assertEquals(1, HubTargets.pickAddresses(ips("fe80::1%1", "fe80::1%2")).size)
    }
}
