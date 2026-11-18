package com.damoreira.snodra.messaging.hub

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The hub choice for keyed requests; the Rust library must produce the same values (shared vectors below). */
class RendezvousTest {

    @Test fun `reference vectors`() {
        assertEquals(0xcbf29ce484222325uL.toLong(), Rendezvous.fnv1a64(""))
        assertEquals(0xaf63dc4c8601ec8cuL.toLong(), Rendezvous.fnv1a64("a"))
        assertEquals(0L, Rendezvous.fmix64(0))
        assertEquals(0xb456bcfc34c2cb2cuL.toLong(), Rendezvous.fmix64(1))
        assertEquals(0xd8d5618ff3a93f55uL.toLong(), Rendezvous.score("hub-a", "order-1"))
        assertEquals(0xeb119984ce491337uL.toLong(), Rendezvous.score("hub-b", "order-1"))
        assertEquals(0x9d21a20b3a1b75f6uL.toLong(), Rendezvous.score("hub-c", "order-1"))
        assertEquals(0x5c855509f5ad7c8duL.toLong(), Rendezvous.score("hub-1", "order-42")) // the Rust library's vector
    }

    @Test fun `the highest unsigned score wins`() {
        val hubs = listOf("hub-a", "hub-b", "hub-c")
        assertEquals("hub-b", Rendezvous.choose("order-1", hubs) { it })
        // key-0: hub-a 0x543f3e82e8a29a42, hub-b 0xbd443f5acaf98254 (negative as a signed Long, yet the highest).
        assertEquals(0xbd443f5acaf98254uL.toLong(), Rendezvous.score("hub-b", "key-0"))
        assertEquals("hub-b", Rendezvous.choose("key-0", hubs) { it })
        assertNull(Rendezvous.choose("key-0", emptyList<String>()) { it })
    }

    @Test fun `the choice is stable and independent of the candidate order`() {
        val hubs = listOf("hub-a", "hub-b", "hub-c", "hub-d")
        for (i in 0 until 500) {
            val key = "key-$i"
            val first = Rendezvous.choose(key, hubs) { it }
            assertEquals(first, Rendezvous.choose(key, hubs.reversed()) { it })
            assertEquals(first, Rendezvous.choose(key, hubs.shuffled()) { it })
        }
    }

    @Test fun `a hub leaving moves only its own keys, and they spread over the others`() {
        val hubs = listOf("hub-a", "hub-b", "hub-c", "hub-d")
        val keys = (0 until 4_000).map { "key-$it" } // sequential keys: the hardest case for an unmixed hash
        val before = keys.associateWith { Rendezvous.choose(it, hubs) { h -> h }!! }
        val after = keys.associateWith { Rendezvous.choose(it, hubs - "hub-c") { h -> h }!! }
        for (key in keys) {
            if (before[key] != "hub-c") assertEquals(before[key], after[key], key)
        }
        val moved = keys.filter { before[it] == "hub-c" }.groupingBy { after[it] }.eachCount()
        assertEquals(setOf("hub-a", "hub-b", "hub-d"), moved.keys)
        // Roughly even: each hub owned about a quarter of the keys before.
        before.values.groupingBy { it }.eachCount().values.forEach { assertTrue(it in 900..1_100, "share $it of 4000") }
    }

    @Test fun `initial permits are split as ceil(max over hubs)`() {
        assertEquals(256, HubProtocol.initialPermits(256, 1))
        assertEquals(128, HubProtocol.initialPermits(256, 2))
        assertEquals(86, HubProtocol.initialPermits(256, 3))
        assertEquals(1, HubProtocol.initialPermits(1, 3))
        assertEquals(2, HubProtocol.initialPermits(3, 2))
        assertEquals(10, HubProtocol.initialPermits(10, 0)) // no hub counted yet: the whole budget
    }
}
