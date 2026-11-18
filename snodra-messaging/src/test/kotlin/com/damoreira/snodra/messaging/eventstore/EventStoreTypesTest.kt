package com.damoreira.snodra.messaging.eventstore

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class EventStoreTypesTest {
    @Test
    fun `capabilities have the protocol names`() {
        assertEquals(listOf("replay", "browse", "lag", "native-dead-letter"), Capability.entries.map { it.wireName })
        assertEquals("native-dead-letter", Capability.NATIVE_DEAD_LETTER.toString())
    }

    @Test
    fun `the dead-letter source is the source with dlt`() {
        assertEquals("snodra.events.dlt", deadLetterSource("snodra.events"))
    }

    @Test
    fun `the seven headers have the protocol names`() {
        assertEquals(listOf("type", "id", "correlationId", "timestamp", "source", "userId", "traceparent"), EventHeaders.ALL)
    }

    @Test
    fun `payloads compare by content`() {
        val a = Delivery("0", "1", "k", mapOf("id" to "e"), "{}".toByteArray(), 5)
        assertEquals(a, Delivery("0", "1", "k", mapOf("id" to "e"), "{}".toByteArray(), 5))
        assertNotEquals(a, Delivery("0", "1", "k", mapOf("id" to "e"), "{ }".toByteArray(), 5))
        val e = OutgoingEvent("s", "e", "t", null, emptyMap(), "{}".toByteArray())
        assertEquals(e, OutgoingEvent("s", "e", "t", null, emptyMap(), "{}".toByteArray()))
    }
}
