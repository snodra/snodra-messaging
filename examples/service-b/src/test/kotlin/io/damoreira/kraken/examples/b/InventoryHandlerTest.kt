package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.Inventory
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InventoryHandlerTest {
    @Test fun `Inventory answers with the service, a fixed count and how often it answered`() = runBlocking {
        val buses = InMemoryBuses(typePrefix = "service-b").register(InventoryHandler("service-b"))

        val first = buses.send(Inventory("widget"))
        val second = buses.send(Inventory("gadget"))

        assertEquals("service-b", first.service)
        assertTrue(first.instance.isNotBlank())
        assertEquals("widget", first.item)
        assertEquals(InventoryHandler.COUNT, first.count)
        assertEquals(1, first.answered)
        assertEquals(2, second.answered)
    }

    @Test fun `the handler is registered as a shared query type`() {
        val buses = InMemoryBuses(typePrefix = "service-b").register(InventoryHandler("service-b"))
        assertTrue("examples.Inventory" in buses.registry.sharedQueryTypes(), "${buses.registry.sharedQueryTypes()}")
    }
}
