package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.Counter
import io.damoreira.kraken.examples.api.CounterChanged
import io.damoreira.kraken.examples.api.WatchCounter
import io.damoreira.kraken.messaging.complete
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CounterHandlerTest {
    private val buses = InMemoryBuses(typePrefix = "service-b")
    private val handler = CounterHandler("service-b", buses.emitter).also { buses.register(it) }

    @Test fun `the initial result is the counter's current value, 0 for a name nobody incremented`() = runBlocking {
        val first = buses.subscribe(WatchCounter("fresh")).initial
        assertEquals("service-b", first.service)
        assertTrue(first.instance.isNotBlank())
        assertEquals(Counter("service-b", first.instance, "fresh", 0), first)

        handler.increment("fresh")
        handler.increment("fresh")
        assertEquals(2, buses.subscribe(WatchCounter("fresh")).initial.value)
    }

    @Test fun `increment returns the new value and counts every name on its own`() {
        assertEquals(1, handler.increment("a").value)
        assertEquals(2, handler.increment("a").value)
        assertEquals(1, handler.increment("b").value)
        assertEquals("a", handler.increment("a").name)
    }

    @Test fun `increment emits the change to the subscriptions of that name, in order`() = runBlocking {
        val a = buses.subscribe(WatchCounter("a"))
        val b = buses.subscribe(WatchCounter("b"))

        handler.increment("a")
        handler.increment("a")
        handler.increment("b")
        buses.emitter.complete<WatchCounter> { true }

        val instance = a.initial.instance
        assertEquals(
            listOf(CounterChanged("service-b", instance, "a", 1), CounterChanged("service-b", instance, "a", 2)),
            a.updates.toList(),
        )
        assertEquals(listOf(CounterChanged("service-b", instance, "b", 1)), b.updates.toList())
    }

    @Test fun `the handler is registered as a shared query type`() {
        assertTrue("examples.WatchCounter" in buses.registry.sharedQueryTypes(), "${buses.registry.sharedQueryTypes()}")
    }
}
