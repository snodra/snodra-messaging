package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.Rows
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RowsHandlerTest {
    private val buses = InMemoryBuses(typePrefix = "service-b").register(RowsHandler("service-b"))

    @Test fun `Rows streams the rows 1 to count naming the service and instance`() = runBlocking {
        val rows = buses.stream(Rows(5)).toList()

        assertEquals((1..5).toList(), rows.map { it.n })
        assertTrue(rows.all { it.service == "service-b" && it.instance.isNotBlank() }, "$rows")
        assertEquals(emptyList(), buses.stream(Rows(0)).toList().map { it.n })
    }

    @Test fun `the collector may stop early`() = runBlocking {
        assertEquals(listOf(1, 2), buses.stream(Rows(1000)).take(2).toList().map { it.n })
    }

    @Test fun `failAt fails the stream after the rows before it`() = runBlocking {
        val seen = mutableListOf<Int>()
        val e = assertFailsWith<RemoteHandlerException> { buses.stream(Rows(10, failAt = 3)).collect { seen += it.n } }
        assertEquals(listOf(1, 2), seen)
        assertEquals("IllegalStateException: Rows failed at row 3", e.message)
    }

    @Test fun `the handler is registered as a shared query type`() {
        assertTrue("examples.Rows" in buses.registry.sharedQueryTypes(), "${buses.registry.sharedQueryTypes()}")
    }
}
