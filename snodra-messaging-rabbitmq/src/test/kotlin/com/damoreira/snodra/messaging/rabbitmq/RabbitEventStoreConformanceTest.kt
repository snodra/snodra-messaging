package com.damoreira.snodra.messaging.rabbitmq

import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.deadLetterSource
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.test.eventstore.EventStoreConformanceTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * The event store conformance kit against a real broker ([RabbitBroker]): a super stream of three partitions and its `.dlt`
 * stream, one [RabbitEventStore] per service instance, with the management API (every check, `replay` included). The markers of
 * the at-least-once check are kept in the core's in-memory message store.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
open class RabbitEventStoreConformanceTest : EventStoreConformanceTest() {
    private val opened = mutableListOf<RabbitEventStore>()
    private val markers = MemoryMessageStore()
    protected open val management = true
    override val source: String = "snodra.conformance." + UUID.randomUUID().toString().replace("-", "").take(12)

    @BeforeAll
    fun streams() {
        RabbitBroker.superStream(source, 3)
        RabbitBroker.stream(deadLetterSource(source))
    }

    @AfterAll
    fun closeStores() = synchronized(opened) { opened.forEach { it.close() } }

    override fun eventStore(): EventStore =
        RabbitEventStore.open(RabbitBroker.settings(management = management)).also { synchronized(opened) { opened += it } }

    override fun messageStore(): MessageStore = markers

    @Test
    fun `the capabilities follow the management API`() {
        val expected = if (management) Capability.entries.toSet() else setOf(Capability.BROWSE, Capability.LAG, Capability.NATIVE_DEAD_LETTER)
        assertEquals(expected, eventStore().capabilities)
    }
}

/** The kit on the default configuration: no management API, so no `replay` (the kit checks the refusal). */
class RabbitEventStoreWithoutManagementConformanceTest : RabbitEventStoreConformanceTest() {
    override val management = false
}
