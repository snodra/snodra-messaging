package com.damoreira.snodra.messaging.test.inmemory

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.memory.MemoryEventStore
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.ProcessingGroup
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.test.InMemoryEvents
import com.damoreira.snodra.messaging.test.SnodraInMemory
import com.damoreira.snodra.messaging.test.SnodraInMemoryExclusions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.transaction.annotation.Transactional
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

@MessageType("inmemory.Ordered")
data class Ordered(val orderId: String, val poison: Boolean = false) : Event

@MessageType("inmemory.Shipped")
data class Shipped(val orderId: String) : Event

/** Ships every order it is told of, in the processor `shipping`; a poison order fails. */
@ProcessingGroup("shipping")
open class Shipping(private val events: EventBus) {
    val shipped = CopyOnWriteArrayList<String>()

    @EventHandler
    fun on(order: Ordered) {
        check(!order.poison) { "cannot ship ${order.orderId}" }
        shipped += order.orderId
        events.publishBlocking(Shipped(order.orderId), key = order.orderId)
    }
}

/** Takes orders through its own `@Transactional` method. */
open class Orders(private val events: EventBus) {
    @Transactional
    open fun order(orderId: String) = events.publishBlocking(Ordered(orderId), key = orderId)
}

@SpringBootConfiguration(proxyBeanMethods = false)
@EnableAutoConfiguration
open class InMemoryApplication {
    @Bean open fun shipping(events: EventBus) = Shipping(events)

    @Bean open fun orders(events: EventBus) = Orders(events)
}

/**
 * A service test in the zero-infrastructure mode: the whole application on the in-memory stores, no container, no hub; events
 * go in through [InMemoryEvents.publish] and the service's own publishes and dead letters come out through [InMemoryEvents].
 */
@SpringBootTest(
    classes = [InMemoryApplication::class],
    properties = ["snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=orders", "snodra.messaging.events.consumer.max-attempts=1"],
)
@SnodraInMemory
class SnodraInMemoryTest {
    @Autowired lateinit var events: InMemoryEvents

    @Autowired lateinit var shipping: Shipping

    @Autowired lateinit var orders: Orders

    @Autowired lateinit var context: ApplicationContext

    @Test
    fun theApplicationRunsOnTheInMemoryStoresWithoutAHub() {
        assertIs<MemoryEventStore>(context.getBean(EventStore::class.java))
        assertIs<MemoryMessageStore>(context.getBean(MessageStore::class.java))
        assertTrue(context.getBeanNamesForType(com.damoreira.snodra.messaging.CommandBus::class.java).isEmpty(), "no hub")
    }

    @Test
    fun theApplicationsOwnAutoConfigurationExclusionsAreKept() {
        assertTrue(context.getBeanNamesForType(ExcludedProbe::class.java).isEmpty(), "the application's spring.autoconfigure.exclude still applies")
        assertTrue(context.getBeanNamesForType(KeptProbe::class.java).isNotEmpty(), "the other auto-configurations run")
    }

    @Test
    fun theDefaultExclusionsAreSnodraInMemoryExclusionsDefault() {
        @Suppress("UNCHECKED_CAST")
        val default = SnodraInMemory::class.java.getMethod("exclude").defaultValue as Array<String>
        assertEquals(SnodraInMemoryExclusions.DEFAULT, default.toList())
    }

    @Test
    fun anEventPublishedToTheServiceIsHandledAndWhatItPublishesIsRead() {
        val id = events.publish(Ordered("o-1"), key = "o-1")
        events.awaitProcessed(id, processor = "shipping")
        assertTrue("o-1" in shipping.shipped)
        assertTrue(Shipped("o-1") in events.awaitPublished(Shipped::class))
        assertEquals("orders", events.published().first { it.type == "inmemory.Shipped" }.headers["source"])
    }

    @Test
    fun whatTheServicePublishesInATransactionReachesItsOwnProcessors() {
        orders.order("o-2")
        until("o-2 ordered") { Ordered("o-2") in events.published(Ordered::class) }
        until("o-2 shipped by the service's own processor") { Shipped("o-2") in events.published(Shipped::class) }
    }

    private fun until(what: String, done: () -> Boolean) {
        val deadline = System.nanoTime() + 10_000_000_000L
        while (!done()) {
            if (System.nanoTime() > deadline) throw AssertionError("$what within 10 s")
            Thread.sleep(20)
        }
    }

    @Test
    fun aRecordTheProcessorCannotHandleIsDeadLettered() {
        val id = events.publish(Ordered("o-3", poison = true), key = "o-3")
        val letter = events.awaitDeadLetters("shipping").single { it.eventId == id }
        assertEquals(DeadLetterReason.HANDLER_FAILED, letter.reason)
        assertEquals("o-3", letter.key)
        assertTrue("cannot ship o-3" in letter.error.message, letter.error.message)
    }
}
