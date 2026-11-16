package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UngroupedProjection {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: Happened) { seen += "default:${event.id}" }
}

@ProcessingGroup("orders")
class OrdersProjection {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: Happened, ctx: MessageContext) { seen += "orders:${event.id}" }
    // A command handler in a grouped class is not affected by the group (only event handlers have processors).
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

@ProcessingGroup("notify-2")
class NotifyProjection {
    @EventHandler fun on(event: Happened) {}
}

@ProcessingGroup("Orders") class UpperCaseGroup { @EventHandler fun on(event: Happened) {} }
@ProcessingGroup("default") class ExplicitDefaultGroup { @EventHandler fun on(event: Happened) {} }
@ProcessingGroup("") class EmptyGroup { @EventHandler fun on(event: Happened) {} }
@ProcessingGroup("-orders") class LeadingDashGroup { @EventHandler fun on(event: Happened) {} }
@ProcessingGroup("a234567890123456789012345678901234567890123456789") class TooLongGroup { @EventHandler fun on(event: Happened) {} }

class ProcessingGroupTest {
    private val registry = HandlerRegistry(MessageTypeResolver("test"))

    @Test fun `handlers without a group are in processor default, a group puts all event handlers of the class in it`() {
        registry.register(UngroupedProjection())
        registry.register(OrdersProjection())
        registry.register(NotifyProjection())
        assertEquals(setOf("default", "notify-2", "orders"), registry.processors())
        assertEquals(listOf("default"), registry.eventHandlers("test.Happened", "default").map { it.processor })
        assertEquals(listOf("orders"), registry.eventHandlers("test.Happened", "orders").map { it.processor })
        assertEquals(3, registry.eventHandlers("test.Happened").size) // every processor's handlers
        assertEquals(setOf("test.Happened"), registry.eventTypes("orders"))
        assertEquals(emptySet(), registry.eventTypes("missing"))
        assertTrue(registry.commandHandler("test.Ping") != null)
    }

    @Test fun `every processor, default included, has the subscription (today's consumer group) and the markers of its name`() {
        assertEquals("svc.default", Processors.subscriptionName("svc", Processors.DEFAULT))
        assertEquals("svc.orders", Processors.subscriptionName("svc", "orders"))
        assertEquals("processor:default:e-1", Processors.markerId(Processors.DEFAULT, "e-1"))
        assertEquals("processor:orders:printer:k-1", Processors.markerId("orders", "printer:k-1"))
    }

    @Test fun `a 48 character name is accepted`() {
        @ProcessingGroup("a23456789012345678901234567890123456789012345678") class MaxLength { @EventHandler fun on(event: Happened) {} }
        registry.register(MaxLength())
        assertTrue("a23456789012345678901234567890123456789012345678" in registry.processors())
    }

    @Test fun `an invalid or explicit default name is a startup error naming the handler`() {
        for (bean in listOf(UpperCaseGroup(), ExplicitDefaultGroup(), EmptyGroup(), LeadingDashGroup(), TooLongGroup())) {
            val e = assertThrows<IllegalStateException> { registry.register(bean) }
            val name = bean::class.simpleName!!
            assertTrue(e.message!!.contains("$name.on"), "${e.message}")
            assertTrue(e.message!!.contains("@ProcessingGroup"), "${e.message}")
        }
        assertEquals(emptySet(), registry.processors())
    }

    @Test fun `invokeEvent with a processor runs only that processor's handlers`() = runBlocking {
        val plain = UngroupedProjection()
        val orders = OrdersProjection()
        registry.register(plain)
        registry.register(orders)
        val invoker = HandlerInvoker(registry, JsonCodec())
        val payload = JsonCodec().encode(Happened("e-1", Instant.EPOCH))
        val ctx = MessageContext("e-1", "test.Happened", "e-1", null, emptySet(), null, emptyMap())
        invoker.invokeEvent("test.Happened", payload, ctx, "orders")
        assertEquals(listOf("orders:e-1"), orders.seen + plain.seen)
        invoker.invokeEvent("test.Happened", payload, ctx, "default")
        assertEquals(listOf("default:e-1"), plain.seen.toList())
        invoker.invokeEvent("test.Happened", payload, ctx)
        assertEquals(listOf("orders:e-1", "orders:e-1"), orders.seen.toList())
    }
}
