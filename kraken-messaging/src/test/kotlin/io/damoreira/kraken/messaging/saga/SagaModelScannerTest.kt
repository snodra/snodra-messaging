package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.reflect.KClass
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

@MessageType("test.OrderPlaced")
data class OrderPlaced(val orderId: String, val amount: Int = 0) : Event

@MessageType("test.ChargeSucceeded")
data class ChargeSucceeded(val paymentRef: String) : Event

@MessageType("test.RequestCharge")
data class RequestCharge(val paymentRef: String) : Command<Unit>

/** A supertype a handler may be declared on, narrowed by `payloadType`. */
interface OrderEvent : Event {
    val orderId: String
}

@MessageType("test.OrderShipped")
data class OrderShipped(override val orderId: String) : OrderEvent

class BillingBean

@Saga
class PaymentSaga {
    var orderId: String? = null
    var timeoutDeadline: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: OrderPlaced, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager, ctx: MessageContext, billing: BillingBean) {
        orderId = e.orderId
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "paymentRef", keyName = "payment")
    fun on(e: ChargeSucceeded) = Unit

    @DeadlineHandler("charge-timeout")
    fun timeout(payload: String, saga: SagaLifecycle) = Unit

    @EndSaga
    @DeadlineHandler("give-up")
    fun giveUp() = Unit
}

@Saga(name = "billing.Payment")
class NamedSaga {
    @StartSaga @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
}

@Saga
class HeaderSaga {
    @StartSaga @SagaEventHandler(associationProperty = "", associationHeader = "x-order") fun on(e: OrderPlaced) = Unit
}

@Saga
class NarrowedSaga {
    @StartSaga @SagaEventHandler(associationProperty = "orderId", payloadType = OrderShipped::class) fun on(e: OrderEvent) = Unit
}

@Saga
class NoStartSaga {
    @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
}

@Saga
class BadPropertySaga {
    @StartSaga @SagaEventHandler(associationProperty = "nope") fun on(e: OrderPlaced) = Unit
}

@Saga
class SuspendSaga {
    @StartSaga @SagaEventHandler(associationProperty = "orderId") suspend fun on(e: OrderPlaced) = Unit
}

@Saga
class NotAnEventSaga {
    @StartSaga @SagaEventHandler(associationProperty = "length") fun on(e: String) = Unit
}

@Saga
class BadDeadlineParamSaga {
    @StartSaga @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
    @DeadlineHandler("t") fun timeout(deadlines: DeadlineManager, attempts: Int) = Unit
}

@Saga
class NoDefaultConstructorSaga(val orderId: String) {
    @StartSaga @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
}

@Saga
class NotSerialisableSaga {
    /** Jackson cannot read an interface-typed field back. */
    var callback: Runnable? = Runnable { }

    @StartSaga @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
}

@Saga
class StrayEndSaga {
    @StartSaga @SagaEventHandler(associationProperty = "orderId") fun on(e: OrderPlaced) = Unit
    @EndSaga fun finish() = Unit
}

class SagaModelScannerTest {
    private val scanner = SagaModelScanner(MessageTypeResolver("test"), JsonCodec())

    private fun scan(vararg classes: KClass<*>) = scanner.scan(classes.toList())

    private fun failure(klass: KClass<*>): IllegalStateException = assertFailsWith<IllegalStateException> { scan(klass) }

    @Test
    fun `a saga with a start handler and an association property is discovered with its type name`() {
        val type = scan(PaymentSaga::class).single()

        assertEquals(PaymentSaga::class, type.klass)
        assertEquals(setOf("test.OrderPlaced", "test.ChargeSucceeded"), type.eventHandlers.keys)
        val start = type.eventHandlers.getValue("test.OrderPlaced").single()
        assertTrue(start.start)
        assertEquals(false, start.forceNew)
        assertEquals(false, start.end)
        assertEquals(OrderPlaced::class, start.eventClass)
        assertEquals("test.OrderPlaced", start.eventType)
        assertEquals("orderId", start.associationProperty)
        assertNull(start.associationHeader)
        assertEquals(
            listOf(ParamKind.Event, ParamKind.Lifecycle, ParamKind.Commands, ParamKind.Deadlines, ParamKind.Context, ParamKind.Bean(BillingBean::class)),
            start.params,
        )
        val end = type.eventHandlers.getValue("test.ChargeSucceeded").single()
        assertTrue(end.end)
        assertEquals(false, end.start)
        assertEquals(listOf(ParamKind.Event), end.params)

        assertEquals(setOf("charge-timeout", "give-up"), type.deadlineHandlers.keys)
        val timeout = type.deadlineHandlers.getValue("charge-timeout")
        assertEquals(listOf(ParamKind.DeadlinePayload, ParamKind.Lifecycle), timeout.params)
        assertEquals(String::class, timeout.payloadClass)
        assertEquals(false, timeout.end)
        val giveUp = type.deadlineHandlers.getValue("give-up")
        assertTrue(giveUp.end)
        assertEquals(emptyList(), giveUp.params)
        assertNull(giveUp.payloadClass)
    }

    @Test
    fun `the saga name defaults to the class FQCN and @Saga(name) overrides it`() {
        assertEquals(PaymentSaga::class.java.name, scan(PaymentSaga::class).single().name)
        assertEquals("billing.Payment", scan(NamedSaga::class).single().name)
        assertEquals(PaymentSaga::class.java.name, scan(PaymentSaga::class, NamedSaga::class).first { it.klass == PaymentSaga::class }.name)
    }

    @Test
    fun `keyName defaults to the association property`() {
        val type = scan(PaymentSaga::class).single()

        assertEquals("orderId", type.eventHandlers.getValue("test.OrderPlaced").single().keyName)
        assertEquals("payment", type.eventHandlers.getValue("test.ChargeSucceeded").single().keyName)
        val value = type.eventHandlers.getValue("test.OrderPlaced").single().associationValue(OrderPlaced("o-1"), ctx())
        assertEquals("o-1", value)
    }

    @Test
    fun `associationHeader reads the value from the message header`() {
        val handler = scan(HeaderSaga::class).single().eventHandlers.getValue("test.OrderPlaced").single()

        assertNull(handler.associationProperty)
        assertEquals("x-order", handler.associationHeader)
        assertEquals("x-order", handler.keyName)
        assertEquals("h-7", handler.associationValue(OrderPlaced("o-1"), ctx(mapOf("x-order" to "h-7"))))
        assertNull(handler.associationValue(OrderPlaced("o-1"), ctx()))
    }

    @Test
    fun `payloadType narrows a handler declared on a supertype`() {
        val type = scan(NarrowedSaga::class).single()

        val handler = type.eventHandlers.getValue("test.OrderShipped").single()
        assertEquals(OrderShipped::class, handler.eventClass)
        assertEquals("o-9", handler.associationValue(OrderShipped("o-9"), ctx()))
        assertNull(type.eventHandlers["test.Event"])
    }

    @Test
    fun `a saga without a @StartSaga handler`() {
        val e = failure(NoStartSaga::class)
        assertTrue("NoStartSaga" in e.message!! && "@StartSaga" in e.message!!, e.message)
    }

    @Test
    fun `an association property the event does not have`() {
        val e = failure(BadPropertySaga::class)
        assertTrue("BadPropertySaga.on" in e.message!! && "nope" in e.message!! && "OrderPlaced" in e.message!!, e.message)
    }

    @Test
    fun `a suspend saga handler`() {
        val e = failure(SuspendSaga::class)
        assertTrue("SuspendSaga.on" in e.message!! && "suspend" in e.message!!, e.message)
    }

    @Test
    fun `a saga event handler whose first parameter is not an Event`() {
        val e = failure(NotAnEventSaga::class)
        assertTrue("NotAnEventSaga.on" in e.message!! && "Event" in e.message!!, e.message)
    }

    @Test
    fun `a deadline handler with an unresolvable parameter`() {
        val e = failure(BadDeadlineParamSaga::class)
        assertTrue("BadDeadlineParamSaga.timeout" in e.message!! && "attempts" in e.message!!, e.message)
    }

    @Test
    fun `a saga class without a no-argument constructor`() {
        val e = failure(NoDefaultConstructorSaga::class)
        assertTrue("NoDefaultConstructorSaga" in e.message!! && "no-argument constructor" in e.message!!, e.message)
    }

    @Test
    fun `a saga whose fresh instance does not round-trip through the library's JSON codec`() {
        val e = failure(NotSerialisableSaga::class)
        assertTrue("NotSerialisableSaga" in e.message!! && "JSON" in e.message!!, e.message)
    }

    @Test
    fun `@EndSaga on a method that is not a saga event or deadline handler`() {
        val e = failure(StrayEndSaga::class)
        assertTrue("StrayEndSaga.finish" in e.message!! && "@EndSaga" in e.message!!, e.message)
    }

    private fun ctx(headers: Map<String, String> = emptyMap()) =
        MessageContext("m-1", "test.OrderPlaced", "c-1", null, emptySet(), null, headers)
}
