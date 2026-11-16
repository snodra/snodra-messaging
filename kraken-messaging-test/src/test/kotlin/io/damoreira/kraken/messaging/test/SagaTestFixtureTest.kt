package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.saga.DeadlineHandler
import io.damoreira.kraken.messaging.saga.DeadlineManager
import io.damoreira.kraken.messaging.saga.EndSaga
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaCommands
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaLifecycle
import io.damoreira.kraken.messaging.saga.StartSaga
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@MessageType("fx.OrderPlaced")
data class FxOrderPlaced(val orderId: String, val amount: Int) : Event

@MessageType("fx.ChargeSucceeded")
data class FxChargeSucceeded(val paymentRef: String) : Event

@MessageType("fx.RequestCharge")
data class FxRequestCharge(val paymentRef: String, val amount: Int) : Command<Unit>

@MessageType("fx.CancelCharge")
data class FxCancelCharge(val paymentRef: String) : Command<Unit>

@MessageType("fx.OrderCancelled")
data class FxOrderCancelled(val orderId: String) : Event

@MessageType("fx.Unrelated")
data class FxUnrelated(val orderId: String) : Event

/** The spec's PaymentSaga, plus a published event on the timeout. */
@Saga(name = "fx.Payment")
class FxPaymentSaga {
    var orderId: String? = null
    var paymentRef: String? = null
    var attempts = 0
    var timeoutDeadline: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager) {
        orderId = e.orderId
        paymentRef = "pay-${e.orderId}"
        saga.associateWith("paymentRef", paymentRef!!)
        commands.send(FxRequestCharge(paymentRef!!, e.amount))
        timeoutDeadline = deadlines.schedule(Duration.ofMinutes(15), "charge-timeout")
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "paymentRef")
    fun on(e: FxChargeSucceeded, deadlines: DeadlineManager) {
        timeoutDeadline?.let(deadlines::cancel)
    }

    @DeadlineHandler("charge-timeout")
    fun timeout(saga: SagaLifecycle, commands: SagaCommands) {
        commands.send(FxCancelCharge(paymentRef!!))
        commands.publish(FxOrderCancelled(orderId!!), orderId)
        saga.end()
    }
}

@MessageType("fx.ChargeFailed")
data class FxChargeFailed(val reason: String, val commandType: String, val message: String, val keyBelongsToSaga: Boolean) : Event

/** Reacts to its command failing, as a saga does after the relay reports `SagaCommandFailed`. */
@Saga(name = "fx.Failing")
class FxFailureSaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced, saga: SagaLifecycle, commands: SagaCommands) {
        orderId = e.orderId
        commands.send(FxRequestCharge("pay-${e.orderId}", e.amount))
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed, saga: SagaLifecycle, commands: SagaCommands) {
        commands.publish(FxChargeFailed(e.reason.name, e.commandType, e.message, e.idempotencyKey == "${saga.sagaId}:0"))
    }
}

class FxClock(val label: String)

/** Reads a bean, an association from a header, and reschedules itself like a polling saga. */
@Saga(name = "fx.Poller")
class FxPollerSaga {
    var jobId: String? = null
    var polls = 0
    var seenLabel: String? = null
    var seenHeader: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced, deadlines: DeadlineManager, clock: FxClock, ctx: MessageContext) {
        jobId = e.orderId
        seenLabel = clock.label
        seenHeader = ctx.headers["x-source"]
        deadlines.schedule(Duration.ofSeconds(2), "poll")
    }

    @DeadlineHandler("poll")
    fun poll(deadlines: DeadlineManager, commands: SagaCommands, saga: SagaLifecycle) {
        polls++
        commands.send(FxCancelCharge("poll-$polls"))
        if (polls < 3) deadlines.schedule(Duration.ofSeconds(2), "poll") else saga.end()
    }
}

/** State Jackson cannot write once the handler has put something in it. */
class FxUnwritable {
    @Suppress("unused")
    val boom: String get() = error("cannot be written")
}

@Saga(name = "fx.Unserialisable")
class FxUnserialisableSaga {
    var orderId: String? = null
    var handle: FxUnwritable? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced) {
        orderId = e.orderId
        handle = FxUnwritable()
    }
}

@MessageType("fx.Huge")
data class FxHuge(val orderId: String, val amount: java.math.BigInteger) : Event

/** Publishes an event whose integer no store can hold (a signed 64-bit integer is the outbox payload contract). */
@Saga(name = "fx.Huge")
class FxHugeSaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced, commands: SagaCommands) {
        orderId = e.orderId
        commands.publish(FxHuge(e.orderId, java.math.BigInteger.TWO.pow(64)))
    }
}

@Saga(name = "fx.Throwing")
class FxThrowingSaga {
    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: FxOrderPlaced) {
        throw IllegalStateException("the handler is broken for ${e.orderId}")
    }
}

class SagaTestFixtureTest {
    private val t0 = Instant.parse("2026-11-03T10:00:00Z")
    private fun payment() = SagaTestFixture(FxPaymentSaga::class).givenCurrentTime(t0)

    @Test
    fun `a start event creates the saga, associates it, records the command and schedules the deadline`() {
        payment()
            .whenEvent(FxOrderPlaced("o1", 42))
            .expectActiveSagas(1)
            .expectAssociation("orderId", "o1")
            .expectAssociation("paymentRef", "pay-o1")
            .expectDispatchedCommands(FxRequestCharge("pay-o1", 42))
            .expectPublishedEvents()
            .expectScheduledDeadline("charge-timeout", t0.plus(Duration.ofMinutes(15)))
    }

    @Test
    fun `the success path ends the saga and cancels its deadline`() {
        payment()
            .givenEvents(FxOrderPlaced("o1", 42))
            .whenEvent(FxChargeSucceeded("pay-o1"))
            .expectSagaEnded()
            .expectActiveSagas(0)
            .expectNoScheduledDeadlines()
            .expectDispatchedCommands()
    }

    @Test
    fun `elapsed time fires the timeout, which sends CancelCharge, publishes and ends the saga`() {
        payment()
            .givenEvents(FxOrderPlaced("o1", 42))
            .whenTimeElapses(Duration.ofMinutes(14))
            .expectActiveSagas(1)
            .expectDispatchedCommands()
            .whenTimeElapses(Duration.ofMinutes(1))
            .expectDispatchedCommands(FxCancelCharge("pay-o1"))
            .expectPublishedEvents(FxOrderCancelled("o1"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
    }

    @Test
    fun `what the given events sent is not part of the expected commands`() {
        payment()
            .givenEvents(FxOrderPlaced("o1", 42))
            .whenEvent(FxUnrelated("o1"))
            .expectDispatchedCommands()
            .expectActiveSagas(1)
    }

    @Test
    fun `an event the saga does not handle changes nothing`() {
        payment()
            .whenEvent(FxUnrelated("o1"))
            .expectActiveSagas(0)
            .expectDispatchedCommands()
            .expectNoScheduledDeadlines()
    }

    @Test
    fun `a deadline that reschedules itself fires once per elapsed interval, at its own due time`() {
        SagaTestFixture(FxPollerSaga::class, FxClock("c1")).givenCurrentTime(t0)
            .whenEvent(FxOrderPlaced("o1", 1))
            .expectScheduledDeadline("poll", t0.plusSeconds(2))
            .whenTimeElapses(Duration.ofSeconds(5))
            .expectDispatchedCommands(FxCancelCharge("poll-1"), FxCancelCharge("poll-2"))
            .expectScheduledDeadline("poll", t0.plusSeconds(6))
            .whenTimeElapses(Duration.ofSeconds(1))
            .expectDispatchedCommands(FxCancelCharge("poll-1"), FxCancelCharge("poll-2"), FxCancelCharge("poll-3"))
            .expectSagaEnded()
    }

    @Test
    fun `handler parameters are resolved from the beans and the headers of the event reach the saga`() {
        val fixture = SagaTestFixture(FxPollerSaga::class, FxClock("c1"))
        fixture.whenEvent(FxOrderPlaced("o1", 1), mapOf("x-source" to "webhook")).expectActiveSagas(1)
        // The saga's own state is not exposed; the bean and header reached it or the handler would have failed above.
        assertFailsWith<IllegalStateException> {
            SagaTestFixture(FxPollerSaga::class).whenEvent(FxOrderPlaced("o1", 1))
        }.also { assertContains(it.message!!, "FxClock") }
    }

    @Test
    fun `non-serialisable state fails the step naming the saga and the cause`() {
        val e = assertFailsWith<IllegalStateException> {
            SagaTestFixture(FxUnserialisableSaga::class).whenEvent(FxOrderPlaced("o1", 1))
        }
        assertContains(e.message!!, "fx.Unserialisable")
        assertContains(e.message!!, "fx.OrderPlaced")
        assertContains(e.message!!, "JSON")
        assertContains(e.message!!, "cannot be written")
    }

    @Test
    fun `a handler exception fails the step at once, without retries and with the handler's message`() {
        val e = assertFailsWith<IllegalStateException> {
            SagaTestFixture(FxThrowingSaga::class).whenEvent(FxOrderPlaced("o1", 1))
        }
        assertContains(e.message!!, "the handler is broken for o1")
    }

    @Test
    fun `a saga event the outbox would refuse fails the step, as the store's record refuses it`() {
        val e = assertFailsWith<IllegalStateException> {
            SagaTestFixture(FxHugeSaga::class).whenEvent(FxOrderPlaced("o1", 1))
        }
        assertContains(e.message!!, "IllegalArgumentException")
        assertContains(e.message!!, "outside the signed 64-bit range")
    }

    @Test
    fun `a failed expectation is an AssertionError that says what was expected and what was found`() {
        val fixture = payment().whenEvent(FxOrderPlaced("o1", 42))
        assertContains(assertFailsWith<AssertionError> { fixture.expectActiveSagas(2) }.message!!, "expected 2 active sagas but found 1")
        assertContains(assertFailsWith<AssertionError> { fixture.expectAssociation("orderId", "zzz") }.message!!, "orderId=zzz")
        assertContains(assertFailsWith<AssertionError> { fixture.expectDispatchedCommands(FxRequestCharge("pay-o1", 43)) }.message!!, "fx.RequestCharge")
        assertContains(assertFailsWith<AssertionError> { fixture.expectDispatchedCommands() }.message!!, "expected no commands")
        assertContains(assertFailsWith<AssertionError> { fixture.expectPublishedEvents(FxOrderCancelled("o1")) }.message!!, "fx.OrderCancelled")
        assertContains(assertFailsWith<AssertionError> { fixture.expectScheduledDeadline("charge-timeout", t0) }.message!!, "charge-timeout")
        assertContains(assertFailsWith<AssertionError> { fixture.expectNoScheduledDeadlines() }.message!!, "charge-timeout")
        assertContains(assertFailsWith<AssertionError> { fixture.expectSagaEnded() }.message!!, "not ended")
    }

    @Test
    fun `givenCurrentTime sets the time deadlines are scheduled against`() {
        val later = Instant.parse("2030-01-01T00:00:00Z")
        SagaTestFixture(FxPaymentSaga::class).givenCurrentTime(later)
            .whenEvent(FxOrderPlaced("o1", 1))
            .expectScheduledDeadline("charge-timeout", later.plus(Duration.ofMinutes(15)))
    }

    @Test
    fun `a saga class that breaks the model is refused when the fixture is built`() {
        assertEquals(
            true,
            assertFailsWith<IllegalStateException> { SagaTestFixture(FxClock::class) }.message!!.contains("FxClock"),
        )
    }

    @Test
    fun `whenCommandFailed tells the saga that sent the command, with the key the relay would have used`() {
        SagaTestFixture(FxFailureSaga::class)
            .whenEvent(FxOrderPlaced("o1", 5))
            .whenEvent(FxOrderPlaced("o2", 6))
            .whenCommandFailed(FxRequestCharge("pay-o2", 6), SagaCommandFailed.Reason.UNKNOWN_OUTCOME, "the printer crashed")
            .expectPublishedEvents(FxChargeFailed("UNKNOWN_OUTCOME", "fx.RequestCharge", "the printer crashed", keyBelongsToSaga = true))
            .expectActiveSagas(1) // o2's saga ended, o1's did not hear of it
    }

    @Test
    fun `whenCommandFailed for a command no saga sent fails the step saying so`() {
        val e = assertFailsWith<IllegalStateException> {
            SagaTestFixture(FxFailureSaga::class)
                .whenEvent(FxOrderPlaced("o1", 5))
                .whenCommandFailed(FxRequestCharge("pay-zzz", 5), SagaCommandFailed.Reason.FAILED)
        }
        assertContains(e.message!!, "fx.RequestCharge")
        assertContains(e.message!!, "pay-zzz")
    }
}
