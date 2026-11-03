package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import org.slf4j.MDC
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Messages and sagas of the saga runtime tests ("Rt" keeps them apart from the scanner test's). */
@MessageType("rt.OrderPlaced")
data class RtOrderPlaced(val orderId: String) : Event

@MessageType("rt.PaymentTaken")
data class RtPaymentTaken(val paymentRef: String) : Event

@MessageType("rt.Bump")
data class RtBump(val orderId: String, val tag: String) : Event

@MessageType("rt.Forced")
data class RtForced(val orderId: String) : Event

@MessageType("rt.Finish")
data class RtFinish(val orderId: String) : Event

/** Adds (or, with [remove], removes) the association [key] = [value] of the order's saga. */
@MessageType("rt.Assoc")
data class RtAssoc(val orderId: String, val key: String, val value: String, val remove: Boolean = false) : Event

@MessageType("rt.ByRef")
data class RtByRef(val ref: String, val tag: String) : Event

@MessageType("rt.Note")
data class RtNote(val text: String) : Event

@MessageType("rt.DoIt")
data class RtDoIt(val orderId: String) : Command<Unit>

@MessageType("rt.Done")
data class RtDone(val orderId: String) : Event

@MessageType("rt.Nudge")
data class RtNudge(val orderId: String)

class RtBean(val name: String)

/** Test switches the handlers consult: a barrier that makes the first handlers wait for each other, and a failure counter. */
object RtGate {
    @Volatile var barrier: CyclicBarrier? = null
    private val arrivals = AtomicInteger()
    val failFirst = AtomicInteger()

    fun reset() {
        barrier = null
        arrivals.set(0)
        failFirst.set(0)
    }

    /** Waits for the other party, the first [CyclicBarrier.getParties] times only (retries pass through). */
    fun meet() {
        val b = barrier ?: return
        if (arrivals.incrementAndGet() <= b.parties) b.await(10, TimeUnit.SECONDS)
    }

    fun failIfAsked() {
        if (failFirst.getAndUpdate { if (it > 0) it - 1 else 0 } > 0) throw IllegalStateException("handler failed on purpose")
    }
}

@Saga(name = "rt.Order")
class RtOrderSaga {
    var orderId: String? = null
    var placed = 0
    var forced = 0
    var log: MutableList<String> = mutableListOf()
    var sawSagaId: String? = null
    var sawMessageId: String? = null
    var sawBean: String? = null
    var mdcSagaType: String? = null
    var mdcSagaId: String? = null
    var mdcCorrelationId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtOrderPlaced, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager, ctx: MessageContext, bean: RtBean) {
        RtGate.meet()
        orderId = e.orderId
        placed++
        sawSagaId = saga.sagaId
        sawMessageId = ctx.messageId
        sawBean = bean.name
        mdcSagaType = MDC.get("sagaType")
        mdcSagaId = MDC.get("sagaId")
        mdcCorrelationId = MDC.get("correlationId")
        saga.associateWith("paymentRef", "pay-${e.orderId}")
        commands.send(RtDoIt(e.orderId))
        commands.publish(RtDone(e.orderId), "k-${e.orderId}")
        deadlines.schedule(Duration.ofMinutes(10), "timeout", "p-${e.orderId}")
        deadlines.schedule(Instant.parse("2026-11-03T12:00:00Z"), "nudge", RtNudge(e.orderId))
        RtGate.failIfAsked()
    }

    @StartSaga(forceNew = true)
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtForced) {
        forced++
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtBump, commands: SagaCommands) {
        RtGate.meet()
        log += e.tag
        commands.send(RtDoIt("${e.orderId}-${e.tag}"))
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "paymentRef")
    fun on(e: RtPaymentTaken) {
        log += "paid"
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtFinish, saga: SagaLifecycle) {
        saga.end()
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtAssoc, saga: SagaLifecycle) {
        if (e.remove) saga.removeAssociationWith(e.key, e.value) else saga.associateWith(e.key, e.value)
    }

    @SagaEventHandler(associationProperty = "ref")
    fun on(e: RtByRef) {
        log += e.tag
    }
}

/** Throws on every event, as a broken saga type does. */
@Saga(name = "rt.Broken")
class RtBrokenSaga {
    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtOrderPlaced) {
        throw IllegalStateException("broken on ${e.orderId}")
    }
}

/** Associates by the message header `x-order` instead of an event property. */
@Saga(name = "rt.Header")
class RtHeaderSaga {
    var notes = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "", associationHeader = "x-order")
    fun on(e: RtNote) {
        notes++
    }
}
