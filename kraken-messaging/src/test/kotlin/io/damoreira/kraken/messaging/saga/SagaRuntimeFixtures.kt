package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import org.slf4j.MDC
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
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

    /** How often [RtBrokenSaga]'s handler ran. */
    val brokenCalls = AtomicInteger()

    fun reset() {
        barrier = null
        arrivals.set(0)
        failFirst.set(0)
        brokenCalls.set(0)
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

@MessageType("rt.HandedOver")
data class RtHandedOver(val orderId: String) : Event

/** Starts by `orderId` and at once hands its routing over to `paymentRef`: its start handler removes the start association. */
@Saga(name = "rt.Handover")
class RtHandoverSaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtHandedOver, saga: SagaLifecycle) {
        orderId = e.orderId
        saga.associateWith("paymentRef", "pay-${e.orderId}")
        saga.removeAssociationWith("orderId", e.orderId)
    }
}

/** Throws on every event, as a broken saga type does. */
@Saga(name = "rt.Broken")
class RtBrokenSaga {
    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtOrderPlaced) {
        RtGate.brokenCalls.incrementAndGet()
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

/** Two start handlers whose key and value pairs (`k` = `x=1`, `k=x` = `1`) read alike once joined by `=`. */
@Saga(name = "rt.Escape")
class RtEscapeSaga {
    var starts = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "text", keyName = "k")
    fun on(e: RtNote) {
        starts++
    }

    @StartSaga
    @SagaEventHandler(associationProperty = "tag", keyName = "k=x")
    fun on(e: RtBump) {
        starts++
    }
}

/** A clock the test moves: the relay's backoff and the scheduler's due times read it. */
internal class SettableClock(@Volatile var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneId.of("UTC")
    override fun withZone(zone: ZoneId?): Clock = this
    override fun instant(): Instant = now
}


@MessageType("rt.Priced")
data class RtPriced(val orderId: String, val amount: BigDecimal, val tags: Map<String, String>) : Event

@MessageType("rt.PriceCheck")
data class RtPriceCheck(val orderId: String) : Event

@MessageType("rt.ChargeAmount")
data class RtChargeAmount(val amount: BigDecimal, val tags: Map<String, String>) : Command<Unit>

/** Keeps a `BigDecimal` and a map with awkward keys (`a.b`, `$c`) in its state and sends them in commands. */
@Saga(name = "rt.Price")
class RtPriceSaga {
    var orderId: String? = null
    var amount: BigDecimal = BigDecimal.ZERO
    var tags: Map<String, String> = emptyMap()

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtPriced, commands: SagaCommands) {
        orderId = e.orderId
        amount = e.amount
        tags = e.tags
        commands.send(RtChargeAmount(amount, tags))
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: RtPriceCheck, commands: SagaCommands) {
        commands.send(RtChargeAmount(amount, tags))
    }
}

/** The saga state as a BSON document, for assertions (it is stored as JSON text). */
internal val SagaDocument.stateDoc: org.bson.Document get() = org.bson.Document.parse(state)

/** The payload of a dispatch entry as a BSON document, for assertions (it is stored as JSON text). */
internal val SagaDispatchDocument.payloadDoc: org.bson.Document get() = org.bson.Document.parse(payload)

/** The payload of a parked event as a BSON document, for assertions (it is stored as JSON text). */
internal val SagaFailureDocument.payloadDoc: org.bson.Document get() = org.bson.Document.parse(payload)
