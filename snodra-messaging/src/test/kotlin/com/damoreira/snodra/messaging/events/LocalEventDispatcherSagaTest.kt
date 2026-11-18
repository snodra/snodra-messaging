package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.sagas
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.saga.RtBean
import com.damoreira.snodra.messaging.saga.RtOrderPlaced
import com.damoreira.snodra.messaging.saga.RtOrderSaga
import com.damoreira.snodra.messaging.saga.SagaModelScanner
import com.damoreira.snodra.messaging.saga.SagaRetry
import com.damoreira.snodra.messaging.saga.SagaRuntime
import com.damoreira.snodra.messaging.saga.stateDoc
import org.junit.jupiter.api.Test
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** An ordinary handler of the saga's start event that always fails, as one with a bug or a dependency outage does. */
class FailingOrderHandler {
    val calls = AtomicInteger()

    @EventHandler
    fun on(e: RtOrderPlaced) {
        calls.incrementAndGet()
        throw IllegalStateException("ordinary handler is broken on ${e.orderId}")
    }
}

/** The dispatcher and the saga runtime over the in-memory message store (no container). */
class LocalEventDispatcherSagaTest {
    private val store = MemoryMessageStore()
    private val tx = TransactionTemplate(store.transactions)
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("rt")

    @Test
    fun `a throwing ordinary handler does not stop a saga from applying the event exactly once across redeliveries`() {
        val handler = FailingOrderHandler()
        val registry = HandlerRegistry(resolver).also { it.register(handler) }
        val runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(RtOrderSaga::class)), ports = store.sagas, tx = tx, codec = codec, typeResolver = resolver,
            beans = { RtBean("bean") }, retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), store, runtime)
        val type = resolver.typeNameOf(RtOrderPlaced::class)
        val ctx = MessageContext("e-1", type, "corr-1", null, emptySet(), null, mapOf("id" to "e-1", "type" to type))

        repeat(3) { assertFailsWith<IllegalStateException> { dispatcher.dispatch("e-1", type, codec.encode(RtOrderPlaced("o1")), ctx) } }

        assertEquals(3, handler.calls.get(), "stage 1 ran on every delivery and failed")
        val saga = store.sagas().single()
        assertEquals(1, saga.stateDoc.getInteger("placed"), "the saga applied the event once, however often the record came back")
    }
}
