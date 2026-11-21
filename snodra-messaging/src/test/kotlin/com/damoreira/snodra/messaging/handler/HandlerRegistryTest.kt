package com.damoreira.snodra.messaging.handler

import com.damoreira.snodra.messaging.DuplicateHandlerException
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.ExportRows
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.Pong
import com.damoreira.snodra.messaging.fixtures.Row
import com.damoreira.snodra.messaging.fixtures.Balance
import com.damoreira.snodra.messaging.fixtures.BalanceChanged
import com.damoreira.snodra.messaging.fixtures.WatchAccount
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class AllKindsHandlers {
    @EventHandler fun on(event: Happened) {}
    @CommandHandler suspend fun handle(command: Ping, ctx: MessageContext): Pong = Pong(ctx.userId!!, Instant.EPOCH)
    @QueryHandler fun handle(query: GetCount): Count = Count(7)
}

class DuplicateCommandHandlers {
    @CommandHandler fun a(command: Ping): Pong = Pong("a", Instant.EPOCH)
    @CommandHandler fun b(command: Ping): Pong = Pong("b", Instant.EPOCH)
}

class TwoEventHandlers {
    @EventHandler fun first(event: Happened) {}
    @EventHandler fun second(event: Happened) {}
}

class WrongSecondParameter {
    @CommandHandler fun handle(command: Ping, other: String): Pong = Pong("x", Instant.EPOCH)
}

class ZeroParameters {
    @EventHandler fun on() {}
}

class Throwing {
    @CommandHandler fun handle(command: Ping): Pong = throw IllegalStateException("boom")
}

class AuthOnEvent {
    @EventHandler @RequiresAuthentication fun on(event: Happened) {}
}

class AuthOnCommand {
    @CommandHandler @RequiresAuthentication("hasRole('snodra-admin')") fun handle(command: Ping): Pong = Pong("x", Instant.EPOCH)
    @QueryHandler fun handle(query: GetCount): Count = Count(1)
}

class SharedCountHandlers {
    @QueryHandler(shared = true) fun handle(query: GetCount): Count = Count(3)
}

class ExclusiveCountHandlers {
    @QueryHandler fun handle(query: GetCount): Count = Count(4)
}

class BothWaysCountHandlers {
    @QueryHandler fun exclusive(query: GetCount): Count = Count(5)
    @QueryHandler(shared = true) fun shared(query: GetCount): Count = Count(6)
}

class StreamingRowsHandlers {
    @QueryHandler fun handle(query: ExportRows): Flow<Row> = flow { repeat(query.count) { emit(Row(it + 1)) } }
}

class SharedStreamingRowsHandlers {
    @QueryHandler(shared = true) suspend fun handle(query: ExportRows, ctx: MessageContext): Flow<Row> = flow { emit(Row(0)) }
}

class StreamingWithoutFlow {
    @QueryHandler fun handle(query: ExportRows): List<Row> = emptyList()
}

class StreamingAsCommand {
    @CommandHandler fun handle(query: ExportRows): Flow<Row> = flow { }
}

class FlowFromPlainQuery {
    @QueryHandler fun handle(query: GetCount): Flow<Count> = flow { emit(Count(1)) }
}

class AccountWatchers {
    @QueryHandler fun handle(query: WatchAccount): Balance = Balance(query.account * 10)
}

class SharedAccountWatchers {
    @QueryHandler(shared = true) suspend fun handle(query: WatchAccount, ctx: MessageContext): Balance = Balance(1)
}

class SubscriptionReturningFlow {
    @QueryHandler fun handle(query: WatchAccount): Flow<BalanceChanged> = flow { }
}

class SubscriptionAsCommand {
    @CommandHandler fun handle(query: WatchAccount): Balance = Balance(0)
}

class ResettingProjection {
    val calls = mutableListOf<String>()
    @EventHandler fun on(event: Happened) {}
    @ResetHandler fun clear() { calls += "clear" }
    @ResetHandler suspend fun note(ctx: com.damoreira.snodra.messaging.control.ResetContext) { calls += "note:${ctx.operationId}" }
}

@ProcessingGroup("audit")
class ResettingAudit {
    @ResetHandler fun clear() {}
}

class ResetWithWrongParameter {
    @ResetHandler fun clear(event: Happened) {}
}

class HandlerRegistryTest {
    private val registry = HandlerRegistry(MessageTypeResolver("test"))
    private val ctx = MessageContext("m1", "test.Ping", "c1", "alice", setOf("USER"), null, emptyMap())

    @Test fun `registers event command and query handlers by parameter type`() {
        val added = registry.register(AllKindsHandlers())
        assertEquals(3, added.size)
        assertEquals(setOf("test.Happened"), registry.eventTypes())
        assertEquals(setOf("test.Ping"), registry.commandTypes())
        assertEquals(setOf("test.GetCount"), registry.queryTypes())
        val command = assertNotNull(registry.commandHandler("test.Ping"))
        assertEquals(HandlerKind.COMMAND, command.kind)
        assertEquals(Ping::class, command.messageClass)
        assertTrue(command.wantsContext)
        assertEquals(1, registry.eventHandlers("test.Happened").size)
        assertEquals(false, assertNotNull(registry.queryHandler("test.GetCount")).wantsContext)
        assertNull(registry.commandHandler("test.Nope"))
    }

    @Test fun `duplicate command handler fails`() {
        val ex = assertThrows<DuplicateHandlerException> { registry.register(DuplicateCommandHandlers()) }
        assertTrue(ex.message!!.contains("DuplicateCommandHandlers.a"))
        assertTrue(ex.message!!.contains("DuplicateCommandHandlers.b"))
    }

    @Test fun `two event handlers for one type are allowed`() {
        registry.register(TwoEventHandlers())
        assertEquals(listOf("first", "second"), registry.eventHandlers("test.Happened").map { it.function.name })
    }

    @Test fun `wrong second parameter fails with method name in message`() {
        val ex = assertThrows<IllegalStateException> { registry.register(WrongSecondParameter()) }
        assertEquals("Handler WrongSecondParameter.handle must take (message) or (message, MessageContext)", ex.message)
    }

    @Test fun `zero parameter handler fails`() {
        val ex = assertThrows<IllegalStateException> { registry.register(ZeroParameters()) }
        assertEquals("Handler ZeroParameters.on must take (message) or (message, MessageContext)", ex.message)
    }

    @Test fun `invoke passes context when wanted and supports suspend`() = runBlocking {
        registry.register(AllKindsHandlers())
        val pong = registry.commandHandler("test.Ping")!!.invoke(Ping("hi"), ctx) as Pong
        assertEquals("alice", pong.echoed)
        assertEquals(Count(7), registry.queryHandler("test.GetCount")!!.invoke(GetCount("k"), ctx))
        assertSame(Unit, registry.eventHandlers("test.Happened").single().invoke(Happened("1", Instant.EPOCH), ctx))
    }

    @Test fun `invoke unwraps handler exception`() {
        registry.register(Throwing())
        val ex = assertThrows<IllegalStateException> {
            runBlocking { registry.commandHandler("test.Ping")!!.invoke(Ping("x"), ctx) }
        }
        assertEquals("boom", ex.message)
    }

    @Test fun `onChange fires after register`() {
        var fired = 0
        registry.onChange { fired++ }
        registry.register(AllKindsHandlers())
        assertEquals(1, fired)
        registry.register(Any())
        assertEquals(1, fired)
    }

    @Test fun `requires authentication on event handler fails registration`() {
        val e = assertThrows<IllegalStateException> { HandlerRegistry(MessageTypeResolver("test")).register(AuthOnEvent()) }
        assertTrue(e.message!!.contains("AuthOnEvent.on"), e.message)
        assertTrue(e.message!!.contains("@RequiresAuthentication is not supported on @EventHandler"), e.message)
    }

    @Test fun `handler method carries compiled authorization`() {
        val r = HandlerRegistry(MessageTypeResolver("test"))
        r.register(AuthOnCommand())
        assertEquals("hasRole('snodra-admin')", assertNotNull(r.commandHandler("test.Ping")).authorization?.source)
        assertNull(assertNotNull(r.queryHandler("test.GetCount")).authorization)
    }

    // --- shared query types ---

    @Test fun `a shared query handler is listed as a shared query type`() {
        registry.register(SharedCountHandlers())
        assertEquals(setOf("test.GetCount"), registry.queryTypes())
        assertEquals(setOf("test.GetCount"), registry.sharedQueryTypes())
        assertEquals(emptySet(), registry.exclusiveQueryTypes())
        assertTrue(assertNotNull(registry.queryHandler("test.GetCount")).shared)
    }

    @Test fun `an exclusive query handler is not shared`() {
        registry.register(ExclusiveCountHandlers())
        assertEquals(setOf("test.GetCount"), registry.exclusiveQueryTypes())
        assertEquals(emptySet(), registry.sharedQueryTypes())
        assertEquals(false, assertNotNull(registry.queryHandler("test.GetCount")).shared)
    }

    @Test fun `one type registered exclusive and shared by one service fails at startup`() {
        registry.register(ExclusiveCountHandlers())
        val e = assertThrows<IllegalStateException> { registry.register(SharedCountHandlers()) }
        assertTrue(e.message!!.contains("test.GetCount"), e.message)
        assertTrue(e.message!!.contains("ExclusiveCountHandlers.handle"), e.message)
        assertTrue(e.message!!.contains("SharedCountHandlers.handle"), e.message)
        assertTrue(e.message!!.contains("both exclusive and shared"), e.message)

        val within = assertThrows<IllegalStateException> { HandlerRegistry(MessageTypeResolver("test")).register(BothWaysCountHandlers()) }
        assertTrue(within.message!!.contains("both exclusive and shared"), within.message)
    }

    // --- streaming queries ---

    @Test fun `a QueryHandler taking a StreamingQuery and returning a Flow is a streaming query handler`() = runBlocking {
        registry.register(StreamingRowsHandlers())
        val handler = assertNotNull(registry.queryHandler("test.ExportRows"))
        assertTrue(handler.streaming)
        assertEquals(false, handler.shared)
        assertEquals(setOf("test.ExportRows"), registry.exclusiveQueryTypes())
        assertEquals(setOf("test.ExportRows"), registry.queryTypes())
        @Suppress("UNCHECKED_CAST")
        val rows = (handler.invoke(ExportRows(3), ctx) as Flow<Row>).toList()
        assertEquals(listOf(Row(1), Row(2), Row(3)), rows)
        assertEquals(false, assertNotNull(HandlerRegistry(MessageTypeResolver("test")).apply { register(AllKindsHandlers()) }.queryHandler("test.GetCount")).streaming)
    }

    @Test fun `a shared streaming query handler is announced as a shared query type`() {
        registry.register(SharedStreamingRowsHandlers())
        val handler = assertNotNull(registry.queryHandler("test.ExportRows"))
        assertTrue(handler.streaming && handler.shared && handler.wantsContext)
        assertEquals(setOf("test.ExportRows"), registry.sharedQueryTypes())
        assertEquals(emptySet(), registry.exclusiveQueryTypes())
    }

    @Test fun `a streaming query handler must return a Flow`() {
        val e = assertThrows<IllegalStateException> { registry.register(StreamingWithoutFlow()) }
        assertEquals("Handler StreamingWithoutFlow.handle takes a StreamingQuery and must return kotlinx.coroutines.flow.Flow<R>", e.message)
    }

    @Test fun `a query handler returning a Flow must take a StreamingQuery`() {
        val e = assertThrows<IllegalStateException> { registry.register(FlowFromPlainQuery()) }
        assertEquals("Handler FlowFromPlainQuery.handle returns a Flow: its query must implement StreamingQuery<R>", e.message)
    }

    @Test fun `a command handler cannot take a StreamingQuery`() {
        val e = assertThrows<IllegalStateException> { registry.register(StreamingAsCommand()) }
        assertEquals("Handler StreamingAsCommand.handle takes a StreamingQuery: use @QueryHandler", e.message)
    }

    // --- subscription queries ---

    @Test fun `a QueryHandler taking a SubscriptionQuery is a subscription query handler answering the initial result`() = runBlocking {
        registry.register(AccountWatchers())
        val handler = assertNotNull(registry.queryHandler("test.WatchAccount"))
        assertTrue(handler.subscription)
        assertEquals(false, handler.streaming)
        assertEquals(false, handler.shared)
        assertEquals(setOf("test.WatchAccount"), registry.exclusiveQueryTypes())
        assertEquals(setOf("test.WatchAccount"), registry.queryTypes())
        assertEquals(Balance(70), handler.invoke(WatchAccount(7), ctx))
        assertEquals(false, assertNotNull(HandlerRegistry(MessageTypeResolver("test")).apply { register(AllKindsHandlers()) }.queryHandler("test.GetCount")).subscription)
    }

    @Test fun `a shared subscription query handler is announced as a shared query type`() {
        registry.register(SharedAccountWatchers())
        val handler = assertNotNull(registry.queryHandler("test.WatchAccount"))
        assertTrue(handler.subscription && handler.shared && handler.wantsContext)
        assertEquals(setOf("test.WatchAccount"), registry.sharedQueryTypes())
        assertEquals(emptySet(), registry.exclusiveQueryTypes())
    }

    @Test fun `a subscription query handler must not return a Flow`() {
        val e = assertThrows<IllegalStateException> { registry.register(SubscriptionReturningFlow()) }
        assertEquals("Handler SubscriptionReturningFlow.handle takes a SubscriptionQuery and must return its initial result I, not a Flow (updates go through QueryUpdateEmitter)", e.message)
    }

    @Test fun `a command handler cannot take a SubscriptionQuery`() {
        val e = assertThrows<IllegalStateException> { registry.register(SubscriptionAsCommand()) }
        assertEquals("Handler SubscriptionAsCommand.handle takes a SubscriptionQuery: use @QueryHandler", e.message)
    }

    @Test fun `reset handlers are kept per processor in registration order and take no parameter or a ResetContext`() = runBlocking {
        val registry = HandlerRegistry(MessageTypeResolver("test"))
        val projection = ResettingProjection()
        registry.register(projection)
        registry.register(ResettingAudit())
        assertEquals(listOf("ResettingProjection.clear", "ResettingProjection.note"), registry.resetHandlers("default").map { it.label })
        assertEquals(listOf("audit"), registry.resetHandlers("audit").map { it.processor })
        val ctx = com.damoreira.snodra.messaging.control.ResetContext("default", "op-1", com.damoreira.snodra.messaging.eventstore.ResetTarget.Earliest, emptyList())
        registry.resetHandlers("default").forEach { it.invoke(ctx) }
        assertEquals(listOf("clear", "note:op-1"), projection.calls)
        val e = assertThrows<IllegalStateException> { registry.register(ResetWithWrongParameter()) }
        assertEquals("Reset handler ResetWithWrongParameter.clear must take no parameter or a ResetContext", e.message)
    }
}
