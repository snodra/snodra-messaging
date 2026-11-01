package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.ScatterPart
import io.damoreira.kraken.messaging.ScatterResult
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.codec.ResponseTypes
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.KrakenForwardedAuthentication
import io.damoreira.kraken.messaging.security.LocalIdentity
import io.damoreira.kraken.messaging.security.MessagePrincipal
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.hub.HubErrors
import io.damoreira.kraken.messaging.hub.HubProtocol
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Local, synchronous implementation of all three buses for unit tests. Messages still go through the JSON codec
 * so serialization problems show up in tests. A scatter-gather asks the one registered query handler: one part from
 * service and instance [IN_MEMORY] (a handler error is a failed part), `NoHandlerException` without a handler. A
 * streaming query collects the registered streaming handler's flow (no credits: nothing to pace in memory), honouring
 * `maxResults`; its failure maps as a unary one.
 */
class InMemoryBuses(
    typePrefix: String = "test",
    private val userId: String? = "user-1",
    private val roles: Set<String> = emptySet(),
) : EventBus, CommandBus, QueryBus {
    private val resolver = MessageTypeResolver(typePrefix)
    private val codec = JsonCodec()
    val registry = HandlerRegistry(resolver)
    private val invoker = HandlerInvoker(registry, codec)
    private val publishedEvents = CopyOnWriteArrayList<PublishedEvent>()

    val published: List<PublishedEvent> get() = publishedEvents.toList()

    data class PublishedEvent(val type: String, val key: String?, val event: Event)

    fun register(vararg beans: Any): InMemoryBuses {
        beans.forEach { registry.register(it) }
        return this
    }

    override suspend fun publish(event: Event, key: String?) {
        val type = resolver.typeNameOf(event::class)
        publishedEvents += PublishedEvent(type, key, event)
        invoker.invokeEvent(type, codec.encode(event), context(type))
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(command: Command<R>, routingKey: String?): R {
        val type = resolver.typeNameOf(command::class)
        return unwrap(type, command::class, invoker.invokeCommand(type, codec.encode(command), context(type), identity())) as R
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(query: Query<R>): R {
        val type = resolver.typeNameOf(query::class)
        return unwrap(type, query::class, invoker.invokeQuery(type, codec.encode(query), context(type), identity())) as R
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> scatterGather(query: Query<R>, options: ScatterOptions): ScatterResult<R> {
        val result = try {
            ScatterPart(IN_MEMORY, IN_MEMORY, send(query), null)
        } catch (e: RemoteHandlerException) {
            ScatterPart<R>(IN_MEMORY, IN_MEMORY, null, e)
        }
        return ScatterResult(listOf(result), failed = if (result.error != null) 1 else 0, timedOut = 0)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <R> stream(query: StreamingQuery<R>, options: StreamOptions): Flow<R> {
        val type = resolver.typeNameOf(query::class)
        val elementType = ResponseTypes.responseTypeOf(query::class)
        val parts = flow {
            val failure = invoker.invokeStream(type, codec.encode(query), context(type), identity()) { payload ->
                emit(codec.decode(payload, elementType) as R)
            }
            if (failure != null) throw HubErrors.toException(type, failure.code, failure.message, failure.details, HubProtocol.STREAMING)
        }
        return if (options.maxResults > 0) parts.take(options.maxResults) else parts
    }

    private fun unwrap(type: String, messageClass: kotlin.reflect.KClass<*>, result: InvocationResult): Any = when (result) {
        is InvocationResult.Success -> codec.decode(result.payload, ResponseTypes.responseTypeOf(messageClass))
        is InvocationResult.Failure -> throw HubErrors.toException(type, result.code, result.message, result.details)
    }

    private fun context(type: String) = MessageContext(
        messageId = UUID.randomUUID().toString(),
        type = type,
        correlationId = UUID.randomUUID().toString(),
        userId = userId,
        roles = roles,
        authorization = null,
        headers = emptyMap(),
    )

    /**
     * The caller for annotated handlers: the current `SecurityContext` when a test installed one, otherwise an
     * identity synthesised from the configured `userId` and `roles` (anonymous when `userId` is null).
     */
    private fun identity(): Identity? = LocalIdentity.current() ?: userId?.let {
        val principal = MessagePrincipal(it, roles, verified = true)
        Identity(principal, KrakenForwardedAuthentication(null, principal))
    }

    companion object {
        /** Service and instance name of the one part of an in-memory scatter-gather. */
        const val IN_MEMORY = "in-memory"
    }
}
