package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.codec.ResponseTypes
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.security.LocalIdentity
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.hub.HubErrors
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Local, synchronous implementation of all three buses for unit tests. Messages still go through the JSON codec
 * so serialization problems show up in tests.
 */
class InMemoryBuses(typePrefix: String = "test", private val userId: String? = "user-1") : EventBus, CommandBus, QueryBus {
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
        return unwrap(type, command::class, invoker.invokeCommand(type, codec.encode(command), context(type), LocalIdentity.current())) as R
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(query: Query<R>): R {
        val type = resolver.typeNameOf(query::class)
        return unwrap(type, query::class, invoker.invokeQuery(type, codec.encode(query), context(type), LocalIdentity.current())) as R
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
        roles = emptySet(),
        authorization = null,
        headers = emptyMap(),
    )
}
