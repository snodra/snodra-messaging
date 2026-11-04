package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.QueryUpdateEmitter
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.ScatterPart
import io.damoreira.kraken.messaging.ScatterResult
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.SubscriptionOptions
import io.damoreira.kraken.messaging.SubscriptionResult
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.api.SubscriptionQuery
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.codec.ResponseTypes
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.saga.LocalRawCommandSender
import io.damoreira.kraken.messaging.saga.RawCommandSender
import io.damoreira.kraken.messaging.saga.RawOutcome
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.KrakenForwardedAuthentication
import io.damoreira.kraken.messaging.security.LocalIdentity
import io.damoreira.kraken.messaging.security.MessagePrincipal
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.handler.SubscriptionAdmission
import io.damoreira.kraken.messaging.hub.HubErrors
import io.damoreira.kraken.hub.v1.ErrorCode
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import java.util.UUID
import kotlin.reflect.KClass
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Local, synchronous implementation of all three buses for unit tests. Messages still go through the JSON codec
 * so serialization problems show up in tests. A scatter-gather asks the one registered query handler: one part from
 * service and instance [IN_MEMORY] (a handler error is a failed part), `NoHandlerException` without a handler. A
 * streaming query collects the registered streaming handler's flow (no credits: nothing to pace in memory), honouring
 * `maxResults`; its failure maps as a unary one. A subscription query ([subscribe]) answers its initial result from the
 * registered handler and takes its updates from [emitter], the in-memory [QueryUpdateEmitter]: no hub, no credits, no
 * transactions (an emit is delivered at once, where the real emitter defers it to after the commit), `maxResults` and
 * `cancel()` honoured. Like the library, a subscription is **registered before its initial result is read**: an update
 * emitted while the initial handler runs is neither lost nor reordered, it is delivered after the initial (and dropped
 * with the subscription when the initial fails).
 */
class InMemoryBuses(
    typePrefix: String = "test",
    private val userId: String? = "user-1",
    private val roles: Set<String> = emptySet(),
) : EventBus, CommandBus, QueryBus, RawCommandSender {
    private val resolver = MessageTypeResolver(typePrefix)
    private val codec = JsonCodec()
    val registry = HandlerRegistry(resolver)
    private val invoker = HandlerInvoker(registry, codec)
    private val publishedEvents = CopyOnWriteArrayList<PublishedEvent>()

    val published: List<PublishedEvent> get() = publishedEvents.toList()

    private val subscriptions = CopyOnWriteArrayList<LocalSubscription>()

    /** The emitter of the subscriptions [subscribe] opened here: what a service under test calls where it would use the starter's bean. */
    val emitter: QueryUpdateEmitter = LocalEmitter()

    /** How many subscriptions are open: not cancelled, not completed, not ended by `maxResults`. */
    val openSubscriptions: Int get() = subscriptions.size

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

    override suspend fun <R> send(command: Command<R>, routingKey: String?): R = send(command, SendOptions(routingKey = routingKey))

    /** Like the hub bus, [SendOptions.headers] reach the handler's `MessageContext.headers`; the other options have no effect in memory. */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(command: Command<R>, options: SendOptions): R {
        val type = resolver.typeNameOf(command::class)
        return unwrap(type, command::class, invoker.invokeCommand(type, codec.encode(command), context(type, options.headers), identity())) as R
    }

    /** The saga relay's raw path in memory: [LocalRawCommandSender] over the registered handlers, with no caller (as a saga command has none). */
    override suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions): RawOutcome =
        LocalRawCommandSender(invoker).sendRaw(type, payload, options)

    override suspend fun <R> send(query: Query<R>): R = send(query, SendOptions())

    /** As [send] for a command: [SendOptions.headers] reach the handler's `MessageContext.headers`. */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(query: Query<R>, options: SendOptions): R {
        val type = resolver.typeNameOf(query::class)
        return unwrap(type, query::class, invoker.invokeQuery(type, codec.encode(query), context(type, options.headers), identity())) as R
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
            if (failure != null) throw HubErrors.toException(type, failure.code, failure.message, failure.details)
        }
        return if (options.maxResults > 0) parts.take(options.maxResults) else parts
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <I, U> subscribe(query: SubscriptionQuery<I, U>, options: SubscriptionOptions): SubscriptionResult<I, U> {
        val type = resolver.typeNameOf(query::class)
        val payload = codec.encode(query)
        if (registry.queryHandler(type) == null) throw HubErrors.toException(type, ErrorCode.NO_HANDLER, "", ByteArray(0), subscription = true)
        val decoded = when (val admission = invoker.admitSubscription(type, payload, context(type), identity())) {
            is SubscriptionAdmission.Rejected -> admission.failure.let {
                throw HubErrors.toException(type, it.code, it.message, it.details, subscription = true)
            }
            is SubscriptionAdmission.Accepted -> admission.query
        }
        val updateType = ResponseTypes.updateTypeOf(query::class)
        val channel = Channel<ByteArray>(Channel.UNLIMITED)
        val subscription = LocalSubscription(type, decoded, channel, options.maxResults)
        subscriptions += subscription // registered before the initial request, the library's rule (see the class comment): an update emitted meanwhile is not lost
        val initial = try {
            unwrap(type, query::class, invoker.invokeQuery(type, payload, context(type), identity(), subscriptionInitial = true)) as I
        } catch (e: Throwable) {
            subscriptions -= subscription
            channel.close()
            throw e
        }
        val updates = channel.receiveAsFlow().takeWhile { !subscription.cancelled }.onCompletion { subscriptions -= subscription }
        return SubscriptionResult(initial, flow { updates.collect { emit(codec.decode(it, updateType) as U) } }) {
            subscription.cancelled = true
            subscriptions -= subscription
            channel.close()
        }
    }

    private fun unwrap(type: String, messageClass: kotlin.reflect.KClass<*>, result: InvocationResult): Any = when (result) {
        is InvocationResult.Success -> codec.decode(result.payload, ResponseTypes.responseTypeOf(messageClass))
        is InvocationResult.Failure -> throw HubErrors.toException(type, result.code, result.message, result.details)
    }

    private fun context(type: String, headers: Map<String, String> = emptyMap()) = MessageContext(
        messageId = UUID.randomUUID().toString(),
        type = type,
        correlationId = UUID.randomUUID().toString(),
        userId = userId,
        roles = roles,
        authorization = null,
        headers = headers,
    )

    /**
     * The caller for annotated handlers: the current `SecurityContext` when a test installed one, otherwise an
     * identity synthesised from the configured `userId` and `roles` (anonymous when `userId` is null).
     */
    private fun identity(): Identity? = LocalIdentity.current() ?: userId?.let {
        val principal = MessagePrincipal(it, roles, verified = true)
        Identity(principal, KrakenForwardedAuthentication(null, principal))
    }

    /** One open subscription: the decoded query (what a filter looks at) and the channel its updates are queued on. */
    private class LocalSubscription(val type: String, val query: Any, val channel: Channel<ByteArray>, val maxResults: Int) {
        @Volatile var cancelled = false
        private var delivered = 0

        /** Queues one update; true when this was the last one the subscriber asked for. */
        @Synchronized fun deliver(payload: ByteArray): Boolean {
            channel.trySend(payload)
            delivered++
            return maxResults > 0 && delivered >= maxResults
        }
    }

    private inner class LocalEmitter : QueryUpdateEmitter {
        override fun <Q : SubscriptionQuery<*, U>, U : Any> emit(queryType: KClass<Q>, filter: (Q) -> Boolean, update: U) {
            val matching = matching(queryType, filter)
            if (matching.isEmpty()) return
            val payload = codec.encode(update)
            for (subscription in matching) {
                if (subscription.deliver(payload)) end(subscription, null)
            }
        }

        override fun <Q : SubscriptionQuery<*, *>> complete(queryType: KClass<Q>, filter: (Q) -> Boolean) {
            matching(queryType, filter).forEach { end(it, null) }
        }

        override fun <Q : SubscriptionQuery<*, *>> completeExceptionally(queryType: KClass<Q>, filter: (Q) -> Boolean, cause: Throwable) {
            matching(queryType, filter).forEach { end(it, cause) }
        }

        private fun end(subscription: LocalSubscription, cause: Throwable?) {
            subscriptions -= subscription
            if (cause == null) {
                subscription.channel.close()
            } else {
                val message = "${cause::class.simpleName}: ${cause.message}"
                subscription.channel.close(HubErrors.toException(subscription.type, ErrorCode.HANDLER_ERROR, message, ByteArray(0), subscription = true))
            }
        }

        /** The open subscriptions of [queryType] whose query passes [filter]; a filter that throws skips its subscription. */
        private fun <Q : Any> matching(queryType: KClass<Q>, filter: (Q) -> Boolean): List<LocalSubscription> {
            val type = resolver.typeNameOf(queryType)
            return subscriptions.filter { subscription ->
                if (subscription.type != type) return@filter false
                if (!queryType.java.isInstance(subscription.query)) return@filter false
                val query = queryType.java.cast(subscription.query)
                try { filter(query) } catch (e: Exception) { false }
            }
        }
    }

    companion object {
        /** Service and instance name of the one part of an in-memory scatter-gather. */
        const val IN_MEMORY = "in-memory"
    }
}
