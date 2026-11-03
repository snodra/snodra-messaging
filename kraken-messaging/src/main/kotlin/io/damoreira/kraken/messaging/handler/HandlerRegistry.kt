package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.DuplicateHandlerException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.api.SubscriptionQuery
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.external.IdempotentHandler
import io.damoreira.kraken.messaging.external.IdempotentHandlers
import kotlinx.coroutines.flow.Flow
import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberFunctions

/** Collects `@EventHandler`, `@CommandHandler` and `@QueryHandler` methods from beans. */
class HandlerRegistry(private val typeResolver: MessageTypeResolver) {
    private val log = LoggerFactory.getLogger(HandlerRegistry::class.java)
    private val events = ConcurrentHashMap<String, CopyOnWriteArrayList<HandlerMethod>>()
    private val commands = ConcurrentHashMap<String, HandlerMethod>()
    private val queries = ConcurrentHashMap<String, HandlerMethod>()
    private val registered: MutableSet<Any> = Collections.newSetFromMap(IdentityHashMap())
    private val listeners = CopyOnWriteArrayList<() -> Unit>()

    /** Registers the handlers of [bean] (scanned via its target class); returns those added. Idempotent per instance. */
    fun register(bean: Any): List<HandlerMethod> {
        val added = synchronized(this) {
            if (!registered.add(bean)) return emptyList()
            scan(bean).also { it.forEach(::add) }
        }
        if (added.isNotEmpty()) listeners.forEach { it() }
        return added
    }

    /** Every registered handler marked `@IdempotentHandler`: what [HandlerInvoker.verifyIdempotency] checks at startup. */
    fun idempotentHandlers(): List<HandlerMethod> =
        (events.values.flatten() + commands.values + queries.values).filter { it.idempotency != null }

    fun eventHandlers(type: String): List<HandlerMethod> = events[type]?.toList().orEmpty()
    fun commandHandler(type: String): HandlerMethod? = commands[type]
    fun queryHandler(type: String): HandlerMethod? = queries[type]
    fun commandTypes(): Set<String> = commands.keys.toSet()
    fun queryTypes(): Set<String> = queries.keys.toSet()

    /** Query types of `@QueryHandler(shared = true)` handlers (announced as `shared_query_types`). */
    fun sharedQueryTypes(): Set<String> = queries.filterValues { it.shared }.keys.toSet()

    /** Query types of exclusive `@QueryHandler`s (announced as `query_types`). */
    fun exclusiveQueryTypes(): Set<String> = queries.filterValues { !it.shared }.keys.toSet()

    /** Query types served by a subscription query handler (`SubscriptionQuery<I, U>`, P4c), shared or exclusive. */
    fun subscriptionQueryTypes(): Set<String> = queries.filterValues { it.subscription }.keys.toSet()
    fun eventTypes(): Set<String> = events.keys.toSet()

    /** Registers a listener called after each [register] that added handlers. */
    fun onChange(listener: () -> Unit) {
        listeners.add(listener)
    }

    private fun scan(bean: Any): List<HandlerMethod> {
        val klass = AopUtils.getTargetClass(bean).kotlin
        val found = mutableListOf<HandlerMethod>()
        val pending = mutableMapOf<String, HandlerMethod>() // "KIND:type" -> handler, for duplicates within this bean
        for (fn in klass.memberFunctions.sortedBy { it.name }) {
            val kind = kindOf(fn) ?: continue
            val handler = toHandler(bean, klass, fn, kind)
            val key = "$kind:${handler.typeName}"
            if (kind != HandlerKind.EVENT) {
                val existing = pending[key] ?: existingHandler(kind, handler.typeName)
                if (existing != null) {
                    if (existing.shared != handler.shared) {
                        val (exclusive, shared) = if (existing.shared) handler to existing else existing to handler
                        throw IllegalStateException(
                            "Query type '${handler.typeName}' is registered both exclusive and shared in one service " +
                                "(exclusive: ${labelOf(exclusive)}, shared: ${labelOf(shared)}); declare it one way",
                        )
                    }
                    throw DuplicateHandlerException(handler.typeName, labelOf(existing), labelOf(klass, fn))
                }
                pending[key] = handler
            }
            found += handler
        }
        return found
    }

    private fun labelOf(handler: HandlerMethod) =
        "${AopUtils.getTargetClass(handler.bean).kotlin.simpleName}.${handler.function.name}"

    private fun labelOf(klass: KClass<*>, fn: KFunction<*>) = "${klass.simpleName}.${fn.name}"

    private fun existingHandler(kind: HandlerKind, type: String): HandlerMethod? =
        if (kind == HandlerKind.COMMAND) commands[type] else queries[type]

    private fun kindOf(fn: KFunction<*>): HandlerKind? = when {
        fn.findAnnotation<EventHandler>() != null -> HandlerKind.EVENT
        fn.findAnnotation<CommandHandler>() != null -> HandlerKind.COMMAND
        fn.findAnnotation<QueryHandler>() != null -> HandlerKind.QUERY
        else -> null
    }

    private fun toHandler(bean: Any, klass: KClass<*>, fn: KFunction<*>, kind: HandlerKind): HandlerMethod {
        val params = fn.parameters.drop(1) // drop the receiver
        val wantsContext = params.size == 2 && params[1].type.classifier == MessageContext::class
        val messageClass = params.firstOrNull()?.type?.classifier as? KClass<*>
        if (messageClass == null || !(params.size == 1 || wantsContext)) {
            throw IllegalStateException("Handler ${klass.simpleName}.${fn.name} must take (message) or (message, MessageContext)")
        }
        val requires = fn.findAnnotation<RequiresAuthentication>()
        if (requires != null && kind == HandlerKind.EVENT) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)}: @RequiresAuthentication is not supported on @EventHandler")
        }
        val authorization = requires?.let { HandlerAuthorizations.compile(it.expression, labelOf(klass, fn)) }
        val shared = kind == HandlerKind.QUERY && fn.findAnnotation<QueryHandler>()!!.shared
        // A StreamingQuery<R> (P4b) is served by a @QueryHandler returning Flow<R>; the hub routes it with mode STREAMING.
        val streaming = messageClass.isSubclassOf(StreamingQuery::class)
        if (streaming && kind != HandlerKind.QUERY) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)} takes a StreamingQuery: use @QueryHandler")
        }
        if (streaming && fn.returnType.classifier != Flow::class) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)} takes a StreamingQuery and must return kotlinx.coroutines.flow.Flow<R>")
        }
        // A SubscriptionQuery<I, U> (P4c) is served by a @QueryHandler returning I (the initial result); the updates go
        // through the QueryUpdateEmitter, so a Flow would be a streaming handler's shape, never a subscription's.
        val subscription = messageClass.isSubclassOf(SubscriptionQuery::class)
        if (subscription && streaming) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)} takes a message that is both a StreamingQuery and a SubscriptionQuery: implement one of them")
        }
        if (subscription && kind != HandlerKind.QUERY) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)} takes a SubscriptionQuery: use @QueryHandler")
        }
        if (subscription && fn.returnType.classifier == Flow::class) {
            throw IllegalStateException("Handler ${labelOf(klass, fn)} takes a SubscriptionQuery and must return its initial result I, not a Flow (updates go through QueryUpdateEmitter)")
        }
        if (!streaming && kind == HandlerKind.QUERY && fn.returnType.classifier == Flow::class) {
            // A Flow is no reply: it would be JSON-encoded as an empty object. Only a streaming query is served part by part.
            throw IllegalStateException("Handler ${labelOf(klass, fn)} returns a Flow: its query must implement StreamingQuery<R>")
        }
        val idempotency = fn.findAnnotation<IdempotentHandler>()?.let {
            if (kind == HandlerKind.QUERY) {
                throw IllegalStateException("Handler ${labelOf(klass, fn)}: @IdempotentHandler is for command and event handlers, not queries")
            }
            if (fn.isSuspend) {
                throw IllegalStateException("Handler ${labelOf(klass, fn)}: an @IdempotentHandler must not be suspend (its bookkeeping runs in transactions that are bound to the thread)")
            }
            IdempotentHandlers.compile(it, labelOf(klass, fn))
        }
        return HandlerMethod(bean, fn, kind, messageClass, typeResolver.typeNameOf(messageClass), wantsContext, authorization, shared, streaming, subscription, idempotency)
    }

    private fun add(handler: HandlerMethod) {
        handler.authorization?.let { log.info("handler {} requires authentication: {}", handler.label, it.source) }
        when (handler.kind) {
            HandlerKind.EVENT -> events.computeIfAbsent(handler.typeName) { CopyOnWriteArrayList() }.add(handler)
            HandlerKind.COMMAND -> commands[handler.typeName] = handler
            HandlerKind.QUERY -> queries[handler.typeName] = handler
        }
    }
}
