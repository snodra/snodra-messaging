package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.DuplicateHandlerException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.findAnnotation
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

    fun eventHandlers(type: String): List<HandlerMethod> = events[type]?.toList().orEmpty()
    fun commandHandler(type: String): HandlerMethod? = commands[type]
    fun queryHandler(type: String): HandlerMethod? = queries[type]
    fun commandTypes(): Set<String> = commands.keys.toSet()
    fun queryTypes(): Set<String> = queries.keys.toSet()

    /** Query types of `@QueryHandler(shared = true)` handlers (announced as `shared_query_types`). */
    fun sharedQueryTypes(): Set<String> = queries.filterValues { it.shared }.keys.toSet()

    /** Query types of exclusive `@QueryHandler`s (announced as `query_types`). */
    fun exclusiveQueryTypes(): Set<String> = queries.filterValues { !it.shared }.keys.toSet()
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
        return HandlerMethod(bean, fn, kind, messageClass, typeResolver.typeNameOf(messageClass), wantsContext, authorization, shared)
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
