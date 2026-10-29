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
    fun eventTypes(): Set<String> = events.keys.toSet()

    /** Registers a listener called after each [register] that added handlers. */
    fun onChange(listener: () -> Unit) {
        listeners.add(listener)
    }

    private fun scan(bean: Any): List<HandlerMethod> {
        val klass = AopUtils.getTargetClass(bean).kotlin
        val found = mutableListOf<HandlerMethod>()
        val pending = mutableMapOf<String, String>() // "KIND:type" -> label, for duplicates within this bean
        for (fn in klass.memberFunctions) {
            val kind = kindOf(fn) ?: continue
            val handler = toHandler(bean, klass, fn, kind)
            val key = "$kind:${handler.typeName}"
            if (kind != HandlerKind.EVENT) {
                val existing = pending[key] ?: existingLabel(kind, handler.typeName)
                if (existing != null) throw DuplicateHandlerException(handler.typeName, existing, labelOf(klass, fn))
                pending[key] = labelOf(klass, fn)
            }
            found += handler
        }
        return found
    }

    private fun labelOf(klass: KClass<*>, fn: KFunction<*>) = "${klass.simpleName}.${fn.name}"

    private fun existingLabel(kind: HandlerKind, type: String): String? =
        (if (kind == HandlerKind.COMMAND) commands[type] else queries[type])?.label

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
        return HandlerMethod(bean, fn, kind, messageClass, typeResolver.typeNameOf(messageClass), wantsContext, authorization)
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
