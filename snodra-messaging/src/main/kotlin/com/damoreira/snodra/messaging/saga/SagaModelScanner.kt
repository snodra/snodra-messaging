package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * Discovers saga classes by reflection (as `HandlerRegistry` does for beans) and checks them at startup; every violation
 * is an `IllegalStateException` naming the class and method.
 *
 * Rules: the class is annotated [Saga], concrete, with a no-argument constructor, at least one [StartSaga] handler and
 * a state that round-trips through the library's JSON codec (checked on a fresh instance). A [SagaEventHandler] takes an
 * `Event` first (a concrete class, or the supertype narrowed by `payloadType`) whose association property exists; a
 * [DeadlineHandler] takes the payload or nothing first; neither is `suspend`, as they run inside the saga's thread-bound
 * store transaction. [StartSaga] and [EndSaga] only go on those handlers. Handlers of one event type are ordered by
 * method name then parameter types: reflection does not expose declaration order.
 */
class SagaModelScanner(private val typeResolver: MessageTypeResolver, private val codec: JsonCodec) {

    fun scan(classes: Collection<KClass<*>>): List<SagaType> {
        val types = classes.map(::scanClass).sortedBy { it.name }
        types.groupBy { it.name }.filterValues { it.size > 1 }.keys.firstOrNull()?.let { name ->
            val clashing = types.filter { it.name == name }.joinToString { it.klass.qualifiedName ?: it.klass.java.name }
            throw IllegalStateException("Saga type name '$name' is used by several classes: $clashing; set @Saga(name) on one")
        }
        return types
    }

    private fun scanClass(klass: KClass<*>): SagaType {
        val label = klass.simpleName ?: klass.java.name
        val saga = klass.findAnnotation<Saga>()
            ?: throw IllegalStateException("Class ${klass.qualifiedName} is not annotated with @Saga")
        if (klass.isAbstract || klass.java.isInterface || klass.isInner) {
            throw IllegalStateException("Saga $label must be a concrete, non-inner class")
        }
        if (klass.constructors.none { c -> c.parameters.all { it.isOptional } }) {
            throw IllegalStateException("Saga $label needs a no-argument constructor (every parameter of one must have a default)")
        }
        val eventHandlers = mutableMapOf<String, MutableList<SagaEventHandlerMethod>>()
        val deadlineHandlers = mutableMapOf<String, DeadlineHandlerMethod>()
        val functions = klass.memberFunctions.sortedWith(compareBy({ it.name }, { f -> f.parameters.joinToString { it.type.toString() } }))
        for (fn in functions) {
            val event = fn.findAnnotation<SagaEventHandler>()
            val deadline = fn.findAnnotation<DeadlineHandler>()
            val where = "$label.${fn.name}"
            if (event != null && deadline != null) {
                throw IllegalStateException("Saga handler $where cannot be both @SagaEventHandler and @DeadlineHandler")
            }
            if (event == null && deadline == null) {
                if (fn.findAnnotation<StartSaga>() != null) throw IllegalStateException("Saga handler $where: @StartSaga only goes on a @SagaEventHandler")
                if (fn.findAnnotation<EndSaga>() != null) throw IllegalStateException("Saga handler $where: @EndSaga only goes on a @SagaEventHandler or a @DeadlineHandler")
                continue
            }
            if (fn.isSuspend) {
                throw IllegalStateException("Saga handler $where must not be suspend: it runs inside the saga's thread-bound store transaction")
            }
            if (event != null) {
                val handler = eventHandler(where, fn, event)
                eventHandlers.getOrPut(handler.eventType) { mutableListOf() } += handler
            } else {
                if (fn.findAnnotation<StartSaga>() != null) throw IllegalStateException("Saga handler $where: @StartSaga only goes on a @SagaEventHandler")
                val handler = deadlineHandler(where, fn, deadline!!)
                deadlineHandlers.put(handler.deadlineName, handler)?.let {
                    throw IllegalStateException("Saga $label has two handlers for deadline '${handler.deadlineName}': ${it.function.name} and ${fn.name}")
                }
            }
        }
        if (eventHandlers.values.flatten().none { it.start }) {
            throw IllegalStateException("Saga $label has no @StartSaga handler: no instance could ever be created")
        }
        val type = SagaType(saga.name.ifBlank { klass.java.name }, klass, eventHandlers, deadlineHandlers)
        checkRoundTrip(label, type)
        return type
    }

    private fun eventHandler(where: String, fn: KFunction<*>, annotation: SagaEventHandler): SagaEventHandlerMethod {
        val declared = fn.parameters.getOrNull(1)?.type?.classifier as? KClass<*>
        if (declared == null || !declared.isSubclassOf(Event::class)) {
            throw IllegalStateException("Saga handler $where: the first parameter must be an Event, was ${fn.parameters.getOrNull(1)?.type}")
        }
        val narrowed = annotation.payloadType.takeIf { it != Event::class }
        if (narrowed != null && !narrowed.isSubclassOf(declared)) {
            throw IllegalStateException("Saga handler $where: payloadType ${narrowed.simpleName} is not a ${declared.simpleName}")
        }
        val eventClass = narrowed ?: declared
        if (eventClass == Event::class) {
            throw IllegalStateException("Saga handler $where takes the bare Event interface: declare a concrete event or set payloadType")
        }
        val property = annotation.associationProperty.takeIf { it.isNotBlank() }
        val header = annotation.associationHeader.takeIf { it.isNotBlank() }
        if (property == null && header == null) {
            throw IllegalStateException("Saga handler $where: @SagaEventHandler needs an associationProperty or an associationHeader")
        }
        if (property != null && eventClass.memberProperties.none { it.name == property }) {
            throw IllegalStateException("Saga handler $where: association property '$property' is not a property of ${eventClass.simpleName}")
        }
        val params = SagaParameters.plan(where, fn, ParamKind.Event)
        return SagaEventHandlerMethod(
            function = fn,
            eventClass = eventClass,
            eventType = typeResolver.typeNameOf(eventClass),
            keyName = annotation.keyName.ifBlank { property ?: header!! },
            associationProperty = property,
            associationHeader = header,
            start = fn.findAnnotation<StartSaga>() != null,
            forceNew = fn.findAnnotation<StartSaga>()?.forceNew == true,
            end = fn.findAnnotation<EndSaga>() != null,
            params = params,
        )
    }

    private fun deadlineHandler(where: String, fn: KFunction<*>, annotation: DeadlineHandler): DeadlineHandlerMethod {
        if (annotation.deadlineName.isBlank()) throw IllegalStateException("Saga handler $where: @DeadlineHandler needs a deadlineName")
        // Only a first parameter can be the payload; later ones are lifecycle types or beans.
        val params = SagaParameters.plan(where, fn, ParamKind.DeadlinePayload)
        val payloadClass = if (params.firstOrNull() == ParamKind.DeadlinePayload) fn.parameters[1].type.classifier as KClass<*> else null
        return DeadlineHandlerMethod(fn, annotation.deadlineName, fn.findAnnotation<EndSaga>() != null, payloadClass, params)
    }

    /** The stored form (the codec's exact JSON text of a fresh instance) must be an object that decodes and encodes to the same text. */
    private fun checkRoundTrip(label: String, type: SagaType) {
        try {
            val stored = String(codec.encode(type.newInstance()), Charsets.UTF_8)
            if (!codec.decodeTree(stored.toByteArray(Charsets.UTF_8)).isObject) throw IllegalStateException("it is not a JSON object: $stored")
            val again = String(codec.encode(codec.decode(stored.toByteArray(Charsets.UTF_8), type.klass)), Charsets.UTF_8)
            if (again != stored) throw IllegalStateException("encoding it again gave a different document: $stored then $again")
        } catch (e: Exception) {
            throw IllegalStateException("Saga $label does not round-trip through the library's JSON codec (its state is stored as that JSON text): ${e.message}", e)
        }
    }
}
