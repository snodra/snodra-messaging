package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.MessageContext
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * A discovered saga class: its wire [name] (`sagas.type`), the handlers by event type name and the deadline handlers by
 * deadline name. Event handlers of one event type are in the scanner's stable order (by method name, then parameters).
 */
class SagaType(
    val name: String,
    val klass: KClass<*>,
    val eventHandlers: Map<String, List<SagaEventHandlerMethod>>,
    val deadlineHandlers: Map<String, DeadlineHandlerMethod>,
) {
    private val constructor = klass.constructors.first { c -> c.parameters.all { it.isOptional } }.also { it.isAccessible = true }

    /** A fresh instance through the no-argument constructor (the scanner checked it exists). */
    fun newInstance(): Any = constructor.callBy(emptyMap())!!

    override fun toString() = "SagaType($name)"
}

/**
 * One `@SagaEventHandler` method. [eventClass] is the class the payload is decoded as (the first parameter's type, or
 * `payloadType` when narrowed) and [eventType] its wire name. The association is `keyName = value`, the value read from
 * [associationProperty] of the event or, when [associationHeader] is set, from that message header.
 */
class SagaEventHandlerMethod(
    val function: KFunction<*>,
    val eventClass: KClass<*>,
    val eventType: String,
    val keyName: String,
    val associationProperty: String?,
    val associationHeader: String?,
    val start: Boolean,
    val forceNew: Boolean,
    val end: Boolean,
    val params: List<ParamKind>,
) {
    private val property: KProperty1<Any, *>? = associationProperty?.let { name ->
        @Suppress("UNCHECKED_CAST")
        (eventClass.memberProperties.first { it.name == name } as KProperty1<Any, *>).also { it.isAccessible = true }
    }

    init {
        function.isAccessible = true
    }

    /** The association value of [event]: the header's value when [associationHeader] is set, else the property's; null when absent. */
    fun associationValue(event: Any, ctx: MessageContext): String? =
        if (associationHeader != null) ctx.headers[associationHeader] else property?.get(event)?.toString()

    /** Calls the handler on [saga] with [args] (the resolved parameters, event first). Never suspends: see the saga rules. */
    fun invoke(saga: Any, args: List<Any?>): Any? = call(function, saga, args)
}

/** One `@DeadlineHandler` method; [payloadClass] is the type of the first parameter when it receives the payload. */
class DeadlineHandlerMethod(
    val function: KFunction<*>,
    val deadlineName: String,
    val end: Boolean,
    val payloadClass: KClass<*>?,
    val params: List<ParamKind>,
) {
    init {
        function.isAccessible = true
    }

    /** Calls the handler on [saga] with [args] (the resolved parameters, payload first when there is one). */
    fun invoke(saga: Any, args: List<Any?>): Any? = call(function, saga, args)
}

private fun call(function: KFunction<*>, saga: Any, args: List<Any?>): Any? = try {
    function.call(saga, *args.toTypedArray())
} catch (e: java.lang.reflect.InvocationTargetException) {
    throw e.targetException ?: e
}
