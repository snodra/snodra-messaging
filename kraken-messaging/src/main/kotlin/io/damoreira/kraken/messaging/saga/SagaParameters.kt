package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.full.isSubclassOf

/** How the saga runtime fills one handler parameter, per call. */
sealed interface ParamKind {
    /** The event (first parameter of a saga event handler). */
    data object Event : ParamKind

    /** The [SagaLifecycle] of the handling instance. */
    data object Lifecycle : ParamKind

    /** The [SagaCommands] recording what the instance sends. */
    data object Commands : ParamKind

    /** The [DeadlineManager] of the handling instance. */
    data object Deadlines : ParamKind

    /** The [MessageContext] of the message being handled. */
    data object Context : ParamKind

    /** The payload of the deadline (first parameter of a deadline handler that is not a lifecycle type). */
    data object DeadlinePayload : ParamKind

    /** A Spring bean of [type], looked up when the handler runs. */
    data class Bean(val type: KClass<*>) : ParamKind
}

/** Plans the parameter resolution of saga handlers at startup; an unresolvable parameter is an error naming it. */
internal object SagaParameters {
    /** The plan for [fn]'s parameters after the receiver; [first] decides what a first parameter is (event or payload). */
    fun plan(label: String, fn: KFunction<*>, first: ParamKind?): List<ParamKind> {
        val params = fn.parameters.drop(1)
        return params.mapIndexed { index, p ->
            when (val special = special(p)) {
                null -> when {
                    index == 0 && first != null -> first
                    else -> bean(label, p)
                }
                else -> special
            }
        }
    }

    private fun special(p: KParameter): ParamKind? = when (p.type.classifier) {
        SagaLifecycle::class -> ParamKind.Lifecycle
        SagaCommands::class -> ParamKind.Commands
        DeadlineManager::class -> ParamKind.Deadlines
        MessageContext::class -> ParamKind.Context
        else -> null
    }

    private fun bean(label: String, p: KParameter): ParamKind {
        val klass = p.type.classifier as? KClass<*>
        if (klass == null || p.isVararg || klass.java.isPrimitive || klass in NOT_BEANS || klass.isSubclassOf(Collection::class) ||
            klass.isSubclassOf(Map::class) || klass.isSubclassOf(Enum::class) || klass.java.isArray
        ) {
            throw IllegalStateException(
                "Saga handler $label: parameter '${p.name}' of type ${p.type} cannot be resolved " +
                    "(allowed: SagaLifecycle, SagaCommands, DeadlineManager, MessageContext, or a Spring bean type)",
            )
        }
        return ParamKind.Bean(klass)
    }

    private val NOT_BEANS: Set<KClass<*>> = setOf(
        Any::class, Unit::class, String::class, Number::class, Boolean::class, Char::class,
        java.time.Duration::class, java.time.Instant::class,
    )
}
