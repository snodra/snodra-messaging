package com.damoreira.snodra.messaging.codec

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Query
import com.damoreira.snodra.messaging.api.StreamingQuery
import com.damoreira.snodra.messaging.api.SubscriptionQuery
import kotlin.reflect.KClass
import kotlin.reflect.full.allSupertypes

object ResponseTypes {
    private val MESSAGE_INTERFACES = setOf(Command::class, Query::class, StreamingQuery::class, SubscriptionQuery::class)

    /**
     * The reply type `R` of a [Command]`<R>` or [Query]`<R>` implementation; the element type of a [StreamingQuery]`<R>`;
     * the initial result type `I` of a [SubscriptionQuery]`<I, U>`.
     */
    fun responseTypeOf(messageClass: KClass<*>): KClass<*> = argumentOf(messageClass, 0)

    /** The update type `U` of a [SubscriptionQuery]`<I, U>` implementation. */
    fun updateTypeOf(messageClass: KClass<*>): KClass<*> {
        val supertype = messageClass.allSupertypes.firstOrNull { it.classifier == SubscriptionQuery::class }
            ?: throw IllegalArgumentException("${messageClass.qualifiedName} does not implement SubscriptionQuery")
        return supertype.arguments.getOrNull(1)?.type?.classifier as? KClass<*>
            ?: throw IllegalArgumentException("Cannot determine the update type of ${messageClass.qualifiedName}")
    }

    private fun argumentOf(messageClass: KClass<*>, index: Int): KClass<*> {
        val supertype = messageClass.allSupertypes.firstOrNull { it.classifier in MESSAGE_INTERFACES }
            ?: throw IllegalArgumentException("${messageClass.qualifiedName} implements neither Command, Query, StreamingQuery nor SubscriptionQuery")
        return supertype.arguments.getOrNull(index)?.type?.classifier as? KClass<*>
            ?: throw IllegalArgumentException("Cannot determine the response type of ${messageClass.qualifiedName}")
    }
}
