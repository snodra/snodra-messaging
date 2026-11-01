package io.damoreira.kraken.messaging.codec

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import kotlin.reflect.KClass
import kotlin.reflect.full.allSupertypes

object ResponseTypes {
    private val MESSAGE_INTERFACES = setOf(Command::class, Query::class, StreamingQuery::class)

    /** The reply type `R` of a [Command]`<R>` or [Query]`<R>` implementation; the element type of a [StreamingQuery]`<R>`. */
    fun responseTypeOf(messageClass: KClass<*>): KClass<*> {
        val supertype = messageClass.allSupertypes.firstOrNull { it.classifier in MESSAGE_INTERFACES }
            ?: throw IllegalArgumentException("${messageClass.qualifiedName} implements neither Command, Query nor StreamingQuery")
        return supertype.arguments.firstOrNull()?.type?.classifier as? KClass<*>
            ?: throw IllegalArgumentException("Cannot determine the response type of ${messageClass.qualifiedName}")
    }
}
