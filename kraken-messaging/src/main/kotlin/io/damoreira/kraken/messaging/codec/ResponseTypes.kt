package io.damoreira.kraken.messaging.codec

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Query
import kotlin.reflect.KClass
import kotlin.reflect.full.allSupertypes

object ResponseTypes {
    /** The reply type `R` of a [Command]`<R>` or [Query]`<R>` implementation. */
    fun responseTypeOf(messageClass: KClass<*>): KClass<*> {
        val supertype = messageClass.allSupertypes.firstOrNull { it.classifier == Command::class || it.classifier == Query::class }
            ?: throw IllegalArgumentException("${messageClass.qualifiedName} implements neither Command nor Query")
        return supertype.arguments.firstOrNull()?.type?.classifier as? KClass<*>
            ?: throw IllegalArgumentException("Cannot determine the response type of ${messageClass.qualifiedName}")
    }
}
