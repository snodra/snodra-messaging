package io.damoreira.kraken.messaging.codec

import io.damoreira.kraken.messaging.api.MessageType
import kotlin.reflect.KClass

/** Resolves the wire name of a message class: the [MessageType] value, else `<typePrefix>.<SimpleName>`. */
class MessageTypeResolver(private val typePrefix: String) {
    fun typeNameOf(klass: KClass<*>): String =
        klass.java.getAnnotation(MessageType::class.java)?.value ?: "$typePrefix.${klass.simpleName}"
}
