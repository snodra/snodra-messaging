package com.damoreira.snodra.messaging.codec

import com.damoreira.snodra.messaging.InvalidMessageException
import tools.jackson.core.JacksonException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import kotlin.reflect.KClass

/** JSON (Jackson 3) encoding of message payloads. [Unit] is carried as the empty object `{}`. */
class JsonCodec(val mapper: JsonMapper = defaultMapper()) {

    fun encode(value: Any): ByteArray =
        if (value === Unit) EMPTY_OBJECT else mapper.writeValueAsBytes(value)

    fun <T : Any> decode(bytes: ByteArray, type: KClass<T>): T {
        try {
            if (type == Unit::class) {
                if (bytes.isNotEmpty() && !mapper.readTree(bytes).isObject) {
                    throw InvalidMessageException("Cannot decode payload as ${type.qualifiedName}")
                }
                @Suppress("UNCHECKED_CAST")
                return Unit as T
            }
            // The JSON literal `null` decodes to null, which no message or response type admits.
            return mapper.readValue(bytes, type.java)
                ?: throw InvalidMessageException("Cannot decode payload as ${type.qualifiedName}: null")
        } catch (e: JacksonException) {
            throw InvalidMessageException("Cannot decode payload as ${type.qualifiedName}", e)
        }
    }

    fun decodeTree(bytes: ByteArray): JsonNode = mapper.readTree(bytes)

    companion object {
        private val EMPTY_OBJECT = "{}".toByteArray()

        fun defaultMapper(): JsonMapper = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()
    }
}
