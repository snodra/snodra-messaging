package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Codec
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.amqp.Symbol
import com.rabbitmq.stream.codec.InternalCodec
import com.rabbitmq.stream.impl.HashUtils
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.netty.buffer.Unpooled

/**
 * The layout on the wire, shared with the Rust add-on (`rabbitmq-stream-client`): how an event becomes an AMQP 1.0 message,
 * which partition stream it goes to, and how a delivered message is read back. These are rules both libraries follow; see
 * [RabbitEventStore] for the whole layout.
 */
internal object RabbitWire {
    /** The message annotation carrying the event's key (RabbitMQ's own name for a routing key); absent for an event without one. */
    const val KEY_ANNOTATION = "x-routing-key"

    /** The content type every event carries (AMQP `properties.content-type`). */
    const val CONTENT_TYPE = "application/json"

    /** The seed of the partition hash: the stream clients' hash routing (murmur3 x86_32), [HashUtils.MURMUR3]'s. */
    const val HASH_SEED = 104_729

    /** The segment of a source that is one plain stream (a dead-letter stream). */
    const val SINGLE_SEGMENT = "0"

    /** The client's own AMQP 1.0 codec, without dependencies (the one its `Client` uses by default). */
    val codec: Codec = InternalCodec()

    /**
     * The index of the partition an event with [routingKey] goes to among [partitions] partitions (in the order the broker lists
     * the super stream's partitions): murmur3 x86_32 of the key's UTF-8 bytes with seed [HASH_SEED], as an unsigned number,
     * modulo the count. Exactly the Java client's `HashRoutingStrategy` (its hash, `Integer.remainderUnsigned`).
     */
    fun partitionIndex(routingKey: String, partitions: Int): Int =
        Integer.remainderUnsigned(HashUtils.MURMUR3.applyAsInt(routingKey), maxOf(1, partitions))

    /** The routing key of [event]: its key, or its id when it has none (an unkeyed event is spread over the partitions by its id). */
    fun routingKey(event: OutgoingEvent): String = event.key ?: event.id

    /**
     * The message of [event]: its payload as the body (one AMQP `data` section), its id as `properties.message-id` (a string),
     * `application/json` as `properties.content-type`, its key (if any) as the message annotation [KEY_ANNOTATION] (a string),
     * and every header as an application property (a string).
     */
    fun message(event: OutgoingEvent): Message {
        val builder = codec.messageBuilder()
        builder.addData(event.payload)
        builder.properties().messageId(event.id).contentType(CONTENT_TYPE)
        event.key?.let { builder.messageAnnotations().entry(KEY_ANNOTATION, it) }
        val properties = builder.applicationProperties()
        for ((name, value) in event.headers.toSortedMap()) properties.entry(name, value)
        return builder.build()
    }

    /** The encoded bytes of [message] (what a publish frame carries for it). */
    fun encode(message: Message): ByteArray {
        val encoded = codec.encode(message)
        val buffer = Unpooled.buffer(encoded.size + 4)
        encoded.writeTo(buffer) // the message's length (4 bytes), then the message
        buffer.skipBytes(4)
        return ByteArray(buffer.readableBytes()).also(buffer::readBytes)
    }

    /** The event's key: the [KEY_ANNOTATION] annotation as text (a string or a symbol). */
    fun keyOf(message: Message): String? = message.messageAnnotations?.get(KEY_ANNOTATION)?.let(::text)

    /** An AMQP value as a header shows it: a string's or symbol's text, a binary's UTF-8, other values as their text; null for null. */
    private fun text(value: Any?): String? = when (value) {
        null -> null
        is String -> value
        is Symbol -> value.toString()
        is ByteArray -> String(value, Charsets.UTF_8)
        else -> value.toString()
    }

    /** The delivery of [message] at [offset] of the stream behind [segment], appended at [timestampMs]. */
    fun delivery(segment: String, offset: Long, timestampMs: Long, message: Message): Delivery {
        val headers = LinkedHashMap<String, String>()
        message.applicationProperties?.forEach { (name, value) -> headers[name] = text(value).orEmpty() }
        val payload = runCatching { message.bodyAsBinary }.getOrNull() ?: ByteArray(0)
        return Delivery(segment, offset.toString(), keyOf(message), headers, payload, timestampMs)
    }

    /** The segment of partition stream [stream] of super stream [source]: the suffix after `<source>-` (`kraken.events-3` is `3`), else the stream's whole name. */
    fun segmentOf(source: String, stream: String): String {
        val suffix = stream.removePrefix("$source-")
        return if (suffix != stream && suffix.isNotEmpty() && stream.startsWith(source)) suffix else stream
    }
}
