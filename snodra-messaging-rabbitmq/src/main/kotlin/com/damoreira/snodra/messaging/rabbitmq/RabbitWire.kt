package com.damoreira.snodra.messaging.rabbitmq

import com.rabbitmq.stream.Codec
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.amqp.Symbol
import com.rabbitmq.stream.codec.InternalCodec
import com.damoreira.snodra.messaging.eventstore.Delivery
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
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

    /** The seed of the partition hash: the stream clients' hash routing (murmur3 x86_32 with this seed). */
    const val HASH_SEED = 104_729

    /** The segment of a source that is one plain stream (a dead-letter stream). */
    const val SINGLE_SEGMENT = "0"

    /** The client's own AMQP 1.0 codec, without dependencies (the one its `Client` uses by default). */
    val codec: Codec = InternalCodec()

    /**
     * The index of the partition an event with [routingKey] goes to among [partitions] partitions (in the order the broker lists
     * the super stream's partitions): murmur3 x86_32 of the key's UTF-8 bytes with seed [HASH_SEED], as an unsigned number,
     * modulo the count. Exactly what the Java client's `HashRoutingStrategy` computes (its hash, `Integer.remainderUnsigned`).
     */
    fun partitionIndex(routingKey: String, partitions: Int): Int =
        Integer.remainderUnsigned(murmur3(routingKey.toByteArray(Charsets.UTF_8), HASH_SEED), maxOf(1, partitions))

    /** MurmurHash3 x86_32 of [data] with [seed] (Austin Appleby's reference algorithm). */
    fun murmur3(data: ByteArray, seed: Int): Int {
        val c1 = -0x3361d2af // 0xcc9e2d51
        val c2 = 0x1b873593
        var h = seed
        val blocks = data.size / 4
        for (i in 0 until blocks) {
            val at = i * 4
            var k = (data[at].toInt() and 0xff) or ((data[at + 1].toInt() and 0xff) shl 8) or
                ((data[at + 2].toInt() and 0xff) shl 16) or ((data[at + 3].toInt() and 0xff) shl 24)
            k *= c1
            k = Integer.rotateLeft(k, 15)
            k *= c2
            h = h xor k
            h = Integer.rotateLeft(h, 13)
            h = h * 5 + -0x19ab949c // 0xe6546b64
        }
        val tail = blocks * 4
        var k = 0
        val rest = data.size and 3
        if (rest >= 3) k = k xor ((data[tail + 2].toInt() and 0xff) shl 16)
        if (rest >= 2) k = k xor ((data[tail + 1].toInt() and 0xff) shl 8)
        if (rest >= 1) {
            k = k xor (data[tail].toInt() and 0xff)
            k *= c1
            k = Integer.rotateLeft(k, 15)
            k *= c2
            h = h xor k
        }
        h = h xor data.size
        h = h xor (h ushr 16)
        h *= -0x7a143595 // 0x85ebca6b
        h = h xor (h ushr 13)
        h *= -0x3d4d51cb // 0xc2b2ae35
        h = h xor (h ushr 16)
        return h
    }

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

    /** Whether [segment] fits the protocol's bound (1 to [com.damoreira.snodra.messaging.eventstore.MAX_SEGMENT_BYTES] bytes). */
    fun segmentFits(segment: String): Boolean =
        segment.isNotEmpty() && segment.toByteArray(Charsets.UTF_8).size <= com.damoreira.snodra.messaging.eventstore.MAX_SEGMENT_BYTES

    /** The segment of partition stream [stream] of super stream [source]: the suffix after `<source>-` (`snodra.events-3` is `3`), else the stream's whole name. */
    fun segmentOf(source: String, stream: String): String {
        val suffix = stream.removePrefix("$source-")
        return if (suffix != stream && suffix.isNotEmpty() && stream.startsWith(source)) suffix else stream
    }
}
