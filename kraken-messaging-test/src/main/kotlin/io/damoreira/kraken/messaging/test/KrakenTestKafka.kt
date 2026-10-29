package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.UUID
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/** Reads events back from the Kafka topic in tests: a fresh consumer group per call, from the earliest offset. */
class KrakenTestKafka(
    private val bootstrap: String,
    private val typePrefix: String,
    private val topic: String = "kraken.events",
    private val codec: JsonCodec = JsonCodec(),
) {
    private val typeResolver = MessageTypeResolver(typePrefix)

    fun <T : Any> awaitEvent(
        type: KClass<T>,
        timeout: Duration = 10.seconds,
        predicate: (T) -> Boolean = { true },
    ): Pair<T, Map<String, String>> {
        val typeName = typeResolver.typeNameOf(type)
        val seen = mutableListOf<String>()
        newConsumer().use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.nanoTime() + timeout.inWholeNanoseconds
            while (System.nanoTime() < deadline) {
                for (record in consumer.poll(java.time.Duration.ofMillis(200))) {
                    val headers = record.headers().associate { it.key() to String(it.value()) }
                    seen += "${headers["type"]}@${record.key()}"
                    if (headers["type"] != typeName) continue
                    val event = codec.decode(record.value(), type)
                    if (predicate(event)) return event to headers
                }
            }
        }
        throw AssertionError("No $typeName matching predicate on '$topic' within $timeout; saw: $seen")
    }

    inline fun <reified T : Any> awaitEvent(
        timeout: Duration = 10.seconds,
        noinline predicate: (T) -> Boolean = { true },
    ): Pair<T, Map<String, String>> = awaitEvent(T::class, timeout, predicate)

    private fun newConsumer() = KafkaConsumer<String, ByteArray>(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG to "kraken-test-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ),
        StringDeserializer(),
        ByteArrayDeserializer(),
    )
}
