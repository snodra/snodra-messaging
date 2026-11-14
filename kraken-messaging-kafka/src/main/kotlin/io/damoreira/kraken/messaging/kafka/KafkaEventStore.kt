package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * The event store on Apache Kafka (ADR 0027), the add-on `kraken-messaging-kafka`.
 *
 * - **Source, segment, position.** A source is a topic (`kraken.events`, and its dead-letter topic `kraken.events.dlt`), a
 *   segment one of its partitions (the partition number in decimal) and a position an offset (in decimal). The topics are
 *   the operator's: this store neither creates nor changes them.
 * - **Subscription.** A subscription name is a consumer group (`<service>.<processor>`): the instances of one name share the
 *   partitions, every name reads every event, a new name starts at the earliest offset. Acknowledging commits the offset after
 *   the acknowledged position, from the subscription's own thread, so the committed offset of the group is the
 *   subscription's.
 * - **Publish.** A record per event, keyed by the event's key (a keyless event is spread by Kafka), with every header as a
 *   record header (UTF-8) and the payload as the value; [publish] returns when the brokers acknowledged it (`acks=all`,
 *   idempotent producer).
 * - **Capabilities.** All four: [Capability.REPLAY] (consumer group offsets moved by an `Admin` client), [Capability.LAG]
 *   (end offsets against committed offsets), [Capability.NATIVE_DEAD_LETTER] (the `.dlt` topic) and [Capability.BROWSE] (the
 *   event browser can read the topics; it is a separate program).
 *
 * Data and wire are those of the core's Kafka path before the event store existed: the same topics, consumer groups and
 * record headers.
 *
 * Thread-safe; [close] releases the producer.
 */
class KafkaEventStore(private val settings: KafkaEventStoreSettings) : EventStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val producerHolder = lazy { KafkaProducer<String, ByteArray>(settings.producerProperties()) }
    private val producer: KafkaProducer<String, ByteArray> get() = producerHolder.value
    private val consumers = AtomicInteger()

    /** Whether the producer was built (it is built by the first publish). */
    internal val producerBuilt: Boolean get() = producerHolder.isInitialized()

    override val name: String = NAME

    override val capabilities: Set<Capability> = setOf(Capability.REPLAY, Capability.BROWSE, Capability.LAG, Capability.NATIVE_DEAD_LETTER)

    override suspend fun publish(event: OutgoingEvent) {
        if (event.source.isBlank()) throw EventStoreException.Invalid("the event has no source")
        val record = ProducerRecord<String, ByteArray>(event.source, null, event.key, event.payload)
        event.headers.forEach { (name, value) -> record.headers().add(RecordHeader(name, value.toByteArray(Charsets.UTF_8))) }
        try {
            // send() may block up to max.block.ms on the metadata of a topic: not on the caller's thread.
            withContext(Dispatchers.IO) {
                suspendCancellableCoroutine<Unit> { cont ->
                    producer.send(record) { _, error -> if (error == null) cont.resume(Unit) else cont.resumeWithException(error) }
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            throw EventStoreException.Failed("publishing ${event.id} to ${event.source}: ${e.message ?: e::class.java.simpleName}", e)
        }
    }

    override suspend fun subscribe(spec: SubscriptionSpec): Subscription {
        if (spec.name.isBlank() || spec.source.isBlank()) throw EventStoreException.Invalid("a subscription needs a name and a source")
        return withContext(Dispatchers.IO) {
            try {
                KafkaSubscription.start(spec, settings, consumers.incrementAndGet())
            } catch (e: Exception) {
                throw EventStoreException.Failed("subscribing ${spec.name} to ${spec.source}: ${e.message ?: e::class.java.simpleName}", e)
            }
        }
    }

    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin = KafkaSubscriptionAdmin(spec, settings)

    /** Offsets are decimal numbers; an event store without them throws [EventStoreException.Invalid] for a position that is none. */
    override fun compare(a: String, b: String): Int {
        val x = a.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$a\" is not an offset")
        val y = b.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$b\" is not an offset")
        return x.compareTo(y)
    }

    override fun close() {
        if (closed.compareAndSet(false, true) && producerHolder.isInitialized()) runCatching { producer.close(Duration.ofSeconds(5)) }
    }

    companion object {
        /** The name this event store reports to the hub. */
        const val NAME = "kafka"
    }
}

/**
 * How [KafkaEventStore] reaches Kafka (`kraken.messaging.kafka.*` in a Spring application).
 *
 * [clientProperties] are added to the producer's, the consumers' and the admin client's configuration (each takes what it knows):
 * security, client ids, quotas. They never replace what the store depends on (the group, the deserializers, manual commits).
 */
data class KafkaEventStoreSettings(
    val bootstrapServers: String,
    val clientProperties: Map<String, Any> = emptyMap(),
    /** The most one call to the brokers waits (an admin call, a commit, the offsets a status refresh reads). */
    val adminTimeout: Duration = Duration.ofSeconds(10),
    /** How often a subscription refreshes the end offsets its lag is counted against. */
    val statusRefresh: Duration = Duration.ofSeconds(1),
    /** The most records one poll of a subscription takes from the brokers. */
    val maxPollRecords: Int = 100,
) {
    init {
        require(bootstrapServers.isNotBlank()) { "kraken.messaging.kafka.bootstrap-servers must not be blank" }
        require(adminTimeout > Duration.ZERO) { "the Kafka admin timeout must be positive, was $adminTimeout" }
        require(statusRefresh > Duration.ZERO) { "the Kafka status refresh must be positive, was $statusRefresh" }
        require(maxPollRecords >= 1) { "the Kafka max poll records must be at least 1, was $maxPollRecords" }
    }

    /** Never prints [clientProperties]' values (a `sasl.jaas.config` holds a secret): only their names. */
    override fun toString(): String =
        "KafkaEventStoreSettings(bootstrapServers=$bootstrapServers, clientProperties=${clientProperties.keys}, adminTimeout=$adminTimeout, " +
            "statusRefresh=$statusRefresh, maxPollRecords=$maxPollRecords)"

    // The client properties go to every client whole (a plugin's own options, such as an interceptor's or a partitioner's, are
    // not in a client's list of known keys); a client only warns about a key it does not use.
    internal fun producerProperties(): Map<String, Any> = clientProperties + mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
        // Bound a send: metadata/buffer wait, then delivery including retries.
        // delivery.timeout.ms must be >= linger.ms (5 ms default) + request.timeout.ms.
        ProducerConfig.MAX_BLOCK_MS_CONFIG to 10_000,
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 30_000,
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to 20_000,
    )

    /** The consumer's configuration; its `client.id` (when [clientProperties] has one) gets `-<group>-<number>` so the clients tell apart. */
    internal fun consumerProperties(group: String, number: Int = 1): Map<String, Any> = clientProperties + mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to group,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
    ) + (clientProperties["client.id"]?.let { mapOf<String, Any>("client.id" to "$it-$group-$number") } ?: emptyMap())

    internal fun adminProperties(): Map<String, Any> =
        clientProperties + mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)
}
