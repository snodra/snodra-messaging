package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.external.ExternalSourceProvider
import io.damoreira.kraken.messaging.handler.Processors
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * A partner's Kafka topic as an external source (`kraken.messaging.external.kafka.sources.<name>`, spec "Inbound 1"): the
 * core's [ExternalSourceProvider], a subscription over [topic] in the partner's own consumer group [groupId] (by default
 * `<service-name>.external.<name>`). The offset is committed after the core dispatched the record (at least once); translating,
 * the retries and the dedupe id (`<name>:<partition>:<offset>`) are the core's.
 *
 * Its consumer properties are [properties] on top of `auto.offset.reset=earliest`, with the servers ([bootstrapServers], by
 * default the service's own Kafka), the group, the deserializers and auto-commit fixed by the library ([FIXED_KEYS]: an entry for
 * one of them is refused, naming the source and the key). A partner's topic is never created: `allow.auto.create.topics` is
 * false (a missing topic is logged by the client at warn and read once it exists).
 */
class KafkaExternalSource(
    /** The source's name. */
    val name: String,
    private val topic: String,
    private val groupId: String,
    private val bootstrapServers: String,
    private val properties: Map<String, String> = emptyMap(),
    private val settings: KafkaEventStoreSettings = KafkaEventStoreSettings(bootstrapServers),
) : ExternalSourceProvider {
    init {
        require(name.isNotBlank()) { "an external Kafka source needs a name" }
        require(topic.isNotBlank()) { "kraken.messaging.external.kafka.sources.$name: the topic must not be blank" }
        check(name !in Processors.RESERVED_SOURCE_NAMES) {
            "kraken.messaging.external.kafka.sources.$name: the source name '$name' is reserved (its markers would collide with " +
                "the library's own marker families ${Processors.RESERVED_SOURCE_NAMES.sorted()}): choose another name"
        }
        // A fixed key would be overwritten without a word: the one in `properties` would never take effect.
        properties.keys.firstOrNull { it in FIXED_KEYS }?.let { key ->
            throw IllegalStateException(
                "kraken.messaging.external.kafka.sources.$name.properties[$key] cannot be set: the library fixes it " +
                    "(use the source's own bootstrap-servers and group-id; deserializers and auto-commit are not configurable). " +
                    "Fixed keys: ${FIXED_KEYS.joinToString()}",
            )
        }
    }

    override val source: String get() = topic

    override val subscription: String get() = groupId

    /** The consumer's configuration. */
    fun consumerProperties(): Map<String, Any> {
        val config = HashMap<String, Any>()
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        config[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = settings.maxPollRecords
        config.putAll(properties)
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        config[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        config[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = false
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        return config
    }

    override suspend fun subscribe(): Subscription = withContext(Dispatchers.IO) {
        try {
            KafkaSubscription.start(SubscriptionSpec(groupId, topic), settings, KafkaConsumer<String, ByteArray>(consumerProperties()))
        } catch (e: Exception) {
            throw EventStoreException.Failed("subscribing $groupId to the partner topic $topic: ${e.message ?: e::class.java.simpleName}", e)
        }
    }

    override fun toString(): String = "KafkaExternalSource(name=$name, topic=$topic, groupId=$groupId, bootstrapServers=$bootstrapServers, properties=${properties.keys})"

    companion object {
        /** The consumer properties the library sets itself; a source's `properties` cannot hold them. */
        val FIXED_KEYS: List<String> = listOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        )
    }
}
