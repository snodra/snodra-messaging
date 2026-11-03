package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.kafka.EventConsumer
import io.damoreira.kraken.messaging.kafka.EventHeaders
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.util.backoff.FixedBackOff

/**
 * Consumes the partner topics of `kraken.messaging.external.kafka.sources.<name>`, one [ConcurrentMessageListenerContainer]
 * per source, and turns their records into local kraken events (spec "Inbound 1").
 *
 * Each record goes through the source's [ExternalRecordTranslator] (the bean named like the source: [translators] must hold
 * one per source, else construction fails naming the source) and the resulting event is dispatched **locally** through the
 * [LocalEventDispatcher] (the ordinary `@EventHandler`s, then the sagas of this service); it is never republished to
 * `kraken.events`. The dedupe id is `"<name>:<partition>:<offset>"`, or `"<name>:<dedupeKey>"` when the translator gives
 * one; it keys the dispatcher's markers, so a record seen twice (a redelivery, or the partner producing the same
 * event twice under one key) is applied once. The handlers' `MessageContext` has that id as `messageId` and in the `id`
 * header, the source's name in `source`, and the record's own headers; the correlation id is the record's `correlationId`
 * header or the dedupe id.
 *
 * Delivery is the main consumer's: manual acknowledgement after the dispatch returned, at-least-once; a translator or
 * handler exception redelivers the record up to [maxAttempts] invocations in all (every [backOffMs]), then logs it (never
 * the payload) and skips it with its offset committed. A null translation is acknowledged without a marker. A source's
 * consumer group is `groupId`, default `<serviceName>.external.<name>`; its consumer properties are the source's
 * `properties` on top of `auto.offset.reset=earliest`, with servers, group, deserializers and auto-commit fixed by the library.
 *
 * Starts and stops with the [EventConsumer] (same lifecycle phase).
 */
class ExternalKafkaSources(
    private val sources: Map<String, KrakenMessagingProperties.Source>,
    private val translators: Map<String, ExternalRecordTranslator>,
    private val dispatcher: LocalEventDispatcher,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val serviceName: String,
    private val defaultBootstrapServers: String,
    private val maxAttempts: Int = 5,
    private val backOffMs: Long = 1000L,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(ExternalKafkaSources::class.java)
    private val containers = LinkedHashMap<String, ConcurrentMessageListenerContainer<String, ByteArray>>()
    @Volatile private var running = false

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
        for (name in sources.keys) {
            check(translators[name] != null) {
                "kraken.messaging.external.kafka.sources.$name has no ExternalRecordTranslator bean named '$name': define one " +
                    "(for example @Component(\"$name\") or a @Bean method called $name) that turns the partner's records into events"
            }
        }
        for ((name, source) in sources) {
            // A fixed key would be overwritten without a word: the one in `properties` would never take effect.
            source.properties.keys.firstOrNull { it in FIXED_KEYS }?.let { key ->
                throw IllegalStateException(
                    "kraken.messaging.external.kafka.sources.$name.properties[$key] cannot be set: the library fixes it " +
                        "(use the source's own bootstrap-servers and group-id; deserializers and auto-commit are not configurable). " +
                        "Fixed keys: ${FIXED_KEYS.joinToString()}",
                )
            }
        }
    }

    private companion object {
        /** The consumer properties the library sets itself, after the source's `properties`. */
        val FIXED_KEYS = listOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        )
    }

    internal fun groupIdFor(name: String): String = sources.getValue(name).groupId ?: "$serviceName.external.$name"

    @Synchronized
    override fun start() {
        if (running) return
        try {
            for ((name, source) in sources) containers[name] = container(name, source).also { it.start() }
        } catch (e: Exception) {
            stopContainers() // a failed start leaves nothing half-running and the lifecycle restartable
            throw e
        }
        running = true
        log.info("External Kafka sources started: {}", sources.entries.joinToString { "${it.key} -> ${it.value.topic}" })
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        stopContainers()
    }

    private fun stopContainers() {
        containers.values.forEach { runCatching { it.stop() } }
        containers.clear()
    }

    override fun isRunning(): Boolean = running
    override fun isAutoStartup(): Boolean = true
    override fun getPhase(): Int = EventConsumer.PHASE

    private fun container(name: String, source: KrakenMessagingProperties.Source): ConcurrentMessageListenerContainer<String, ByteArray> {
        val config = HashMap<String, Any>()
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        config.putAll(source.properties)
        config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = source.bootstrapServers ?: defaultBootstrapServers
        config[ConsumerConfig.GROUP_ID_CONFIG] = groupIdFor(name)
        config[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        val translator = translators.getValue(name)
        val props = ContainerProperties(source.topic).apply {
            setGroupId(groupIdFor(name))
            setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
            setMessageListener(AcknowledgingMessageListener<String, ByteArray> { record, ack -> handle(name, translator, record, ack!!) })
        }
        val errorHandler = DefaultErrorHandler(
            { record, ex -> logSkipped(name, record, ex) },
            FixedBackOff(backOffMs, (maxAttempts - 1).toLong()),
        ).apply {
            setCommitRecovered(true)
            addNotRetryableExceptions(InvalidMessageException::class.java)
        }
        return ConcurrentMessageListenerContainer(DefaultKafkaConsumerFactory<String, ByteArray>(config), props).apply {
            concurrency = 1
            setCommonErrorHandler(errorHandler)
        }
    }

    private fun handle(name: String, translator: ExternalRecordTranslator, record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment) {
        val translated = translator.translate(record)
        if (translated == null) {
            log.debug("Source {} ignored record at {}-{}@{}", name, record.topic(), record.partition(), record.offset())
            ack.acknowledge(); return
        }
        val dedupeId = "$name:" + (translated.dedupeKey?.takeIf { it.isNotBlank() } ?: "${record.partition()}:${record.offset()}")
        val type = typeResolver.typeNameOf(translated.event::class)
        val recordHeaders = record.headers().associate { it.key() to String(it.value() ?: ByteArray(0), Charsets.UTF_8) }
        val headers = recordHeaders + mapOf(EventHeaders.ID to dedupeId, EventHeaders.TYPE to type, EventHeaders.SOURCE to name)
        val ctx = MessageContext(
            messageId = dedupeId,
            type = type,
            correlationId = recordHeaders[EventHeaders.CORRELATION_ID]?.takeIf { it.isNotBlank() } ?: dedupeId,
            userId = null,
            roles = emptySet(),
            authorization = null,
            headers = headers,
        )
        dispatcher.dispatch(dedupeId, type, codec.encode(translated.event), ctx)
        ack.acknowledge()
    }

    private fun logSkipped(name: String, record: ConsumerRecord<*, *>, ex: Exception) {
        val cause = ex.cause ?: ex
        log.error(
            "Giving up on a record of external source {} at {}-{}@{} after {} attempts: {}: {}",
            name, record.topic(), record.partition(), record.offset(), maxAttempts, cause::class.java.name, cause.message,
        )
    }
}
