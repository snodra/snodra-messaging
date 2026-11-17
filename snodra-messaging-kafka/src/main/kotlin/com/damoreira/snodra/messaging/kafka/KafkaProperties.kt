package com.damoreira.snodra.messaging.kafka

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * `snodra.messaging.kafka.*`: how the Kafka event store ([KafkaEventStore]) reaches Kafka. The topics, the subscriptions' names
 * and the dead-letter mode are the core's `snodra.messaging.events.*`.
 */
@ConfigurationProperties("snodra.messaging.kafka")
data class KafkaProperties(
    val bootstrapServers: String = "broker:19092",
    /**
     * Kafka client properties added to the producer's, the consumers' and the admin client's configuration (security, client
     * ids, quotas: `client-properties[security.protocol]=SASL_SSL`). They never replace what the store depends on (the group,
     * the deserializers, manual commits, acks and idempotence). Their values never appear in logs. An invalid one does not fail
     * the startup: the clients are built on first use, so a processor reads FAILING (its `last_error` names the problem) and
     * subscribes again with backoff, and the outbox relay retries its publishes.
     */
    val clientProperties: Map<String, String> = emptyMap(),
    /** The most one call to the brokers waits (an admin call, a commit, the offsets a status refresh reads). */
    val adminTimeout: Duration = Duration.ofSeconds(10),
    /** How often a subscription refreshes the end offsets its lag is counted against. */
    val statusRefresh: Duration = Duration.ofSeconds(1),
    /** The most records one poll of a subscription takes from the brokers. */
    val maxPollRecords: Int = 100,
) {
    /** The event store's settings. */
    fun toSettings() = KafkaEventStoreSettings(bootstrapServers, clientProperties, adminTimeout, statusRefresh, maxPollRecords)

    /** Never prints [clientProperties]' values (a `sasl.jaas.config` holds a secret): only their names. */
    override fun toString(): String =
        "KafkaProperties(bootstrapServers=$bootstrapServers, clientProperties=${clientProperties.keys}, adminTimeout=$adminTimeout, " +
            "statusRefresh=$statusRefresh, maxPollRecords=$maxPollRecords)"
}

/** `snodra.messaging.external.kafka.*`: partner Kafka topics read into local snodra events ([KafkaExternalSource]). */
@ConfigurationProperties("snodra.messaging.external.kafka")
data class KafkaExternalProperties(
    /**
     * Partner Kafka topics consumed into local events, by source name. Each source needs an `ExternalRecordTranslator` bean
     * named like it (a missing one fails the startup naming the source).
     */
    val sources: Map<String, Source> = emptyMap(),
) {
    /** One partner topic, `external.kafka.sources.<name>`. */
    data class Source(
        /** Bootstrap servers of the partner's cluster; defaults to `snodra.messaging.kafka.bootstrap-servers`. */
        val bootstrapServers: String? = null,
        /** The partner's topic. */
        val topic: String,
        /** Consumer group; defaults to `<service-name>.external.<name>`. */
        val groupId: String? = null,
        /** Extra consumer properties (for example `security.protocol`); bootstrap servers, group id, deserializers and auto-commit are not overridable. */
        val properties: Map<String, String> = emptyMap(),
    ) {
        init {
            require(topic.isNotBlank()) { "the topic of an external Kafka source must not be blank" }
        }

        /** Never prints [properties]' values (a `sasl.jaas.config` holds a secret): only their names. */
        override fun toString(): String =
            "Source(bootstrapServers=$bootstrapServers, topic=$topic, groupId=$groupId, properties=${properties.keys})"
    }
}
