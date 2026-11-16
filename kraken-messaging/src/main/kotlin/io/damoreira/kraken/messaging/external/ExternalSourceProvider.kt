package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.eventstore.Subscription

/**
 * A partner's source an event-store add-on reads (the Kafka add-on's `KafkaExternalSource` for a partner's topic, for example),
 * whatever the service's own event store is: a subscription of its own over the partner's source. The core reads it
 * ([ExternalSources]): translating, retrying and the dedupe id are the core's.
 */
interface ExternalSourceProvider {
    /** The partner's source it reads ([ExternalRecord.source]). */
    val source: String

    /** The name of the subscription it reads in (a consumer group on Kafka), for the logs. */
    val subscription: String

    /**
     * Joins the subscription as one more instance; see the [Subscription] contract (a failure of `next` ends it, and the core
     * subscribes again). Never creates the partner's source.
     */
    suspend fun subscribe(): Subscription
}

/**
 * The partner sources an add-on provides, by source name (the `<name>` of `kraken.messaging.external.<add-on>.sources.<name>`):
 * the beans [ExternalSources] reads. A source name is unique across every such bean.
 */
fun interface ExternalSourceProviders {
    fun providers(): Map<String, ExternalSourceProvider>
}
