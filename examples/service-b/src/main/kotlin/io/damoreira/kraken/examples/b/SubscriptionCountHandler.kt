package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.OpenSubscriptions
import io.damoreira.kraken.examples.api.SubscriptionCount
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * service-b's share of the shared query `examples.OpenSubscriptions`: how many subscriptions this instance serves now
 * (the starter's [SubscriptionRegistry], every type, over every hub). The smoke scatter-gathers it to see that nothing
 * is left behind once a subscriber is gone.
 */
@Component
class SubscriptionCountHandler(
    @Value("\${kraken.messaging.service-name:service-b}") private val service: String,
    private val registry: SubscriptionRegistry,
) {
    private val instance: String = System.getenv("HOSTNAME") ?: "local"

    @QueryHandler(shared = true)
    fun handle(q: OpenSubscriptions): SubscriptionCount = SubscriptionCount(service, instance, registry.size.toLong())
}
