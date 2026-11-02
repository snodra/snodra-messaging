package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.OpenSubscriptions
import io.damoreira.kraken.examples.api.SubscriptionCount
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import io.damoreira.kraken.messaging.test.InMemoryBuses
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SubscriptionCountHandlerTest {
    private val buses = InMemoryBuses(typePrefix = "service-b").register(SubscriptionCountHandler("service-b", SubscriptionRegistry()))

    @Test fun `OpenSubscriptions answers with the service, the instance and the subscriptions the registry holds`() = runBlocking {
        val count = buses.send(OpenSubscriptions())
        assertEquals(SubscriptionCount("service-b", count.instance, 0), count)
        assertTrue(count.instance.isNotBlank())
    }

    @Test fun `the handler is registered as a shared query type`() {
        assertTrue("examples.OpenSubscriptions" in buses.registry.sharedQueryTypes(), "${buses.registry.sharedQueryTypes()}")
    }
}
