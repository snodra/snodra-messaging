package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.Inventory
import io.damoreira.kraken.examples.api.InventoryLevel
import io.damoreira.kraken.messaging.handler.QueryHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

/**
 * service-b's share of the shared query `examples.Inventory` (service-c owns it too): a fixed count of [COUNT] per item,
 * plus how many inventory queries this instance answered.
 */
@Component
class InventoryHandler(
    @Value("\${kraken.messaging.service-name:service-b}") private val service: String = "service-b",
) {
    private val instance: String = System.getenv("HOSTNAME") ?: "local" // the container id under Docker
    private val log = LoggerFactory.getLogger(InventoryHandler::class.java)
    private val answered = AtomicLong()

    @QueryHandler(shared = true)
    suspend fun handle(q: Inventory): InventoryLevel {
        val level = InventoryLevel(service, instance, q.item, COUNT, answered.incrementAndGet())
        log.info("B: answered Inventory item={} answered={}", q.item, level.answered)
        return level
    }

    companion object {
        const val COUNT = 7L
    }
}
