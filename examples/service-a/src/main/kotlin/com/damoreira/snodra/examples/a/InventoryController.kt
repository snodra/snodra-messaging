package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Inventory
import com.damoreira.snodra.examples.api.InventoryLevel
import com.damoreira.snodra.messaging.QueryBus
import com.damoreira.snodra.messaging.ScatterOptions
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.time.Duration

/** One part of a scatter-gather as JSON: the answering service and instance, then its result or its error. */
data class InventoryPart(val service: String, val instance: String, val result: InventoryLevel?, val error: PartError?)

data class PartError(val code: String, val message: String)

data class InventoryGather(val parts: List<InventoryPart>, val failed: Int, val timedOut: Int)

/**
 * The caller side of the shared query `examples.Inventory` (owned by service-b and service-c): a point query goes to
 * one instance of either owner, a scatter-gather asks every instance of both.
 */
@RestController
class InventoryController(private val queries: QueryBus) {
    private val log = LoggerFactory.getLogger(InventoryController::class.java)

    @GetMapping("/inventory")
    suspend fun inventory(@RequestParam(defaultValue = "widget") item: String): InventoryLevel {
        if (item.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "item must not be blank")
        val level = queries.send(Inventory(item))
        log.info("A: asked Inventory item={} -> {} ({})", item, level.service, level.instance)
        return level
    }

    @GetMapping("/scatter/inventory")
    suspend fun scatter(
        @RequestParam(defaultValue = "widget") item: String,
        @RequestParam(defaultValue = "0") maxResults: Int,
    ): InventoryGather {
        if (item.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "item must not be blank")
        if (maxResults < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "maxResults must not be negative")
        val result = queries.scatterGather(Inventory(item), ScatterOptions(timeout = TIMEOUT, maxResults = maxResults))
        log.info(
            "A: scatter-gathered Inventory item={} -> {} parts ({} failed, {} timed out) from {}",
            item, result.parts.size, result.failed, result.timedOut, result.parts.map { it.service },
        )
        return InventoryGather(
            parts = result.parts.map { p ->
                InventoryPart(p.service, p.instance, p.result, p.error?.let { PartError(it.code, it.message ?: "") })
            },
            failed = result.failed,
            timedOut = result.timedOut,
        )
    }

    companion object {
        /** The deadline of a whole gather: a target that has not answered by then counts as timed out. */
        val TIMEOUT: Duration = Duration.ofSeconds(3)
    }
}
