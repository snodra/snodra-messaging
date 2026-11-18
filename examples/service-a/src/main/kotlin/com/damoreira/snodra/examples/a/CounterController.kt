package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Counter
import com.damoreira.snodra.examples.api.CounterChanged
import com.damoreira.snodra.examples.api.IncrementCounter
import com.damoreira.snodra.examples.api.OpenSubscriptions
import com.damoreira.snodra.examples.api.SubscriptionCount
import com.damoreira.snodra.examples.api.WatchCounter
import com.damoreira.snodra.messaging.CommandBus
import com.damoreira.snodra.messaging.QueryBus
import com.damoreira.snodra.messaging.ScatterOptions
import com.damoreira.snodra.messaging.SubscriptionOptions
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.time.Duration

/** A collected subscription as JSON: the initial result and the updates that arrived, in the order they did. */
data class CounterWatch(val initial: Counter, val updates: List<CounterChanged>)

/** The subscription counts of the owners of `examples.OpenSubscriptions`, one per instance, and how many parts failed. */
data class OpenSubscriptionCounts(val instances: List<SubscriptionCount>, val failed: Int)

/**
 * The caller side of the shared subscription query `examples.WatchCounter` (served by service-b and service-c): a
 * subscription gets the current value of one counter as its initial result and then the changes of every instance that
 * serves the type, until this controller has collected enough of them (or waited long enough) and cancels.
 */
@RestController
class CounterController(private val queries: QueryBus, private val commands: CommandBus) {
    private val log = LoggerFactory.getLogger(CounterController::class.java)

    /**
     * Opens a subscription to the counter [name], collects the initial result and up to [updates] updates, then cancels it.
     * [timeoutMs] bounds the initial result and, separately, the wait for the updates: when it runs out the updates that
     * arrived are returned (fewer than asked for).
     */
    @GetMapping("/subscribe/counter")
    suspend fun watch(
        @RequestParam name: String,
        @RequestParam(defaultValue = "1") updates: Int,
        @RequestParam(defaultValue = "10000") timeoutMs: Long,
    ): CounterWatch {
        if (name.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "name must not be blank")
        if (updates < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "updates must not be negative")
        if (timeoutMs < 1) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "timeoutMs must be at least 1")
        val subscription = queries.subscribe(WatchCounter(name), SubscriptionOptions(timeout = Duration.ofMillis(timeoutMs)))
        val collected = mutableListOf<CounterChanged>()
        try {
            if (updates > 0) {
                withTimeoutOrNull(timeoutMs) { subscription.updates.take(updates).collect { collected += it } }
            }
        } finally {
            subscription.cancel()
        }
        log.info(
            "A: subscribed WatchCounter name={} -> initial {}={} ({}), {} of {} updates {}",
            name, subscription.initial.service, subscription.initial.value, subscription.initial.instance,
            collected.size, updates, collected.map { "${it.service}=${it.value}" },
        )
        return CounterWatch(subscription.initial, collected)
    }

    /** `examples.IncrementCounter`: service-c (the only owner of the command) increments its counter [name] and emits the change. */
    @PostMapping("/counters/{name}/increment")
    suspend fun increment(@PathVariable name: String, @RequestParam(defaultValue = "service-c") service: String): Counter {
        if (name.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "name must not be blank")
        val counter = commands.send(IncrementCounter(name, service))
        log.info("A: sent IncrementCounter name={} service={} -> {}={}", name, service, counter.service, counter.value)
        return counter
    }

    /** How many subscriptions every instance of every owner of `examples.OpenSubscriptions` serves now (a scatter-gather). */
    @GetMapping("/subscribe/open")
    suspend fun open(): OpenSubscriptionCounts {
        val result = queries.scatterGather(OpenSubscriptions(), ScatterOptions(timeout = InventoryController.TIMEOUT))
        return OpenSubscriptionCounts(result.parts.mapNotNull { it.result }, result.failed + result.timedOut)
    }
}
