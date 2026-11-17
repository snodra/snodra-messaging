package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.Counter
import com.damoreira.snodra.examples.api.CounterChanged
import com.damoreira.snodra.examples.api.WatchCounter
import com.damoreira.snodra.messaging.QueryUpdateEmitter
import com.damoreira.snodra.messaging.emit
import com.damoreira.snodra.messaging.handler.QueryHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * service-b's share of the shared subscription query `examples.WatchCounter` (service-c serves it too): an in-memory
 * counter per name. The handler answers the **initial result** (the counter's current value, 0 for a name nobody
 * incremented); [increment] (driven by `POST /counters/{name}/increment`) bumps the counter and pushes the new value to
 * every subscription of that name on this instance through the [QueryUpdateEmitter].
 */
@Component
class CounterHandler(
    @Value("\${snodra.messaging.service-name:service-b}") private val service: String,
    private val emitter: QueryUpdateEmitter,
) {
    private val instance: String = System.getenv("HOSTNAME") ?: "local" // the container id under Docker
    private val log = LoggerFactory.getLogger(CounterHandler::class.java)
    private val counters = HashMap<String, Long>() // guarded by `this`

    @QueryHandler(shared = true)
    @Synchronized
    fun handle(q: WatchCounter): Counter {
        val counter = Counter(service, instance, q.name, counters[q.name] ?: 0)
        log.info("B: subscribed WatchCounter name={} value={}", q.name, counter.value)
        return counter
    }

    /**
     * Adds one to the counter [name] and emits the change. Synchronized so that two increments emit in the order of their
     * values: a subscriber must see 1, then 2.
     */
    @Synchronized
    fun increment(name: String): Counter {
        val value = (counters[name] ?: 0) + 1
        counters[name] = value
        emitter.emit<WatchCounter, CounterChanged>({ it.name == name }, CounterChanged(service, instance, name, value))
        log.info("B: incremented counter name={} -> {}", name, value)
        return Counter(service, instance, name, value)
    }
}
