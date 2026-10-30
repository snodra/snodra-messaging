package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.CountGreetings
import io.damoreira.kraken.examples.api.GreetingAccepted
import io.damoreira.kraken.examples.api.GreetingCount
import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.examples.api.SendGreeting
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.QueryHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Owner of the greetings domain: counts greetings per name in memory (lost on restart, by design). */
@Component
class GreetingHandlers(private val events: EventBus) {
    private val log = LoggerFactory.getLogger(GreetingHandlers::class.java)
    private val counts = ConcurrentHashMap<String, AtomicLong>()

    @CommandHandler
    suspend fun handle(cmd: SendGreeting, ctx: MessageContext): GreetingAccepted {
        val id = UUID.randomUUID().toString()
        counts.computeIfAbsent(cmd.name) { AtomicLong() }.incrementAndGet()
        events.publish(GreetingDelivered(id, cmd.name, Instant.now()), key = cmd.name)
        log.info("B: handled SendGreeting name={} id={} correlationId={}", cmd.name, id, ctx.correlationId)
        return GreetingAccepted(id, cmd.name)
    }

    @QueryHandler
    fun handle(q: CountGreetings): GreetingCount {
        val count = counts[q.name]?.get() ?: 0L
        log.info("B: answered CountGreetings name={} count={}", q.name, count)
        return GreetingCount(q.name, count)
    }
}
