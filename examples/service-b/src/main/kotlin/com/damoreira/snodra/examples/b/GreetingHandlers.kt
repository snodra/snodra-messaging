package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.CountGreetings
import com.damoreira.snodra.examples.api.GreetingAccepted
import com.damoreira.snodra.examples.api.GreetingCount
import com.damoreira.snodra.examples.api.GreetingDelivered
import com.damoreira.snodra.examples.api.SendGreeting
import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.QueryHandler
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Owner of the greetings domain: counts greetings per name in memory (lost on restart, by design). [countDelay]
 * (`examples.count-delay`, env `EXAMPLES_COUNT_DELAY`, default 0) slows the count query down so the sandbox can show
 * hub flow control with a slow handler.
 */
@Component
class GreetingHandlers(
    private val events: EventBus,
    @Value("\${examples.count-delay:0ms}") private val countDelay: Duration = Duration.ZERO,
) {
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
    suspend fun handle(q: CountGreetings): GreetingCount {
        if (!countDelay.isZero) delay(countDelay.toMillis())
        val count = counts[q.name]?.get() ?: 0L
        log.info("B: answered CountGreetings name={} count={}", q.name, count)
        return GreetingCount(q.name, count)
    }
}
