package io.damoreira.kraken.messaging.e2e.b

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.e2e.BAdminOnly
import io.damoreira.kraken.messaging.e2e.BHappened
import io.damoreira.kraken.messaging.e2e.BRecord
import io.damoreira.kraken.messaging.e2e.BPing
import io.damoreira.kraken.messaging.e2e.BPong
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.RequiresAuthentication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet

/** Service B of the end-to-end test: owns `b.Ping` and publishes `b.Happened` while handling it. */
@SpringBootApplication(scanBasePackages = ["io.damoreira.kraken.messaging.e2e.b"])
class ServiceB {
    @Bean
    fun jwtDecoder(): JwtDecoder = TestJwt.decoder()
}

@Component
class ServiceBHandlers(private val eventBus: EventBus) {
    /** `BPing.text` -> correlation id of the request that carried it. */
    val correlationIds = ConcurrentHashMap<String, String>()

    /** Ids received through `b.Record` (sent by A's event handler without a token). */
    val recorded = CopyOnWriteArraySet<String>()

    @CommandHandler
    suspend fun handle(command: BPing, ctx: MessageContext): BPong {
        correlationIds[command.text] = ctx.correlationId
        val now = Instant.now()
        eventBus.publish(BHappened(command.text, now), key = command.text)
        return BPong(ctx.userId!!, now)
    }

    @CommandHandler
    @RequiresAuthentication("hasRole('kraken-admin')")
    fun admin(command: BAdminOnly, ctx: MessageContext): BPong = BPong(ctx.userId!!, Instant.now())

    @CommandHandler
    fun record(command: BRecord) {
        recorded += command.id
    }
}
