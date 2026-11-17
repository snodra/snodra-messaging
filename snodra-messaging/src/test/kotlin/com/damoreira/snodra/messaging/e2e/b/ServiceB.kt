package com.damoreira.snodra.messaging.e2e.b

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.e2e.BAdminOnly
import com.damoreira.snodra.messaging.e2e.BHappened
import com.damoreira.snodra.messaging.e2e.BRecord
import com.damoreira.snodra.messaging.e2e.BPing
import com.damoreira.snodra.messaging.e2e.BPong
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.RequiresAuthentication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet

/** Service B of the end-to-end test: owns `b.Ping` and publishes `b.Happened` while handling it. */
@SpringBootApplication(scanBasePackages = ["com.damoreira.snodra.messaging.e2e.b"])
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
    @RequiresAuthentication("hasRole('snodra-admin')")
    fun admin(command: BAdminOnly, ctx: MessageContext): BPong = BPong(ctx.userId!!, Instant.now())

    @CommandHandler
    fun record(command: BRecord) {
        recorded += command.id
    }
}
