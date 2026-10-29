package io.damoreira.kraken.messaging.e2e.a

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.e2e.BHappened
import io.damoreira.kraken.messaging.e2e.BRecord
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.EventHandler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

/** Service A of the end-to-end test: owns no command or query, listens to B's events. */
@SpringBootApplication(scanBasePackages = ["io.damoreira.kraken.messaging.e2e.a"])
class ServiceA {
    @Bean
    fun jwtDecoder(): JwtDecoder = TestJwt.decoder()
}

@Component
class ServiceAEvents(private val commands: CommandBus) {
    val received = CopyOnWriteArrayList<BHappened>()
    val contexts = CopyOnWriteArrayList<Pair<BHappened, MessageContext>>()

    @EventHandler
    fun on(event: BHappened, ctx: MessageContext) {
        received += event
        contexts += event to ctx
        // No caller token here (events carry only userId): the command reaches B's unannotated handler anonymously.
        commands.sendBlocking(BRecord(event.id))
    }
}
