package io.damoreira.kraken.messaging.e2e.a

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.e2e.BHappened
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
class ServiceAEvents {
    val received = CopyOnWriteArrayList<BHappened>()
    val contexts = CopyOnWriteArrayList<Pair<BHappened, MessageContext>>()

    @EventHandler
    fun on(event: BHappened, ctx: MessageContext) {
        received += event
        contexts += event to ctx
    }
}
