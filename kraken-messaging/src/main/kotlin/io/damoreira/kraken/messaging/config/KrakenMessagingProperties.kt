package io.damoreira.kraken.messaging.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("kraken.messaging")
data class KrakenMessagingProperties(
    /** Logical service name; used as Kafka consumer group and hub registration name. */
    val serviceName: String,
    val hub: Hub = Hub(),
    /** Default time to wait for a command or query reply. */
    val requestTimeout: Duration = Duration.ofSeconds(5),
    val kafka: Kafka = Kafka(),
    val outbox: Outbox = Outbox(),
) {
    data class Hub(val address: String = "http://localhost:7000")
    data class Kafka(val topic: String = "kraken.events")
    data class Outbox(val enabled: Boolean = true, val pollInterval: Duration = Duration.ofMillis(500))
}
