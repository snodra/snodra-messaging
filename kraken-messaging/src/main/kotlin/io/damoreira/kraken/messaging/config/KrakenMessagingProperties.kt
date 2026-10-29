package io.damoreira.kraken.messaging.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * `kraken.messaging.*` settings (see `02-messaging.md`, "Configuration keys"). Durations accept Boot's simple form
 * (`200ms`, `5s`, `7d`); environment variables use relaxed binding (`KRAKEN_MESSAGING_HUB_ADDRESS`).
 */
@ConfigurationProperties("kraken.messaging")
data class KrakenMessagingProperties(
    /** Logical service name: Kafka consumer group, event `source` and hub `Register.service_name`. Required. */
    val serviceName: String,
    /** Prefix of the wire type name of message classes without `@MessageType`. Required. */
    val typePrefix: String,
    val hub: Hub = Hub(),
    val kafka: Kafka = Kafka(),
    val outbox: Outbox = Outbox(),
    val idempotency: Idempotency = Idempotency(),
    val security: Security = Security(),
) {
    data class Hub(
        /** Connect to the hub and expose the hub-backed `CommandBus`/`QueryBus` (addition beyond the spec table). */
        val enabled: Boolean = true,
        /** gRPC target of the hub; an optional `static://` prefix is stripped. */
        val address: String = "static://kraken-hub:7000",
        /** Default per-request timeout, sent as `timeout_ms`. */
        val requestTimeout: Duration = Duration.ofSeconds(5),
        /** Heartbeat period on the hub stream. */
        val heartbeatInterval: Duration = Duration.ofSeconds(10),
        /** Exponential backoff when the stream drops. */
        val reconnectBackoff: Backoff = Backoff(),
        /** Incoming hub requests handled concurrently by this instance; further requests wait for a free slot. */
        val maxConcurrentHandlers: Int = 256,
    )

    data class Backoff(
        val min: Duration = Duration.ofMillis(500),
        val max: Duration = Duration.ofSeconds(30),
    )

    data class Kafka(
        val bootstrapServers: String = "broker:19092",
        /** Events topic. */
        val topic: String = "kraken.events",
        val consumer: Consumer = Consumer(),
    )

    data class Consumer(
        /** Set false in tools that only publish. */
        val enabled: Boolean = true,
        /** Handler invocations per record (first try included) before the record is logged and skipped. */
        val maxAttempts: Int = 5,
    )

    data class Outbox(
        /** Run the outbox publisher that relays `outbox` documents to Kafka. */
        val enabled: Boolean = true,
        val pollInterval: Duration = Duration.ofMillis(200),
        /** Documents per poll. */
        val batchSize: Int = 100,
        /** Sent documents older than this are deleted (TTL index). */
        val retention: Duration = Duration.ofDays(7),
        /** Send attempts per document before it is marked failed (`failedAt`) and skipped. */
        val maxAttempts: Int = 10,
    )

    data class Idempotency(
        /** `processed_events` documents older than this are deleted (TTL index). */
        val retention: Duration = Duration.ofDays(30),
    )

    data class Security(
        /** Put the current request's bearer token in the hub request's `authorization` header. */
        val forwardJwt: Boolean = true,
        /** Reject forwarded requests without a valid JWT; requires a `JwtDecoder` bean. */
        val requireJwt: Boolean = true,
    )
}
