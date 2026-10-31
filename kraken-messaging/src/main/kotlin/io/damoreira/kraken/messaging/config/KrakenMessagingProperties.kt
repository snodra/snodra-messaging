package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.RetryPolicy
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
        /**
         * Per-request timeout sent as `timeout_ms`. Unset (default): a v2 hub applies its default (5 s) or its per-type
         * override (`KRAKEN_HUB_TIMEOUT_OVERRIDES`); a legacy hub gets 5 s. Set it only to force one timeout for every
         * request of this service.
         */
        val requestTimeout: Duration? = null,
        /** Heartbeat period used only when the hub does not announce one (legacy hubs); a v2 hub announces 3 s. */
        val heartbeatInterval: Duration = Duration.ofSeconds(10),
        /** Jittered exponential backoff when the stream drops. */
        val reconnectBackoff: Backoff = Backoff(),
        /**
         * Incoming hub requests handled concurrently by this instance; further requests wait for a free slot. Also the
         * `initial_permits` announced to a hub with flow control, which then queues the excess on its side.
         */
        val maxConcurrentHandlers: Int = 256,
        /** Relative capacity of this instance (`Register.load_factor`, 100 = normal): weights its share of routing. */
        val loadFactor: Int = 100,
        /** Labels announced in `Register.tags` (for example `zone: eu-1`); unkeyed requests prefer matching instances. */
        val tags: Map<String, String> = emptyMap(),
        /** Default `Request.priority` per message type name; types not listed send 0. Higher is served first when queued. */
        val priorities: Map<String, Int> = emptyMap(),
        /** Opt-in retry of failed sends (see `RetryClassification`). */
        val retry: Retry = Retry(),
    )

    data class Retry(
        /** Retry failed sends that are safe to retry; off by default. */
        val enabled: Boolean = false,
        /** Total attempts including the first. */
        val maxAttempts: Int = 3,
        /** Wait before the second attempt. */
        val initialBackoff: Duration = Duration.ofMillis(100),
        /** Growth of the wait per further attempt. */
        val multiplier: Double = 2.0,
        /** Upper bound of a single wait. */
        val maxBackoff: Duration = Duration.ofSeconds(2),
    ) {
        /** The configured policy, or null when retry is disabled. */
        fun policy(): RetryPolicy? = if (enabled) RetryPolicy(maxAttempts, initialBackoff, multiplier, maxBackoff) else null
    }

    data class Backoff(
        val min: Duration = Duration.ofMillis(500),
        val max: Duration = Duration.ofSeconds(5),
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
    )
}
