package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.deadletter.DeadLetterMode
import com.damoreira.snodra.messaging.deadline.DeadlineSettings
import com.damoreira.snodra.messaging.hub.HubTargets
import com.damoreira.snodra.messaging.events.OutboxPublisher
import com.damoreira.snodra.messaging.eventstore.memory.MemoryEventStoreSettings
import com.damoreira.snodra.messaging.saga.RelaySettings
import org.springframework.boot.context.properties.ConfigurationProperties
import java.net.URI
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

/**
 * `snodra.messaging.*` settings (see `02-messaging.md`, "Configuration keys"). Durations accept Boot's simple form
 * (`200ms`, `5s`, `7d`); environment variables use relaxed binding (`SNODRA_MESSAGING_HUB_ADDRESS`).
 */
@ConfigurationProperties("snodra.messaging")
data class SnodraMessagingProperties(
    /** Logical service name: the prefix of the processors' subscriptions, event `source` and hub `Register.service_name`. Required. */
    val serviceName: String,
    /** Prefix of the wire type name of message classes without `@MessageType`. Required. */
    val typePrefix: String,
    /**
     * This instance's id: its hub `Register.instance_id` and the `service.instance.id` of its metrics (ADR 0026). A fresh UUID
     * per start unless set (a pod name, for example); unique among the instances of the service.
     */
    val instanceId: String = UUID.randomUUID().toString(),
    val hub: Hub = Hub(),
    /**
     * The event store's name (`kafka`, `rabbitmq-streams`, `memory`): which one runs when two event-store add-ons are on the
     * classpath (ADR 0027), and the only way to run the in-memory one (`memory`, never chosen otherwise). Unset: the only one
     * there is.
     */
    val eventStore: String? = null,
    /** The message store (ADR 0028): which one runs, and its schema. */
    val store: Store = Store(),
    /** The in-memory event store (`snodra.messaging.event-store=memory`). */
    val memory: Memory = Memory(),
    val events: Events = Events(),
    val outbox: Outbox = Outbox(),
    val idempotency: Idempotency = Idempotency(),
    val security: Security = Security(),
    val sagas: Sagas = Sagas(),
    val deadlines: Deadlines = Deadlines(),
    val external: External = External(),
) {
    data class Hub(
        /** Connect to the hub and expose the hub-backed `CommandBus`/`QueryBus` (addition beyond the spec table). */
        val enabled: Boolean = true,
        /**
         * The hub or hubs (ADR 0016): one gRPC target (`host:port`, optional `static://` prefix), a comma-separated list
         * of them (one connection per hub), or one `dns:///name:port` re-resolved every 30 s. Parsed by `HubTargets`.
         */
        val address: String = "static://snodra-server:7000",
        /** How often an `InstanceStatus` report goes to every hub. */
        val statusInterval: Duration = Duration.ofSeconds(5),
        /**
         * Per-request timeout sent as `timeout_ms`. Unset (default): the hub applies its default (5 s) or its per-type
         * override (`SNODRA_SERVER_TIMEOUT_OVERRIDES`). Set it only to force one timeout for every request of this service.
         */
        val requestTimeout: Duration? = null,
        /** Jittered exponential backoff when the stream drops. */
        val reconnectBackoff: Backoff = Backoff(),
        /**
         * Incoming hub requests handled concurrently by this instance (>= 1); further requests wait for a free slot.
         * Also the `initial_permits` announced to a hub with flow control, which then queues the excess on its side.
         */
        val maxConcurrentHandlers: Int = 256,
        /**
         * Relative capacity of this instance (`Register.load_factor`, 100 = normal, not negative; the hub reads 0 as
         * 100): weights its share of routing.
         */
        val loadFactor: Int = 100,
        /** Labels announced in `Register.tags` (for example `zone: eu-1`); unkeyed requests prefer matching instances. */
        val tags: Map<String, String> = emptyMap(),
        /** Default `Request.priority` per message type name; types not listed send 0. Higher is served first when queued. */
        val priorities: Map<String, Int> = emptyMap(),
        /** Opt-in retry of failed sends (see `RetryClassification`). */
        val retry: Retry = Retry(),
        /**
         * The longest a streaming handler of this instance waits for credit from the collector before it ends the
         * stream with `TIMEOUT` and frees its handler slot and hub permit (a hub that forgot the stream would otherwise
         * leave it waiting forever). Must be positive; the default is the hub's default maximum stream duration.
         */
        val streamCreditTimeout: Duration = Duration.ofMinutes(10),
        /**
         * Subscription queries (`SubscriptionQuery`) this instance serves at once, over every hub (>= 1): a new
         * subscription beyond it is refused with `RESOURCE_EXHAUSTED` ("subscription limit (N) reached"). A subscription
         * holds no handler permit, so `max-concurrent-handlers` does not bound it. The default is the hub's own
         * per-instance limit (`SNODRA_SERVER_MAX_SUBSCRIPTIONS_PER_INSTANCE`).
         */
        val maxSubscriptions: Int = 1024,
        /**
         * Static token sent in `Register.credentials.token` of every hub stream (P5a, ADR 0018): the hub in mode `enforce`
         * (or `permissive`) looks it up in its ACL. Optional; an empty value is the same as none. Never logged, and
         * redacted in [toString] and in the actuator's `configprops` and `env` output. Environment variable
         * `SNODRA_MESSAGING_HUB_TOKEN` (relaxed binding), or `SNODRA_SERVER_TOKEN` when this property is unset.
         */
        val token: String? = null,
        /**
         * File holding the token (a container secret, for example `/run/secrets/snodra-server-token`): read at startup (a file
         * that cannot be read, or holds only whitespace, fails it) and again on every reconnect, so a rotated secret is picked
         * up without a restart. Trimmed at both ends. When both [token] and this are set, the file wins. Environment variable
         * `SNODRA_MESSAGING_HUB_TOKEN_FILE`, or `SNODRA_SERVER_TOKEN_FILE` when this property is unset.
         */
        val tokenFile: Path? = null,
        /** TLS towards the hub (P5a): off by default (plain text). */
        val tls: Tls = Tls(),
        /**
         * Keycloak client-credentials authentication (P5b, ADR 0019): the service gets an access token from [OAuth2.tokenUri] and
         * registers with it as a `jwt` credential, renewing it before it expires. Off unless a token URI is set (here or as
         * `SNODRA_SERVER_OAUTH2_TOKEN_URI`); cannot be combined with [token] or [tokenFile].
         */
        val oauth2: OAuth2 = OAuth2(),
    ) {
        /** The token never appears: every other field as the data class prints it. */
        override fun toString(): String = "Hub(enabled=$enabled, address=$address, statusInterval=$statusInterval, " +
            "requestTimeout=$requestTimeout, reconnectBackoff=$reconnectBackoff, " +
            "maxConcurrentHandlers=$maxConcurrentHandlers, loadFactor=$loadFactor, tags=$tags, priorities=$priorities, " +
            "retry=$retry, streamCreditTimeout=$streamCreditTimeout, maxSubscriptions=$maxSubscriptions, " +
            "token=${if (token.isNullOrEmpty()) token else "****"}, tokenFile=$tokenFile, tls=$tls, oauth2=$oauth2)"

        init {
            // Checked at binding, so a bad value fails the startup even while the hub is disabled.
            require(maxConcurrentHandlers >= 1) {
                "snodra.messaging.hub.max-concurrent-handlers must be >= 1, was $maxConcurrentHandlers"
            }
            require(loadFactor >= 0) { "snodra.messaging.hub.load-factor must not be negative, was $loadFactor" }
            HubTargets.parse(address) // throws IllegalArgumentException naming the key
            require(!statusInterval.isZero && !statusInterval.isNegative) {
                "snodra.messaging.hub.status-interval must be positive, was $statusInterval"
            }
            require(!streamCreditTimeout.isZero && !streamCreditTimeout.isNegative) {
                "snodra.messaging.hub.stream-credit-timeout must be positive, was $streamCreditTimeout"
            }
            require(maxSubscriptions >= 1) { "snodra.messaging.hub.max-subscriptions must be >= 1, was $maxSubscriptions" }
            // A zero back-off would make a refused service hot-loop handshakes.
            require(!reconnectBackoff.max.isZero && !reconnectBackoff.max.isNegative) {
                "snodra.messaging.hub.reconnect-backoff.max must be positive, was ${reconnectBackoff.max}"
            }
            require(!reconnectBackoff.min.isZero && !reconnectBackoff.min.isNegative) {
                "snodra.messaging.hub.reconnect-backoff.min must be positive, was ${reconnectBackoff.min}"
            }
            require(reconnectBackoff.min <= reconnectBackoff.max) {
                "snodra.messaging.hub.reconnect-backoff.min (${reconnectBackoff.min}) must not be above " +
                    "snodra.messaging.hub.reconnect-backoff.max (${reconnectBackoff.max})"
            }
        }
    }

    /** `hub.tls.*` (P5a, ADR 0018); the client certificate (mTLS) is P5c, ADR 0020. */
    data class Tls(
        /** Talk TLS to the hub (every address form, `dns:///` too). Off: plain text, unless a client certificate is set (it implies TLS). */
        val enabled: Boolean = false,
        /** PEM file with the CA certificate(s) that signed the hub's certificate; the JVM's system roots when absent. Read on every connection attempt. */
        val caFile: Path? = null,
        /** Name to verify the hub's certificate against instead of the address's host (for example an IP or a service alias). */
        val serverName: String? = null,
        /**
         * PEM file with the client certificate chain (leaf first) presented in the TLS handshake (mTLS, P5c, ADR 0020). The leaf must have
         * exactly one URI SAN `snodra://service/<name>` with `<name>` equal to `snodra.messaging.service-name`. Needs [clientKeyFile]; implies TLS;
         * cannot be combined with a static token or OAuth2. Checked at startup, read again on every reconnect and heartbeat tick: a renewed pair
         * is adopted without a restart (the hub connections roll over to it one hub at a time). Environment variable
         * `SNODRA_MESSAGING_HUB_TLS_CLIENT_CERT_FILE`.
         */
        val clientCertFile: Path? = null,
        /** PEM file with the private key of [clientCertFile], unencrypted PKCS#8 (`-----BEGIN PRIVATE KEY-----`). Environment variable `SNODRA_MESSAGING_HUB_TLS_CLIENT_KEY_FILE`. */
        val clientKeyFile: Path? = null,
    ) {
        init {
            // At binding, so one setting alone fails the startup naming the other, also while the hub is disabled.
            require(clientCertFile == null || clientKeyFile != null) {
                "snodra.messaging.hub.tls.client-cert-file is set but snodra.messaging.hub.tls.client-key-file is not: a client certificate needs its key"
            }
            require(clientKeyFile == null || clientCertFile != null) {
                "snodra.messaging.hub.tls.client-key-file is set but snodra.messaging.hub.tls.client-cert-file is not: a client key needs its certificate"
            }
        }
    }

    /**
     * `hub.oauth2.*` (P5b, ADR 0019). Every setting also reads the plain environment variable `SNODRA_SERVER_OAUTH2_<NAME>` (`TOKEN_URI`,
     * `CLIENT_ID`, `CLIENT_SECRET`, `CLIENT_SECRET_FILE`, `SCOPE`, and `CA` for [caFile]) when its property is unset; relaxed binding
     * gives `SNODRA_MESSAGING_HUB_OAUTH2_*` as well.
     */
    data class OAuth2(
        /** The token endpoint (Keycloak: `https://<host>/realms/<realm>/protocol/openid-connect/token`). `http` is allowed (a compose network) and warns unless the host is loopback or a single-label name. */
        val tokenUri: URI? = null,
        /** The confidential client's id, which is the `azp` of its tokens and so must be the service name; defaults to `snodra.messaging.service-name`. */
        val clientId: String? = null,
        /** The client secret. Never logged, redacted in [toString] and in the actuator's `configprops` and `env`. Prefer [clientSecretFile]. */
        val clientSecret: String? = null,
        /** File holding the client secret, trimmed at both ends: read at startup (an unreadable or empty file fails it) and again on every token fetch, so a rotated secret applies without a restart. Wins over [clientSecret]. */
        val clientSecretFile: Path? = null,
        /** `scope` of the token request, as sent (space-separated); none by default. */
        val scope: String? = null,
        /** PEM file with CA certificates to trust on top of the platform roots for an `https` [tokenUri] (not the hub's CA: that is `hub.tls.ca-file`). */
        val caFile: Path? = null,
    ) {
        /** The secret never appears: every other field as the data class prints it. */
        override fun toString(): String = "OAuth2(tokenUri=$tokenUri, clientId=$clientId, " +
            "clientSecret=${if (clientSecret.isNullOrEmpty()) clientSecret else "****"}, clientSecretFile=$clientSecretFile, scope=$scope, caFile=$caFile)"
    }

    /** `hub.retry.*`: validated by [RetryPolicy]'s rules at startup, also while [enabled] is false. */
    data class Retry(
        /** Retry failed sends that are safe to retry; off by default. */
        val enabled: Boolean = false,
        /** Total attempts including the first (>= 1; 1 = no retry). */
        val maxAttempts: Int = 3,
        /** Wait before the second attempt (not negative). */
        val initialBackoff: Duration = Duration.ofMillis(100),
        /** Growth of the wait per further attempt (>= 1). */
        val multiplier: Double = 2.0,
        /** Upper bound of a single wait (not negative). */
        val maxBackoff: Duration = Duration.ofSeconds(2),
    ) {
        init {
            // A bad value fails the startup now, not later when someone switches retry on.
            try {
                toPolicy()
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("snodra.messaging.hub.retry: ${e.message}", e)
            }
        }

        /** The configured policy, or null when retry is disabled. */
        fun policy(): RetryPolicy? = if (enabled) toPolicy() else null

        private fun toPolicy() = RetryPolicy(maxAttempts, initialBackoff, multiplier, maxBackoff)
    }

    data class Backoff(
        val min: Duration = Duration.ofMillis(500),
        val max: Duration = Duration.ofSeconds(5),
    )

    /** `snodra.messaging.store.*`. */
    data class Store(
        /**
         * The message store's name (`mongo`, `jdbc`, `memory`): which one runs when two are available, and the only way to run the
         * in-memory one (`memory`, never chosen otherwise). Unset: the only one there is.
         */
        val type: String? = null,
        /** Apply the message store's schema (indexes, tables) at startup; false when the service applies it itself. */
        val initSchema: Boolean = true,
    )

    /** The in-memory event store's settings (`snodra.messaging.memory.*`). */
    data class Memory(
        /** How many segments every source has (1 to 1024): a key always goes to the same one; competing instances share them. */
        val segments: Int = MemoryEventStoreSettings.DEFAULT_SEGMENTS,
    ) {
        init {
            require(segments in 1..MemoryEventStoreSettings.MAX_SEGMENTS) {
                "snodra.messaging.memory.segments must be between 1 and ${MemoryEventStoreSettings.MAX_SEGMENTS}, was $segments"
            }
        }
    }

    /** `events.*`: the service's events on the event store (ADR 0027), whichever add-on provides it. */
    data class Events(
        /** The source events are published to and read from (a topic on Kafka). */
        val source: String = "kraken.events",
        /**
         * The prefix of every event processor's subscription name; unset: the service name. Every processor, `default` included,
         * uses `<this prefix>.<name>` (ADR 0022): on Kafka the consumer group. Changing it on a running deployment starts the
         * subscriptions from the earliest position.
         */
        val subscriptionPrefix: String? = null,
        val consumer: Consumer = Consumer(),
        val deadLetter: DeadLetter = DeadLetter(),
    ) {
        init {
            require(source.isNotBlank()) { "snodra.messaging.events.source must not be blank" }
            require(subscriptionPrefix == null || subscriptionPrefix.isNotBlank()) { "snodra.messaging.events.subscription-prefix must not be blank" }
        }

        /** The prefix of every processor's subscription: [subscriptionPrefix], else [serviceName]. */
        fun subscriptionPrefix(serviceName: String): String = subscriptionPrefix ?: serviceName
    }

    /** `events.dead-letter.*` (P6a, ADR 0022): what an event processor does with a record it cannot handle. */
    data class DeadLetter(
        /**
         * `collection` (park in the message store's dead letters, the default), `event-store` (publish to the event store's
         * `<source>.dlt`; needs the capability `native-dead-letter`) or `log` (log and skip: nothing is parked).
         */
        val mode: DeadLetterMode = DeadLetterMode.COLLECTION,
        /** A letter is deleted this long after it was parked (TTL index on `expireAt`), which also unblocks its key; at least 1 s. */
        val retention: Duration = Duration.ofDays(30),
    ) {
        init {
            require(retention >= Duration.ofSeconds(1)) { "snodra.messaging.events.dead-letter.retention must be at least 1s, was $retention" }
        }
    }

    data class Consumer(
        /** Set false in tools that only publish. */
        val enabled: Boolean = true,
        /** Handler invocations per record (first try included) before the record goes to the dead letters (`events.dead-letter.mode`). */
        val maxAttempts: Int = 5,
    )

    data class Outbox(
        /** Run the outbox publisher that relays the outbox entries to the event store. */
        val enabled: Boolean = true,
        val pollInterval: Duration = Duration.ofMillis(200),
        /** Documents per poll. */
        val batchSize: Int = 100,
        /** Sent documents older than this are deleted (TTL index). */
        val retention: Duration = Duration.ofDays(7),
        /** Send attempts per document before it is marked failed (`failedAt`) and skipped. */
        val maxAttempts: Int = 10,
        /**
         * How long a batch the relay claimed stays its own: another instance relays it only after that (its claimer crashed).
         * Longer than one send can take (45 s).
         */
        val lease: Duration = Duration.ofMinutes(2),
    ) {
        init {
            require(lease > OutboxPublisher.SEND_TIMEOUT) { "snodra.messaging.outbox.lease must be longer than a send (${OutboxPublisher.SEND_TIMEOUT}), was $lease" }
        }
    }

    data class Idempotency(
        /** `processed_events` documents older than this are deleted (TTL index). */
        val retention: Duration = Duration.ofDays(30),
    )

    data class Security(
        /** Put the current request's bearer token in the hub request's `authorization` header. */
        val forwardJwt: Boolean = true,
    )

    /**
     * `sagas.*` (S1 plan, R4 and R8). The saga runtime, the dispatch relay and the deadline scheduler exist only when the
     * application has a `@Saga` class and [enabled] is true. Every duration must be positive; checked at binding, so a bad
     * value fails the startup also in a service with no saga.
     */
    data class Sagas(
        /** Switch for the saga runtime, the dispatch relay and the deadline scheduler; they exist only with a `@Saga` class anyway. */
        val enabled: Boolean = true,
        /** Attempts per saga event (first try included) for version conflicts, start-key duplicates and handler exceptions before the event is parked in `saga_failures` (>= 1). */
        val maxAttempts: Int = 5,
        val relay: SagaRelay = SagaRelay(),
        /** Ended sagas are deleted this long after they ended (TTL index on `sagas.endedAt`). */
        val endedRetention: Duration = Duration.ofDays(30),
        /** Dispatched `saga_dispatch` entries are deleted this long after they were sent (TTL index). */
        val dispatchRetention: Duration = Duration.ofDays(7),
        /**
         * What failed for good is deleted this long after it failed: `saga_dispatch` entries given up on (TTL index on `failedAt`)
         * and parked events (`saga_failures`, TTL index on `lastFailedAt`, which a failed retry moves on).
         */
        val failedRetention: Duration = Duration.ofDays(30),
    ) {
        init {
            require(maxAttempts >= 1) { "snodra.messaging.sagas.max-attempts must be >= 1, was $maxAttempts" }
            requirePositive("snodra.messaging.sagas.ended-retention", endedRetention)
            requirePositive("snodra.messaging.sagas.dispatch-retention", dispatchRetention)
            requirePositive("snodra.messaging.sagas.failed-retention", failedRetention)
        }
    }

    /** `sagas.relay.*`: the after-commit relay that sends what sagas recorded in `saga_dispatch`, in order per saga. */
    data class SagaRelay(
        /** How often the relay looks for due entries. */
        val pollInterval: Duration = Duration.ofMillis(500),
        /** How long a relay instance holds one saga's dispatch order before another may take over (a crashed instance's lease expires). */
        val lease: Duration = Duration.ofSeconds(30),
        /** Attempts per command for transient failures (first try included, >= 1) before it becomes a `SagaCommandFailed(EXHAUSTED)`. */
        val maxAttempts: Int = 10,
        /** Wait before the second attempt of a command; doubles per attempt up to [maxBackoff]. */
        val initialBackoff: Duration = Duration.ofSeconds(1),
        /** Upper bound of one wait between attempts. */
        val maxBackoff: Duration = Duration.ofMinutes(5),
        /**
         * How long a command send may take: one with no answer by then (a hung local adapter) is abandoned and retried as a transient
         * failure. Through the hub never less than the hub's deadline for the command plus 10 s.
         */
        val sendTimeout: Duration = Duration.ofMinutes(1),
    ) {
        init {
            try {
                toSettings()
            } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException("snodra.messaging.sagas.relay: ${e.message}", e)
            }
        }

        fun toSettings() = RelaySettings(pollInterval, lease, maxAttempts, initialBackoff, 2.0, maxBackoff, sendTimeout = sendTimeout)
    }

    /** `deadlines.*`: the scheduler that fires due saga deadlines, at least once. */
    data class Deadlines(
        /** How often the scheduler looks for due deadlines. */
        val pollInterval: Duration = Duration.ofSeconds(1),
        /** How long a claimed deadline stays reserved for the instance that claimed it; an expired claim lets it fire again. */
        val lease: Duration = Duration.ofSeconds(30),
        /** Fired and cancelled deadlines are deleted this long after (TTL index on `deadlines.finishedAt`). */
        val retention: Duration = Duration.ofDays(7),
    ) {
        init {
            requirePositive("snodra.messaging.deadlines.poll-interval", pollInterval)
            requirePositive("snodra.messaging.deadlines.lease", lease)
            requirePositive("snodra.messaging.deadlines.retention", retention)
        }

        fun toSettings() = DeadlineSettings(pollInterval, lease)
    }

    /** `external.*`: the integration toolkit (S1 plan, Tasks 9, 10 and 13). */
    data class External(
        val calls: ExternalCalls = ExternalCalls(),
    )

    /** `external.calls.*`: the `external_calls` collection behind `@IdempotentHandler`. */
    data class ExternalCalls(
        /**
         * Whether the idempotency interceptor and the `external_calls` indexes exist. Unset (the default): they exist when a class in
         * the application's packages has an `@IdempotentHandler` method. Set it to true when the adapter lives outside those packages.
         */
        val enabled: Boolean? = null,
        /** How long an `IN_PROGRESS` call may take before its outcome counts as unknown (a crash in the middle of a call). */
        val lease: Duration = Duration.ofMinutes(5),
        /** `external_calls` documents older than this are deleted (TTL index on `updatedAt`). */
        val retention: Duration = Duration.ofDays(30),
    ) {
        init {
            requirePositive("snodra.messaging.external.calls.lease", lease)
            requirePositive("snodra.messaging.external.calls.retention", retention)
        }
    }
}

private fun requirePositive(key: String, value: Duration) {
    require(!value.isZero && !value.isNegative) { "$key must be positive, was $value" }
}
