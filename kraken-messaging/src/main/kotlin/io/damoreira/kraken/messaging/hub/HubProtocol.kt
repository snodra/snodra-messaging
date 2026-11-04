package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.RegisterAck
import java.time.Duration
import java.util.Properties

/** What this library announces in `Register` and the client-side rules that depend on what the hub answers. */
object HubProtocol {
    /** `Request.initial_credit` the hub assumes when a sender sends 0; the library always sends at least 1. */
    const val DEFAULT_INITIAL_CREDIT = 64

    /** Default of `kraken.messaging.hub.stream-credit-timeout`: the hub's default maximum stream duration. */
    val DEFAULT_STREAM_CREDIT_TIMEOUT: Duration = Duration.ofMinutes(10)

    /** Default period of `InstanceStatus` reports (`kraken.messaging.hub.status-interval`). */
    val STATUS_INTERVAL: Duration = Duration.ofSeconds(5)

    /** How often a `dns:///` hub address is resolved again. */
    val DNS_REFRESH: Duration = Duration.ofSeconds(30)

    /** How often the client certificate files are read (P5c): the heartbeat period kraken-hub announces. */
    val CERTIFICATE_CHECK_INTERVAL: Duration = Duration.ofSeconds(3)

    /** The longest a rollover waits for the work in flight on a stream before it closes it (P5c; shared with the Rust library). */
    val ROLLOVER_DRAIN_TIMEOUT: Duration = Duration.ofSeconds(10)

    /** The longest wait, in [0, this], before a rollover starts after a renewed pair was seen (P5c, spreads the replicas of a service; shared with the Rust library). */
    val ROLLOVER_MAX_DELAY: Duration = Duration.ofSeconds(30)

    /** The longest a rollover waits for the new stream to register (P5c). */
    val ROLLOVER_RECONNECT_TIMEOUT: Duration = Duration.ofSeconds(30)

    /**
     * `Register.initial_permits` for one hub when the instance is connected to [hubs] hubs: `ceil(max / hubs)`, the
     * whole budget when [hubs] is 0 or 1. Permits granted this way are never revoked when the number of hubs changes.
     */
    fun initialPermits(maxConcurrentHandlers: Int, hubs: Int): Int {
        val n = hubs.coerceAtLeast(1)
        return (maxConcurrentHandlers + n - 1) / n
    }

    /**
     * Returned permits go back in one `Permits` message as soon as this many are pending, and at once (whatever is
     * pending) when the hub holds at most `min(PERMIT_BATCH, initial_permits / 2)` permits for the stream
     * ([PermitLedger]) ...
     */
    const val PERMIT_BATCH = 16

    /** ... otherwise at the latest this long after the first unsent grant. */
    val PERMIT_FLUSH_DELAY: Duration = Duration.ofMillis(50)

    /** `Register.client_version`, for example `kraken-messaging/0.1.0`. */
    val CLIENT_VERSION: String = "kraken-messaging/" + (libraryVersion() ?: "unknown")

    /**
     * How long a request waits for a live stream when `request-timeout` is unset, and the hub's deadline assumed before any
     * `RegisterAck` (the retry budget of a request without a timeout).
     */
    val DEFAULT_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(5)

    /** How long a new stream waits for the hub's `RegisterAck` before it fails and the connection reconnects. */
    val HANDSHAKE_TIMEOUT: Duration = Duration.ofSeconds(10)

    /** The heartbeat period of a stream whose `RegisterAck` announces none (kraken-hub always does: 3 s by default). */
    val DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration.ofSeconds(3)

    /** Added to the hub's own deadline: the local guard only fires when the hub never answers at all. */
    val LOCAL_GUARD_MARGIN: Duration = Duration.ofSeconds(1)

    /** Used when an ack carries no `max_timeout_ms` (kraken-hub always sends one). */
    private const val FALLBACK_MAX_TIMEOUT_MS = 60_000L

    /**
     * Upper bound of the hub's deadline for a request of [type] sent with [timeoutMs], by the hub's rule: `timeout_ms > 0`
     * -> `min(timeout_ms, override or max)`, `timeout_ms == 0` -> `override or max` (the hub uses `override or default`
     * there; the guard deliberately stays at the looser `max` so it never pre-empts the hub's own TIMEOUT).
     */
    fun hubDeadlineBoundMs(timeoutMs: Int, type: String, ack: RegisterAck): Long {
        val override = ack.typeTimeoutsMsMap[type]?.toLong()?.takeIf { it > 0 }
        val max = ack.maxTimeoutMs.toLong().takeIf { it > 0 } ?: FALLBACK_MAX_TIMEOUT_MS
        val cap = override ?: max
        return if (timeoutMs > 0) minOf(timeoutMs.toLong(), cap) else cap
    }

    /** How long the client waits for an answer before it gives up locally with `MessageTimeoutException`. */
    fun localGuardMs(timeoutMs: Int, type: String, ack: RegisterAck): Long =
        hubDeadlineBoundMs(timeoutMs, type, ack) + LOCAL_GUARD_MARGIN.toMillis()

    private fun libraryVersion(): String? = runCatching {
        HubProtocol::class.java.getResourceAsStream("/META-INF/kraken-messaging/version.properties")?.use { input ->
            Properties().apply { load(input) }.getProperty("version")?.takeIf { it.isNotBlank() && !it.startsWith("\${") }
        }
    }.getOrNull()
}
