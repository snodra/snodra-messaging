package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.RegisterAck
import java.time.Duration
import java.util.Properties

/**
 * What this library announces in `Register` (protocol v2, see ADR 0013 in the umbrella repo) and the client-side rules
 * that depend on what the hub answers.
 */
object HubProtocol {
    /** `Register.protocol_version`. 0 would be v1; a v2 hub answers with a `RegisterAck`, a v1 hub ignores the field. */
    const val PROTOCOL_VERSION = 2

    /** Capability name of hub flow control: permits, `Register.initial_permits`, `RESOURCE_EXHAUSTED` (P1). */
    const val FLOW_CONTROL = "flow-control"

    /** `Register.capabilities`: optional protocol features this client supports. */
    val CAPABILITIES: List<String> = listOf(FLOW_CONTROL)

    /** A `Permits` message is sent as soon as this many grants are pending ... */
    const val PERMIT_BATCH = 16

    /** ... or this long after the first unsent grant, whichever comes first. */
    val PERMIT_FLUSH_DELAY: Duration = Duration.ofMillis(50)

    /** `Register.client_version`, for example `kraken-messaging/0.1.0`. */
    val CLIENT_VERSION: String = "kraken-messaging/" + (libraryVersion() ?: "unknown")

    /** Request timeout used against a legacy hub when `request-timeout` is unset, and to wait for a stream. */
    val DEFAULT_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(5)

    /** Added to the hub's own deadline: the local guard only fires when the hub never answers at all. */
    val LOCAL_GUARD_MARGIN: Duration = Duration.ofSeconds(1)

    /** Used when a v2 ack carries no `max_timeout_ms` (never sent by kraken-hub). */
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
