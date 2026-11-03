package io.damoreira.kraken.messaging.hub

import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder

/**
 * The counters of the hub credential (P5b), bound to a Micrometer registry when there is one (Boot binds every [MeterBinder] bean).
 * As Prometheus shows them: `kraken_messaging_hub_token_fetches_total{result}` (`ok`, `http_error`, `oauth_error`, `io_error`,
 * `bad_response`; only with `hub.oauth2`), `kraken_messaging_hub_certificate_rotations_total` (only with a client certificate, P5c) and
 * `kraken_messaging_hub_credentials_refreshes_sent_total`. Labels are fixed strings.
 */
class HubAuthMetrics(private val connections: HubConnections) : MeterBinder {
    override fun bindTo(registry: MeterRegistry) {
        if (connections.authStatus().kind == HubAuthKind.JWT) {
            for (result in TokenFetchResult.entries) {
                FunctionCounter.builder("kraken.messaging.hub.token.fetches", connections) { it.authStatus().tokenFetches[result.label]?.toDouble() ?: 0.0 }
                    .tag("result", result.label)
                    .description("Access token fetches by result")
                    .register(registry)
            }
        }
        if (connections.authStatus().kind == HubAuthKind.CERTIFICATE) {
            FunctionCounter.builder("kraken.messaging.hub.certificate.rotations", connections) { it.authStatus().certificateRotations.toDouble() }
                .description("Client certificate pairs adopted from the files")
                .register(registry)
        }
        FunctionCounter.builder("kraken.messaging.hub.credentials.refreshes.sent", connections) { it.credentialsRefreshesSent() }
            .description("ClientMessage.credentials written to the hubs")
            .register(registry)
    }
}
