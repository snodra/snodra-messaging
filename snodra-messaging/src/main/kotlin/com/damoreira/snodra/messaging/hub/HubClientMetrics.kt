package com.damoreira.snodra.messaging.hub

import io.micrometer.core.instrument.FunctionCounter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder

/**
 * The meters of the hub connections, bound to a Micrometer registry when there is one (Boot binds every [MeterBinder] bean).
 * As Prometheus shows them (P9, ADR 0026; the Rust library exports the same series):
 *
 * - `snodra_hub_client_connected_hubs`: the live hubs (acknowledged stream, not rolling over);
 * - `snodra_hub_client_handlers_in_flight`: incoming hub requests being handled or waiting for a handler permit, over all hubs;
 * - the credential counters (P5b, P5c): `snodra_hub_client_token_fetches_total{result}` (`ok`, `http_error`, `oauth_error`,
 *   `io_error`, `bad_response`; only with `hub.oauth2`), `snodra_hub_client_certificate_rotations_total` (only with a client
 *   certificate) and `snodra_hub_client_credentials_refreshes_sent_total`.
 *
 * Every value is read from memory when observed. Labels are fixed strings.
 */
class HubClientMetrics(private val connections: HubConnections) : MeterBinder {
    override fun bindTo(registry: MeterRegistry) {
        Gauge.builder("snodra.hub.client.connected.hubs", connections) { it.liveHubIds().size.toDouble() }
            .description("Hubs with an acknowledged stream")
            .register(registry)
        Gauge.builder("snodra.hub.client.handlers.in.flight", connections) { it.inFlightHandlers.value.toDouble() }
            .description("Incoming hub requests being handled or waiting for a handler permit")
            .register(registry)
        if (connections.authStatus().kind == HubAuthKind.JWT) {
            for (result in TokenFetchResult.entries) {
                FunctionCounter.builder("snodra.hub.client.token.fetches", connections) { it.authStatus().tokenFetches[result.label]?.toDouble() ?: 0.0 }
                    .tag("result", result.label)
                    .description("Access token fetches by result")
                    .register(registry)
            }
        }
        if (connections.authStatus().kind == HubAuthKind.CERTIFICATE) {
            FunctionCounter.builder("snodra.hub.client.certificate.rotations", connections) { it.authStatus().certificateRotations.toDouble() }
                .description("Client certificate pairs adopted from the files")
                .register(registry)
        }
        FunctionCounter.builder("snodra.hub.client.credentials.refreshes.sent", connections) { it.credentialsRefreshesSent() }
            .description("ClientMessage.credentials written to the hubs")
            .register(registry)
    }
}
