package io.damoreira.kraken.messaging.external

/** Where an external call stands in `external_calls`; see [io.damoreira.kraken.messaging.store.ExternalCallRecord]. */
enum class ExternalCallStatus { IN_PROGRESS, SUCCEEDED, FAILED, UNKNOWN }
