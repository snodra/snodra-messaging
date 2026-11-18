package com.damoreira.snodra.messaging.external

/** Where an external call stands in `external_calls`; see [com.damoreira.snodra.messaging.store.ExternalCallRecord]. */
enum class ExternalCallStatus { IN_PROGRESS, SUCCEEDED, FAILED, UNKNOWN }
