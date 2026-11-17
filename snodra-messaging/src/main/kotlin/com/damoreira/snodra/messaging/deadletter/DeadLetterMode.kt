package com.damoreira.snodra.messaging.deadletter

/** What an event processor does with a record it cannot handle (`snodra.messaging.events.dead-letter.mode`, ADR 0022). */
enum class DeadLetterMode(val wireName: String) {
    /** Park it in the `dead_letters` collection, sequence its key's later records behind it, retry or discard it with [DeadLetters]. */
    COLLECTION("collection"),

    /**
     * Publish it to the event store's dead-letter source (`<source>.dlt`) with `snodra-dlt-*` headers; it belongs to the operator's
     * tooling. Needs an event store with the capability `native-dead-letter`.
     */
    EVENT_STORE("event-store"),

    /** Log it (never the payload) and skip it: nothing is parked. */
    LOG("log"),
}
