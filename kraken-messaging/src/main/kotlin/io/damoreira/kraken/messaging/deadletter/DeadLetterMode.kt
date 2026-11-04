package io.damoreira.kraken.messaging.deadletter

/** What an event processor does with a record it cannot handle (`kraken.messaging.kafka.dead-letter.mode`, ADR 0022). */
enum class DeadLetterMode(val wireName: String) {
    /** Park it in the `dead_letters` collection, sequence its key's later records behind it, retry or discard it with [DeadLetters]. */
    COLLECTION("collection"),

    /** Produce it to `<topic>.dlt` with `kraken-dlt-*` headers; the topic belongs to the operator's tooling. */
    TOPIC("topic"),

    /** Log it (never the payload) and skip it: nothing is parked. */
    LOG("log"),
}
