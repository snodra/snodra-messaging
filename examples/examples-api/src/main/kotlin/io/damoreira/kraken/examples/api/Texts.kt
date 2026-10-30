package io.damoreira.kraken.examples.api

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import java.time.Instant

/*
 * The "texts" domain owned by service-c (Rust). Field names and wire types are mirrored in
 * kraken-messaging-rs/examples/service-c/src/messages.rs; keep both in step.
 */

/** Command owned by service-c: upper-case the text. */
@MessageType("examples.Shout")
data class Shout(val text: String) : Command<Shouted>

data class Shouted(val text: String)

/** Query owned by service-c: reverse the text. */
@MessageType("examples.Reverse")
data class Reverse(val text: String) : Query<Reversed>

data class Reversed(val text: String)

/** Published by service-c while handling [Shout]. */
@MessageType("examples.TextShouted")
data class TextShouted(val text: String, val at: Instant) : Event

/** Published by service-c while handling [Reverse]. */
@MessageType("examples.TextReversed")
data class TextReversed(val text: String, val at: Instant) : Event
