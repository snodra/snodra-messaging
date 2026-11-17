package com.damoreira.snodra.examples.api

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.api.Query
import java.time.Instant

/*
 * The "texts" domain owned by service-c (Rust). Field names and wire types are mirrored in
 * snodra-messaging-rs/examples/service-c/src/messages.rs; keep both in step.
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
