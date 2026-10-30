package io.damoreira.kraken.examples.api

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import java.time.Instant

/*
 * The example "greetings" domain shared by service-a (caller) and service-b (owner).
 * Every wire class carries an explicit @MessageType so the name does not depend on either service's type-prefix.
 */

/** Command X: owned by service-b. */
@MessageType("examples.SendGreeting")
data class SendGreeting(val name: String) : Command<GreetingAccepted>

/** Reply to [SendGreeting]. */
data class GreetingAccepted(val id: String, val name: String)

/** Event Y: published by service-b while handling [SendGreeting]; service-a reacts to it. */
@MessageType("examples.GreetingDelivered")
data class GreetingDelivered(val id: String, val name: String, val at: Instant) : Event

/** Query R: owned by service-b. */
@MessageType("examples.CountGreetings")
data class CountGreetings(val name: String) : Query<GreetingCount>

/** Reply to [CountGreetings]. */
data class GreetingCount(val name: String, val count: Long)
