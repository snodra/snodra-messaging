package io.damoreira.kraken.examples.api

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.SubscriptionQuery

/*
 * A shared subscription query type (ADR 0017, P4c): service-b (Kotlin) and service-c (Rust) both serve it with an
 * in-memory counter per name; a subscription gets the current value of one of them as its initial result and then the
 * changes of both, as they happen. Mirrored in kraken-messaging-rs/examples/service-c/src/messages.rs; keep both in step.
 */

/** Subscription query owned by service-b and service-c: watch the counter [name] (initial [Counter], updates [CounterChanged]). */
@MessageType("examples.WatchCounter")
data class WatchCounter(val name: String) : SubscriptionQuery<Counter, CounterChanged>

/** The initial result of [WatchCounter]: the answering [service] and [instance] (the container's host name) and the counter's current [value]. */
data class Counter(val service: String, val instance: String, val name: String, val value: Long)

/** An update of [WatchCounter]: the counter [name] of [service] / [instance] now has [value]. */
data class CounterChanged(val service: String, val instance: String, val name: String, val value: Long)

/**
 * Command owned by service-c (Rust): increments its counter [name] and emits the change to every subscription. [service]
 * says whose counter this is meant for (only service-c owns the command; service-b increments through its own
 * `POST /counters/{name}/increment`); a handler that is not that service refuses it.
 */
@MessageType("examples.IncrementCounter")
data class IncrementCounter(val name: String, val service: String) : Command<Counter>

/**
 * Shared query owned by service-b and service-c: how many subscriptions the answering instance serves now, whatever their
 * type. [scope] is reserved (always "all"). A scatter-gather over it is the sandbox's black-box view of the owners.
 */
@MessageType("examples.OpenSubscriptions")
data class OpenSubscriptions(val scope: String = "all") : Query<SubscriptionCount>

/** Reply to [OpenSubscriptions]: the answering [service] and [instance] and the subscriptions it [open]s now. */
data class SubscriptionCount(val service: String, val instance: String, val open: Long)
