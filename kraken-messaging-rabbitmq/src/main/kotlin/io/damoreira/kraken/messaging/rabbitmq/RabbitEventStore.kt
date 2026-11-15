package io.damoreira.kraken.messaging.rabbitmq

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException

/**
 * The event store on RabbitMQ Streams (ADR 0027), the add-on `kraken-messaging-rabbitmq`: RabbitMQ 3.13 or later with the stream
 * plugin, through the stream protocol (the RabbitMQ Stream Java client's `Client`).
 *
 * **The layout on the wire** is shared with the Rust add-on (`kraken-messaging-rabbitmq`, `rabbitmq-stream-client`): both read
 * and write the same streams the same way. These are rules:
 *
 * - **A source is a super stream** (`kraken.events`, partition streams `kraken.events-0` to `-N`), created by the infrastructure,
 *   never by the library (`rabbitmq-streams add_super_stream kraken.events --partitions 6`); a source that is a plain stream (the
 *   dead-letter stream) is read and written as one segment `0`. A source that is neither fails the publish and the subscription,
 *   naming the command that creates it.
 * - **Routing** is the stream clients' hash routing: murmur3 x86_32 of the routing key's UTF-8 bytes, seed 104729, as an
 *   **unsigned** number modulo the partition count, over the partitions in the order the broker lists them. The routing key is
 *   the event's key; an event without one is routed by its id. With 6 partitions: `order-1` 0, `order-2` 2, `customer-123` 5,
 *   `order-7` 3, `""` 4.
 * - **A message** is one AMQP 1.0 message per event: the JSON payload as exactly one `data` section; `properties.message-id` the
 *   event id (a string); `properties.content-type` the symbol `application/json`; the key as the message annotation `x-routing-key`
 *   (a symbol key, a string value; absent without a key); every header an application property with a string value. Published one
 *   message per entry: **never a sub-entry batch, never compressed** (the Rust client cannot read them).
 * - **A segment** is the partition stream's suffix after `<source>-` (`3`), **a position** the offset in decimal, an event's time
 *   its chunk's timestamp.
 * - **A processor's subscription** `<service>.<processor>` is a consumer of that name on every partition stream, with
 *   `single-active-consumer=true`, `name=<service>.<processor>`, `super-stream=<source>` (absent on a plain stream) and
 *   `kraken-member=<a random id per subscription instance>` (the management API tells the instances apart by it).
 * - **Offset tracking**: the reference is the consumer name, per partition stream; **the stored value is the next offset to read**
 *   (the offset after the last acknowledged event); nothing stored means `first`. Stored every 10 000 acknowledgements or 5 s
 *   ([RabbitEventStoreSettings.offsetStoreMessages], [RabbitEventStoreSettings.offsetStoreInterval]), and always on deactivation and
 *   at close.
 * - **The native dead letter** is the plain stream `<source>.dlt`, created by the infrastructure.
 * - Connections are named `kraken-messaging`.
 *
 * **Capabilities**: [Capability.BROWSE] (the event browser can read the streams; it is a separate program), [Capability.LAG] and
 * [Capability.NATIVE_DEAD_LETTER] always; [Capability.REPLAY] only with the management API ([RabbitEventStoreSettings.management]):
 * a reset is refused while an instance reads the subscription, and only that API can tell. [open] asks it once.
 *
 * **Publish** returns once the broker confirmed the event (at most [RabbitEventStoreSettings.confirmTimeout], 30 s); a refusal for good (access refused, an event over the
 * 1 MiB frame) is [EventStoreException.Rejected], anything else (a missing stream included) [EventStoreException.Failed]. There is
 * no deduplication: an event the outbox publishes again after a crash is a duplicate, which the processed-event markers skip.
 *
 * **The client is pinned**: com.rabbitmq:stream-client 1.13.1 exactly, checked when the store is built ([RabbitClientCheck]); a
 * Spring Boot service sets `rabbit-stream-client.version=1.13.1` (Boot's dependency management sets another version).
 *
 * **Why the client's `Client`, not its `Environment`**: the `Environment`'s consumer builder cannot set a subscription property of
 * its own (`kraken-member`, a wire rule), and its automatic offset tracking follows the "last processed" convention. The `Client`
 * gives every protocol command; nothing of the client's tracking is used.
 */
class RabbitEventStore internal constructor(
    private val settings: RabbitEventStoreSettings,
    /** The management API's client, closed with the store; null: `replay` is not declared. */
    internal val management: RabbitManagement?,
) : EventStore, AutoCloseable {
    /** The store of [settings]; [replay]: whether `replay` is declared (with the management API, unless [open] found its metrics off). */
    constructor(settings: RabbitEventStoreSettings, replay: Boolean = settings.management != null) :
        this(settings, settings.management?.takeIf { replay }?.let { managementOf(settings, it) })

    init {
        RabbitClientCheck.verify()
    }

    private val connector = RabbitConnector(settings, CONNECTION_NAME)
    private val publisher = RabbitPublisher(connector, settings.confirmTimeout)
    private val live = RabbitLive()

    override val name: String = NAME

    override val capabilities: Set<Capability> =
        if (management != null) Capability.entries.toSet() else setOf(Capability.BROWSE, Capability.LAG, Capability.NATIVE_DEAD_LETTER)

    override suspend fun publish(event: OutgoingEvent) = publisher.publish(event)

    override suspend fun subscribe(spec: SubscriptionSpec): Subscription {
        if (spec.name.isBlank() || spec.source.isBlank()) throw EventStoreException.Invalid("a subscription needs a name and a source")
        val member = UUID.randomUUID().toString().replace("-", "")
        var built: RabbitSubscription? = null
        try {
            return withContext(Dispatchers.IO) {
                val leave = live.enter(spec, member)
                try {
                    RabbitSubscription.join(spec, connector, member, settings, leave).also { built = it }
                } catch (e: Exception) {
                    leave()
                    throw e
                }
            }
        } catch (e: CancellationException) {
            // Cancelled after the subscription was built (withContext then drops its result): it must not stay a consumer.
            built?.let { withContext(NonCancellable) { it.close() } }
            throw e
        }
    }

    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin {
        val api = management ?: throw EventStoreException.Unsupported(Capability.REPLAY)
        return RabbitSubscriptionAdmin(spec, connector, api, live)
    }

    /** Offsets are decimal numbers; a position that is none is [EventStoreException.Invalid]. */
    override fun compare(a: String, b: String): Int {
        val x = a.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$a\" is not an offset")
        val y = b.toLongOrNull() ?: throw EventStoreException.Invalid("position \"$b\" is not an offset")
        return x.compareTo(y)
    }

    /** Closes the publishers' connections (each subscription closes its own) and the management API's client. */
    override fun close() {
        try {
            publisher.close()
        } finally {
            management?.close()
        }
    }

    override fun toString(): String = "RabbitEventStore($settings, capabilities=$capabilities)"

    companion object {
        /** The name this event store reports to the hub. */
        const val NAME = "rabbitmq-streams"

        /** The name every connection of the store carries. */
        const val CONNECTION_NAME = "kraken-messaging"

        private val log = LoggerFactory.getLogger(RabbitEventStore::class.java)

        /**
         * The event store of [settings], having asked the management API (when configured) once: [EventStoreException.Failed] when it
         * does not answer (or answers what cannot be trusted); `replay` is not declared when its metrics collection is off
         * (`management.disable_stats`), since its consumer list is then empty whoever reads. Blocking.
         */
        fun open(settings: RabbitEventStoreSettings): RabbitEventStore {
            val api = settings.management?.let { managementOf(settings, it) } ?: return RabbitEventStore(settings)
            val stats = try {
                api.statsEnabled()
            } catch (e: Exception) {
                api.close()
                throw e
            }
            if (stats) return RabbitEventStore(settings, api)
            api.close()
            log.warn("The RabbitMQ management API collects no metrics (management.disable_stats): the event store does not declare replay")
            return RabbitEventStore(settings, null)
        }

        private fun managementOf(settings: RabbitEventStoreSettings, management: RabbitManagementSettings) =
            RabbitManagement(management, settings.username, settings.password, settings.virtualHost)
    }
}
