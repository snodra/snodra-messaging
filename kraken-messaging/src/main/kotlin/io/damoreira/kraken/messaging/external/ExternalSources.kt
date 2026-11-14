package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.events.EventConsumer
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.events.Resubscribe
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.eventstore.SegmentOrder
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.handler.Processors
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.ConcurrentHashMap

/**
 * Reads the partner sources ([ExternalSourceProvider], one per source name: on Kafka `kraken.messaging.external.kafka.sources.<name>`
 * with `kraken-messaging-kafka`), one subscription and one thread per source, and turns their records into local kraken events
 * (spec "Inbound 1", ADR 0027).
 *
 * Each record ([ExternalRecord]) goes through the source's [ExternalRecordTranslator] (the bean named like the source:
 * [translators] must hold one per source, else construction fails naming the source) and the resulting event is dispatched
 * **locally** through the [LocalEventDispatcher] (the ordinary `@EventHandler`s, then the sagas of this service); it is never
 * republished to the service's events. The dedupe id is `"<name>:<segment>:<position>"` (`"<name>:<partition>:<offset>"` on
 * Kafka, as always), or `"<name>:<dedupeKey>"` when the translator gives one; it keys the dispatcher's markers, so a record seen
 * twice (a redelivery, or the partner producing the same event twice under one key) is applied once. The handlers'
 * `MessageContext` has that id as `messageId` and in the `id` header, the source's name in `source`, and the record's own
 * headers; the correlation id is the record's `correlationId` header or the dedupe id.
 *
 * Delivery is the processors': acknowledgement after the dispatch returned, at-least-once; a translator or handler exception
 * retries the record up to [maxAttempts] invocations in all (every [backOffMs]), then logs it (never the payload) and skips it,
 * acknowledged. An [InvalidMessageException] skips it at once. A null translation is acknowledged without a marker. A
 * subscription whose `next` fails is closed and subscribed again after a backoff ([Resubscribe]).
 *
 * The local dispatch reaches the handlers of every event processor, each under its own marker `processor:<name>:<dedupeId>`
 * (`default` included); these records are never dead-lettered. The source names `processor`, `saga`, `ingress` and `failed` are
 * refused (construction fails naming the source; ADR 0021, ADR 0022): a dedupe id `failed:...` is the one of a failed notice,
 * and the other names stay free for the library's own marker families.
 *
 * A source whose reader an [Error] (not an exception) ended stays stopped ([stoppedSources], logged at error) until the sources
 * are stopped and started again (a restart): no automatic restart, which would retry a poison record without end.
 *
 * Starts and stops with the event processors (same lifecycle phase).
 */
class ExternalSources(
    private val sources: Map<String, ExternalSourceProvider>,
    private val translators: Map<String, ExternalRecordTranslator>,
    private val dispatcher: LocalEventDispatcher,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val maxAttempts: Int = 5,
    private val backOffMs: Long = 1000L,
    private val resubscribe: () -> Resubscribe = { Resubscribe() },
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(ExternalSources::class.java)
    private val readers = LinkedHashMap<String, Pair<Job, Thread>>()
    @Volatile private var running = false

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
        for (name in sources.keys) {
            // The dedupe id `<name>:...` must not look like a failed notice's (`failed:<id>`) or a marker family of the library.
            check(name !in Processors.RESERVED_SOURCE_NAMES) {
                "external source '$name': the source name is reserved (its markers would collide with " +
                    "the library's own marker families ${Processors.RESERVED_SOURCE_NAMES.sorted()}): choose another name"
            }
            check(translators[name] != null) {
                "external source '$name' has no ExternalRecordTranslator bean named '$name': define one " +
                    "(for example @Component(\"$name\") or a @Bean method called $name) that turns the partner's records into events"
            }
        }
    }

    /** Readers that outlived their [stop] (a handler still running); [start] waits for them rather than run a second reader. */
    private val lingering = ArrayList<Thread>()

    /** The sources whose reader an Error ended: they read nothing until the next [start]. */
    private val stopped = ConcurrentHashMap.newKeySet<String>()

    /** The sources whose reader an Error ended (logged at error): nothing reads them until the sources are started again. */
    fun stoppedSources(): Set<String> = stopped.toSet()

    @Synchronized
    override fun start() {
        if (running) return
        lingering.forEach { it.join(STOP_JOIN_MS) }
        lingering.removeIf { !it.isAlive }
        check(lingering.isEmpty()) { "external sources cannot start: a previous reader is still running a handler" }
        stopped.clear()
        for ((name, provider) in sources) {
            val job = Job()
            val thread = Thread({ read(name, provider, job) }, "kraken-external-$name").apply {
                isDaemon = true
                start()
            }
            readers[name] = job to thread
        }
        running = true
        log.info("External sources started: {}", sources.entries.joinToString { "${it.key} -> ${it.value.source} (${it.value.subscription})" })
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        readers.values.forEach { it.first.cancel() }
        readers.values.forEach { it.second.join(STOP_JOIN_MS) }
        lingering += readers.values.map { it.second }.filter { it.isAlive }
        readers.clear()
    }

    override fun isRunning(): Boolean = running
    override fun isAutoStartup(): Boolean = true
    override fun getPhase(): Int = EventConsumer.PHASE

    private fun read(name: String, provider: ExternalSourceProvider, job: Job) {
        val translator = translators.getValue(name)
        try {
            runBlocking(job) {
                resubscribe().run(
                    subscribe = { provider.subscribe() },
                    read = { sub -> readSubscription(name, provider, translator, sub) },
                    failed = { e, wait ->
                        log.warn("External source {}: the subscription {} failed, subscribing again in {}: {}", name, provider.subscription, wait, e.toString())
                    },
                )
            }
        } catch (_: CancellationException) {
            // stopped
        } catch (e: Throwable) {
            stopped += name
            log.error("External source {} stopped reading until the sources are started again: {}", name, e.toString(), e)
        }
    }

    private suspend fun readSubscription(
        name: String, provider: ExternalSourceProvider, translator: ExternalRecordTranslator, sub: Subscription,
    ): Resubscribe.Ended {
        var delivered = false
        while (true) {
            val event = try {
                sub.next()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                sub.takeError()?.let { log.warn("External source {}: {}", name, it) }
                return Resubscribe.Ended.Failed(e, delivered)
            }
            when (event) {
                is SubscriptionEvent.Assigned -> log.info("External source {}: segments {} of {} assigned", name, event.segments.sortedWith(SegmentOrder), provider.source)
                is SubscriptionEvent.Revoked -> log.info("External source {}: segments {} of {} revoked", name, event.segments.sortedWith(SegmentOrder), provider.source)
                is Delivery -> {
                    delivered = true
                    val record = ExternalRecord(provider.source, event.key, event.headers, event.payload, event.segment, event.position)
                    var attempts = 0
                    while (true) {
                        attempts++
                        val failure = try {
                            handle(name, translator, record)
                            null
                        } catch (e: Exception) {
                            e
                        } ?: break
                        if (failure is InvalidMessageException || attempts >= maxAttempts) {
                            log.error(
                                "Giving up on a record of external source {} at {}-{}@{} after {} attempts: {}: {}",
                                name, record.source, record.segment, record.position, attempts, failure::class.java.name, failure.message,
                            )
                            break
                        }
                        delay(backOffMs) // cancelled by a stop: not acknowledged, read again after the restart
                    }
                    sub.ack(event.segment, event.position)
                }
            }
            sub.takeError()?.let { log.warn("External source {}: {}", name, it) }
        }
    }

    private fun handle(name: String, translator: ExternalRecordTranslator, record: ExternalRecord) {
        val translated = translator.translate(record)
        if (translated == null) {
            log.debug("Source {} ignored record at {}-{}@{}", name, record.source, record.segment, record.position)
            return
        }
        val dedupeId = "$name:" + (translated.dedupeKey?.takeIf { it.isNotBlank() } ?: "${record.segment}:${record.position}")
        val type = typeResolver.typeNameOf(translated.event::class)
        val headers = record.headers + mapOf(EventHeaders.ID to dedupeId, EventHeaders.TYPE to type, EventHeaders.SOURCE to name)
        val ctx = MessageContext(
            messageId = dedupeId,
            type = type,
            correlationId = record.headers[EventHeaders.CORRELATION_ID]?.takeIf { it.isNotBlank() } ?: dedupeId,
            userId = null,
            roles = emptySet(),
            authorization = null,
            headers = headers,
        )
        dispatcher.dispatch(dedupeId, type, codec.encode(translated.event), ctx)
    }

    private companion object {
        const val STOP_JOIN_MS = 30_000L
    }
}
