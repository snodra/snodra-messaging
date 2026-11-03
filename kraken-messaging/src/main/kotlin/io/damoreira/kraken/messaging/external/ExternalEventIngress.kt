package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.kafka.EventHeaders
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.ProcessedEventDocument
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import java.time.Clock

/**
 * The webhook side of the integration toolkit (spec "Inbound 2"): a service exposes its own endpoint, verifies the request
 * with [WebhookSignatureVerifier], turns the body into an [Event] and calls [accept].
 *
 * [accept] runs the local dispatch **synchronously** on the calling thread, as `ExternalKafkaSources` does for a record: the
 * ordinary `@EventHandler`s of this service, then its sagas, each in the transactions of the [LocalEventDispatcher]; when it
 * returns they have committed. The dedupe id is `"<source>:<dedupeKey>"` and keys the dispatcher's markers, so the same event
 * is applied once however often the partner sends it. A handler failure is thrown (answer 5xx: the partner retries and the
 * retry is applied then), a call that returned is final.
 *
 * It returns [Result.DUPLICATE] when an earlier call with the same source and dedupe key already returned (an `ingress:<id>`
 * marker in `processed_events`, written after the dispatch succeeded), else [Result.ACCEPTED]. Two simultaneous deliveries of
 * one key may both dispatch; the markers make the second one a no-op and only one of them is told `ACCEPTED`. An event nobody
 * handles is `ACCEPTED` (logged at warn, no marker): the partner must not retry what this service will never use.
 *
 * Blocking: call it from a request thread, not from a coroutine that switches threads.
 */
class ExternalEventIngress(
    private val dispatcher: LocalEventDispatcher,
    private val mongo: MongoTemplate,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(ExternalEventIngress::class.java)

    enum class Result { ACCEPTED, DUPLICATE }

    /**
     * Dispatches [event] as the one identified by [source] and [dedupeKey] (the partner's own event or delivery id, stable
     * across its retries). [headers] are extra message headers for the handlers (a saga's `associationHeader` reads them).
     */
    @JvmOverloads
    fun accept(source: String, dedupeKey: String, event: Event, headers: Map<String, String> = emptyMap()): Result {
        require(source.isNotBlank()) { "source must not be blank" }
        require(dedupeKey.isNotBlank()) { "dedupeKey must not be blank" }
        val dedupeId = "$source:$dedupeKey"
        val marker = "$MARKER_PREFIX$dedupeId"
        if (mongo.exists(Query(Criteria.where("_id").`is`(marker)), ProcessedEventDocument::class.java)) return Result.DUPLICATE

        val type = typeResolver.typeNameOf(event::class)
        val ctx = MessageContext(
            messageId = dedupeId,
            type = type,
            correlationId = headers[EventHeaders.CORRELATION_ID]?.takeIf { it.isNotBlank() } ?: dedupeId,
            userId = null,
            roles = emptySet(),
            authorization = null,
            headers = headers + mapOf(EventHeaders.ID to dedupeId, EventHeaders.TYPE to type, EventHeaders.SOURCE to source),
        )
        val outcome = dispatcher.dispatch(dedupeId, type, codec.encode(event), ctx)
        if (outcome == LocalEventDispatcher.Outcome.NO_INTEREST) {
            log.warn("Webhook event {} of type {} from {} has no handler or saga in this service", dedupeKey, type, source)
            return Result.ACCEPTED
        }
        return try {
            mongo.insert(ProcessedEventDocument(marker, type, clock.instant()))
            Result.ACCEPTED
        } catch (_: DuplicateKeyException) {
            Result.DUPLICATE // a simultaneous delivery finished first; the dispatch above skipped everything by its markers
        }
    }

    companion object {
        /** Prefix of the marker id that remembers a finished call, next to the dispatcher's own markers. */
        const val MARKER_PREFIX = "ingress:"
    }
}
