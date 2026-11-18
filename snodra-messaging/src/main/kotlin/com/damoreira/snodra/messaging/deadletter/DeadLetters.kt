package com.damoreira.snodra.messaging.deadletter

import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.events.RecordPosition
import com.damoreira.snodra.messaging.store.DeadLetterStore
import com.damoreira.snodra.messaging.store.DeadLetterStore.Summary
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

/**
 * The admin API of the dead letters (ADR 0022), the bean a service exposes on its own admin endpoints. It works on any
 * instance of the service, but only for the processors this instance runs (it has a handler in it; `default` also when
 * sagas run): a call naming another processor, or a letter of one, throws [IllegalArgumentException] naming it. In modes
 * `event-store` and `log` there are no letters and every call throws [IllegalStateException] naming the mode.
 *
 * Every call is blocking (the message store, and for a retry the handlers via `runBlocking` on the calling thread, like the consumer):
 * call it from a request thread, not from a coroutine that must not block.
 *
 * - [retry] runs the letter's processor's stage 1 (all its handlers for the type, under the processor's marker) once, **in the
 *   marker transaction together with the letter's deletion**: handled and deleted commit together or not at all. Stage 2 (the
 *   sagas) is not run again: it ran when the record was consumed.
 * - [retryAll] retries a processor's letters per key in position order, stopping a key at its first failure; then the keyless ones.
 * - [discard] deletes a letter; discarding a key's head lets the next letter of the key be retried.
 *
 * A key stays blocked for new records while any of its letters is parked (retry or discard them all to drain it); a letter that
 * expires (`snodra.messaging.events.dead-letter.retention`) unblocks its key.
 */
class DeadLetters(
    private val mode: DeadLetterMode,
    private val store: DeadLetterStore?,
    private val dispatcher: LocalEventDispatcher,
) {
    private val log = LoggerFactory.getLogger(DeadLetters::class.java)

    /** Letters sorted by `parkedAt`, then `_id`, of [processor] or of every processor; at most [limit] (>= 1). */
    @JvmOverloads
    fun list(processor: String? = null, limit: Int = 100): List<DeadLetter> {
        val store = available()
        require(limit >= 1) { "limit must be >= 1, was $limit" }
        processor?.let(::requireRuns)
        return runBlocking { store.list(processor, limit) }
    }

    /** The letter [id], or null. */
    fun get(id: String): DeadLetter? {
        val letter = runBlocking { available().get(id) } ?: return null
        requireRuns(letter.processor)
        return letter
    }

    /** How many letters [processor] (or every processor) has. */
    @JvmOverloads
    fun count(processor: String? = null): Long {
        val store = available()
        processor?.let(::requireRuns)
        return runBlocking { store.count(processor) }
    }

    /** Retries the letter [id] once; see [RetryResult] and the class comment. */
    fun retry(id: String): RetryResult {
        val store = available()
        val letter = runBlocking { store.get(id) } ?: return RetryResult.NotFound
        requireRuns(letter.processor)
        return retry(store, letter)
    }

    /**
     * Retries every letter of [processor]: per key in position order, as the store orders them (keys in the order of their head's `parkedAt`, `_id`), a key
     * stopping at its first failure (its later letters count as blocked); then the keyless letters by `parkedAt`, `_id`.
     * INVALID letters, and letters gone meanwhile, are skipped.
     */
    fun retryAll(processor: String): RetryAllResult {
        val store = available()
        requireRuns(processor)
        val all = runBlocking { store.summaries(processor) }
        var retried = 0
        var failed = 0
        var blocked = 0
        var skipped = all.count { it.reason == DeadLetterReason.INVALID }
        val order = compareBy<Summary>({ it.parkedAt }, { it.id })
        val retriable = all.filter { it.reason != DeadLetterReason.INVALID }
        // The store lists them by position (its order), then id: a key's letters keep that order.
        val keys = retriable.filter { it.key != null }.groupBy { it.key!! }.values
            .sortedWith { a, b -> order.compare(a.first(), b.first()) }
        val keyless = retriable.filter { it.key == null }.sortedWith(order).map { listOf(it) }
        for (group in keys + keyless) {
            var stopped = false
            for (summary in group) {
                if (stopped) { blocked++; continue }
                val letter = runBlocking { store.get(summary.id) }
                if (letter == null) { skipped++; continue }
                when (retry(store, letter)) {
                    RetryResult.Retried, RetryResult.Duplicate -> retried++
                    is RetryResult.Failed -> { failed++; stopped = true }
                    is RetryResult.Blocked -> { blocked++; stopped = true }
                    RetryResult.NotRetriable, RetryResult.NotFound -> skipped++
                }
            }
        }
        log.info("Retried the dead letters of processor {}: {} retried, {} failed, {} blocked, {} skipped", processor, retried, failed, blocked, skipped)
        return RetryAllResult(retried, failed, blocked, skipped)
    }

    /** Deletes the letter [id]; false when there was none. */
    fun discard(id: String): Boolean {
        val store = available()
        val letter = runBlocking { store.get(id) } ?: return false
        requireRuns(letter.processor)
        return runBlocking { store.delete(id) }.also { if (it) log.info("Discarded dead letter {} ({} of type {})", id, letter.reason, letter.eventType) }
    }

    private fun retry(store: DeadLetterStore, letter: DeadLetter): RetryResult {
        if (letter.reason == DeadLetterReason.INVALID) return RetryResult.NotRetriable
        letter.key?.let { key -> runBlocking { store.earlier(letter.processor, key, letter.position) }?.let { return RetryResult.Blocked(it) } }
        val type = letter.eventType ?: return RetryResult.NotRetriable // only INVALID letters lack a type
        val dedupeId = letter.eventId?.takeIf { it.isNotBlank() } ?: "${letter.source}-${letter.segment}-${letter.position}"
        val ctx = MessageContext(
            messageId = dedupeId,
            type = type,
            correlationId = letter.headers[EventHeaders.CORRELATION_ID] ?: dedupeId,
            userId = letter.headers[EventHeaders.USER_ID]?.takeIf { it.isNotBlank() },
            roles = emptySet(),
            authorization = null,
            headers = letter.headers,
        )
        return try {
            val position = RecordPosition(letter.source, letter.segment, letter.position) // it is that record's stage 1 (P6b)
            // A letter parked during a replay is retried as one: without its @DisallowReplay handlers (P6b).
            val outcome = dispatcher.runStage1(letter.processor, dedupeId, type, letter.value(), ctx, position, letter.replay) {
                if (!runBlocking { store.deleteInTransaction(letter.id) }) throw Gone // retried or discarded meanwhile: roll the handling back
            }
            when (outcome) {
                LocalEventDispatcher.Stage1.HANDLED -> RetryResult.Retried.also { log.info("Retried dead letter {}", letter.id) }
                LocalEventDispatcher.Stage1.DUPLICATE -> {
                    runBlocking { store.delete(letter.id) }
                    RetryResult.Duplicate.also { log.info("Dead letter {} was already handled; deleted it", letter.id) }
                }
                LocalEventDispatcher.Stage1.NO_HANDLER ->
                    failed(store, letter, DeadLetterError("NoHandler", "no handler for $type in processor ${letter.processor}"))
            }
        } catch (_: Gone) {
            RetryResult.NotFound
        } catch (e: Exception) {
            failed(store, letter, DeadLetterError.of(e))
        }
    }

    private fun failed(store: DeadLetterStore, letter: DeadLetter, error: DeadLetterError): RetryResult {
        if (!runBlocking { store.recordFailure(letter.id, error) }) return RetryResult.NotFound
        log.warn("Retry of dead letter {} failed: {}: {}", letter.id, error.type, error.message)
        return RetryResult.Failed(error.message)
    }

    private fun available(): DeadLetterStore {
        check(mode == DeadLetterMode.COLLECTION && store != null) {
            "dead letters are not kept in mode ${mode.wireName} (snodra.messaging.events.dead-letter.mode): there is nothing to list, retry or discard"
        }
        return store
    }

    private fun requireRuns(processor: String) {
        require(dispatcher.runs(processor)) {
            "processor $processor does not run in this instance (no @EventHandler in it): use an instance that runs it"
        }
    }

    /** The letter was gone when the retry's transaction came to delete it. */
    private object Gone : RuntimeException("dead letter gone", null, false, false) {
        private fun readResolve(): Any = Gone
    }
}
