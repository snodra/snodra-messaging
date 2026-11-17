package com.damoreira.snodra.examples.b

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.deadletter.RetryAllResult
import com.damoreira.snodra.messaging.deadletter.RetryResult
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.time.Instant

/** Body of `POST /audit/notes`. */
data class AuditRequest(val key: String, val text: String)

/** One dead letter as the sandbox shows it: what it is, why it is parked and what it waits behind. */
data class LetterView(
    val id: String,
    val processor: String,
    val key: String?,
    val eventType: String?,
    val reason: String,
    val attempts: Int,
    val error: String,
    val blockedBy: String?,
)

/** What a retry did: the [result] (`Retried`, `Duplicate`, `Failed`, `Blocked`, `NotRetriable`, `NotFound`) and its [detail] (the failure, the blocking letter). */
data class RetryView(val result: String, val detail: String?)

/**
 * Example-only admin edge of service-b's `audit` processor and of the library's [DeadLetters]: publish a note, flip the
 * processor's failure switch (while on, notes whose text starts with `poison` fail), read what it handled, its resets and
 * what its `@DisallowReplay` notifier sent, and list, retry and discard dead letters. `curl -X POST 'localhost:8092/audit/failing?on=true'`, then `curl -X POST
 * localhost:8092/audit/notes -H 'content-type: application/json' -d '{"key":"k1","text":"poison-1"}'`, then
 * `curl localhost:8092/audit/dead-letters`. The sandbox is closed; a real service puts its own authorization in front of such
 * endpoints.
 */
@RestController
class AuditController(
    private val events: EventBus,
    private val audit: AuditHandler,
    private val notifier: AuditNotifier,
    private val deadLetters: DeadLetters,
) {
    @PostMapping("/audit/notes")
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun publish(@RequestBody request: AuditRequest): AuditNote {
        if (request.key.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "key must not be blank")
        val note = AuditNote(request.key, request.text, Instant.now())
        events.publishBlocking(note, key = request.key)
        return note
    }

    @PostMapping("/audit/failing")
    fun failing(@RequestParam on: Boolean): Map<String, Boolean> {
        audit.failing = on
        return mapOf("failing" to on)
    }

    @GetMapping("/audit/handled")
    fun handled(): List<AuditNote> = audit.handled

    /** The processor's resets that replayed something (P6b), as its `@ResetHandler` saw them. */
    @GetMapping("/audit/resets")
    fun resets(): List<AuditReset> = audit.resets

    /** What the `@DisallowReplay` notifier notified: each note once, a replay adds nothing. */
    @GetMapping("/audit/notified")
    fun notified(): List<AuditNote> = notifier.notified

    @GetMapping("/audit/dead-letters")
    fun list(@RequestParam(required = false) processor: String?, @RequestParam(defaultValue = "100") limit: Int): List<LetterView> =
        admin { deadLetters.list(processor, limit).map(::view) }

    @GetMapping("/audit/dead-letters/count")
    fun count(@RequestParam(required = false) processor: String?): Map<String, Long> =
        admin { mapOf("count" to deadLetters.count(processor)) }

    @PostMapping("/audit/dead-letters/{id}/retry")
    fun retry(@PathVariable id: String): RetryView = admin {
        when (val result = deadLetters.retry(id)) {
            is RetryResult.Failed -> RetryView("Failed", result.message)
            is RetryResult.Blocked -> RetryView("Blocked", result.headId)
            else -> RetryView(result.toString(), null)
        }
    }

    @PostMapping("/audit/dead-letters/retry-all")
    fun retryAll(@RequestParam(defaultValue = "audit") processor: String): RetryAllResult = admin { deadLetters.retryAll(processor) }

    @DeleteMapping("/audit/dead-letters/{id}")
    fun discard(@PathVariable id: String) {
        if (!admin { deadLetters.discard(id) }) throw ResponseStatusException(HttpStatus.NOT_FOUND, "no dead letter $id")
    }

    private fun view(letter: DeadLetter) = LetterView(
        letter.id, letter.processor, letter.key, letter.eventType, letter.reason.name, letter.attempts,
        "${letter.error.type}: ${letter.error.message}", letter.blockedBy,
    )

    /** A processor this instance does not run is the caller's mistake (400); a mode without letters is a conflict (409). */
    private fun <T> admin(call: () -> T): T = try {
        call()
    } catch (e: IllegalArgumentException) {
        throw ResponseStatusException(HttpStatus.BAD_REQUEST, e.message)
    } catch (e: IllegalStateException) {
        throw ResponseStatusException(HttpStatus.CONFLICT, e.message)
    }
}
