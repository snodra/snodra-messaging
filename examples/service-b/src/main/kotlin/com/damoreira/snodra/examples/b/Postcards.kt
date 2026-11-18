package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.PostcardFailed
import com.damoreira.snodra.examples.api.PostcardPrinted
import com.damoreira.snodra.messaging.handler.EventHandler
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.util.concurrent.ConcurrentHashMap

/** How a postcard ended: [status] `PRINTED` or `FAILED` (with its [reason]). */
data class PostcardOutcome(val jobId: String, val status: String, val reason: String?)

/**
 * What the sandbox (the smoke test, a curl) reads to see how a postcard ended: an in-memory projection of the two events
 * the saga's story ends with, lost on restart like service-b's greeting counts.
 */
@Component
class PostcardOutcomes {
    private val outcomes = ConcurrentHashMap<String, PostcardOutcome>()

    @EventHandler
    fun on(e: PostcardPrinted) {
        outcomes[e.jobId] = PostcardOutcome(e.jobId, "PRINTED", null)
    }

    @EventHandler
    fun on(e: PostcardFailed) {
        outcomes[e.jobId] = PostcardOutcome(e.jobId, "FAILED", e.reason)
    }

    fun of(jobId: String): PostcardOutcome? = outcomes[jobId]
}

/** `GET /postcards/{jobId}`: the outcome, 404 while the postcard is still in flight (or unknown). */
@RestController
class PostcardController(private val outcomes: PostcardOutcomes) {
    @GetMapping("/postcards/{jobId}")
    fun outcome(@PathVariable jobId: String): PostcardOutcome =
        outcomes.of(jobId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "no outcome for postcard $jobId yet")
}
