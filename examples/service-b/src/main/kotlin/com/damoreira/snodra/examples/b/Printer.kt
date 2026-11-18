package com.damoreira.snodra.examples.b

import com.damoreira.snodra.messaging.external.ExternalCallUnknownOutcomeException
import com.damoreira.snodra.messaging.external.RetryableExternalException
import org.springframework.http.HttpStatusCode
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import java.net.ConnectException
import java.net.UnknownHostException
import java.net.http.HttpConnectTimeoutException

/** A job as the printer reports it: [done] once the postcard is printed. */
data class PrinterJob(val jobId: String, val name: String, val done: Boolean)

/** The external printer, as the adapter sees it. */
interface Printer {
    /** Submits the job under [idempotencyKey]: the printer prints once per key however often it is asked. */
    fun submit(jobId: String, name: String, idempotencyKey: String)

    /** The printer's view of the job, null when it does not know it. */
    fun status(jobId: String): PrinterJob?
}

/**
 * The printer over HTTP (`POST /jobs`, `GET /jobs/{id}`), classifying what goes wrong the way an adapter must:
 * nothing reached the printer (connection refused, connect timeout), a 5xx or a 429 are [RetryableExternalException] (the
 * saga relay backs off and calls again); a 4xx is permanent; a connection that broke **after** a submit went out leaves
 * the outcome unknown ([ExternalCallUnknownOutcomeException]: the printer may or may not have printed, so nobody calls it
 * again by itself and the saga is told `UNKNOWN_OUTCOME`). A read can always be repeated, so a failed [status] is retryable.
 */
class HttpPrinter(private val http: RestClient) : Printer {
    override fun submit(jobId: String, name: String, idempotencyKey: String) {
        try {
            http.post().uri("/jobs").header("Idempotency-Key", idempotencyKey).body(mapOf("jobId" to jobId, "name" to name)).retrieve().toBodilessEntity()
        } catch (e: HttpServerErrorException) {
            throw RetryableExternalException("The printer failed with ${e.statusCode.value()} submitting $jobId", e)
        } catch (e: HttpClientErrorException) {
            if (e.statusCode.value() == TOO_MANY_REQUESTS) throw RetryableExternalException("The printer is rate limiting (429) while submitting $jobId", e)
            throw IllegalStateException("The printer refused job $jobId with ${e.statusCode.value()}", e)
        } catch (e: ResourceAccessException) {
            if (neverSent(e)) throw RetryableExternalException("The printer could not be reached submitting $jobId: ${e.message}", e)
            throw ExternalCallUnknownOutcomeException("The connection to the printer broke after job $jobId was sent (${e.message}): it may or may not have printed")
        }
    }

    override fun status(jobId: String): PrinterJob? = try {
        http.get().uri("/jobs/{id}", jobId).retrieve().body(PrinterJobStatus::class.java)?.let { PrinterJob(it.jobId, it.name, it.status == "DONE") }
    } catch (e: HttpClientErrorException) {
        if (e.statusCode == HttpStatusCode.valueOf(NOT_FOUND)) null
        else if (e.statusCode.value() == TOO_MANY_REQUESTS) throw RetryableExternalException("The printer is rate limiting (429) polling $jobId", e)
        else throw IllegalStateException("The printer refused polling job $jobId with ${e.statusCode.value()}", e)
    } catch (e: HttpServerErrorException) {
        throw RetryableExternalException("The printer failed with ${e.statusCode.value()} polling $jobId", e)
    } catch (e: ResourceAccessException) {
        throw RetryableExternalException("The printer could not be read polling $jobId: ${e.message}", e)
    }

    private fun neverSent(e: ResourceAccessException) = generateSequence<Throwable>(e) { it.cause?.takeIf { c -> c !== it } }
        .any { it is ConnectException || it is HttpConnectTimeoutException || it is UnknownHostException }

    private data class PrinterJobStatus(val jobId: String, val name: String, val status: String)

    private companion object {
        const val TOO_MANY_REQUESTS = 429
        const val NOT_FOUND = 404
    }
}
