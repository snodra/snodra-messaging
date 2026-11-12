package io.damoreira.kraken.examples.printer

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

/** The printer's HTTP API: `curl -X POST localhost:8093/jobs -H 'Idempotency-Key: k1' -H 'content-type: application/json' -d '{"jobId":"j1","name":"poll-ada"}'`. */
@RestController
class PrinterController(private val printer: FakePrinter) {
    @PostMapping("/jobs")
    fun submit(@RequestHeader("Idempotency-Key") key: String, @RequestBody job: JobRequest): SubmitResult = try {
        printer.submit(key, job)
    } catch (e: IllegalArgumentException) {
        throw ResponseStatusException(HttpStatus.BAD_REQUEST, e.message)
    }

    @GetMapping("/jobs/{jobId}")
    fun status(@PathVariable jobId: String): JobStatus =
        printer.status(jobId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "no job $jobId")
}
