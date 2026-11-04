package io.damoreira.kraken.examples.printer

import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * A print job as the service asks for it. [replyWebhook] and [replyTopic] say where this customer wants its notification
 * (service-c's banners); without them the printer answers its configured customer (service-b's postcards).
 */
data class JobRequest(val jobId: String, val name: String, val replyWebhook: String? = null, val replyTopic: String? = null)

/** The answer to a submit; [duplicate] when the idempotency key was already seen (nothing printed again). */
data class SubmitResult(val jobId: String, val status: String, val duplicate: Boolean)

/** A job as [FakePrinter.status] reports it: `PENDING`, then `DONE`. */
data class JobStatus(val jobId: String, val name: String, val status: String)

/** What the printer tells its customer when a postcard is printed (webhook body and topic record alike). */
data class Notification(val deliveryId: String, val jobId: String, val name: String)

/** How the printer reaches its customer: a signed webhook call to [url], or a record on [topic]; null means the configured default. */
interface Notifier {
    fun webhook(n: Notification, url: String?)
    fun topic(n: Notification, topic: String?)
}

/**
 * A fake postcard printer for the sandbox. What a job does depends on the greeting name's prefix:
 *
 * - `webhook-*` (and any name with no known prefix): a moment after accepting it, posts the notification to the customer's webhook
 *   (the job's `replyWebhook`, else the configured one);
 * - `topic-*`: produces the notification to the printer's topic (the job's `replyTopic`, else its own);
 * - `poll-*`: tells nobody; `GET /jobs/{id}` is `PENDING` for [pollDelay] after the job was accepted, then `DONE`;
 * - `silent-*`: accepts and never answers (the customer's timeout path);
 * - `crash-*`: accepts, then takes the process down before replying (the customer's unknown-outcome path; compose restarts it).
 *
 * A job submitted twice under the same idempotency key is accepted twice and printed once, as a real partner that
 * de-duplicates by the key it is given. [later] runs a task after a delay (the printing time); the other collaborators
 * are seams for the tests.
 */
class FakePrinter(
    private val clock: () -> Instant,
    private val notifier: Notifier,
    private val later: (Duration, () -> Unit) -> Unit,
    private val crash: () -> Unit,
    private val pollDelay: Duration = Duration.ofSeconds(5),
    private val printDelay: Duration = Duration.ofSeconds(1),
) {
    private val log = LoggerFactory.getLogger(FakePrinter::class.java)
    private val seen = ConcurrentHashMap.newKeySet<String>()
    private val jobs = ConcurrentHashMap<String, Accepted>()

    private class Accepted(val name: String, val at: Instant)

    fun submit(idempotencyKey: String, job: JobRequest): SubmitResult {
        if (!seen.add(idempotencyKey)) {
            log.info("Printer: job {} under key {} already accepted, not printing again", job.jobId, idempotencyKey)
            return SubmitResult(job.jobId, "ACCEPTED", duplicate = true)
        }
        jobs.putIfAbsent(job.jobId, Accepted(job.name, clock()))
        val notification = Notification("printed-${job.jobId}", job.jobId, job.name)
        when {
            job.name.startsWith("silent-") -> log.info("Printer: job {} accepted, going silent", job.jobId)
            job.name.startsWith("poll-") -> log.info("Printer: job {} accepted, done in {}", job.jobId, pollDelay)
            job.name.startsWith("crash-") -> {
                log.warn("Printer: job {} accepted, crashing before the reply", job.jobId)
                crash()
            }
            job.name.startsWith("topic-") -> later(printDelay) { notifier.topic(notification, job.replyTopic) }
            else -> later(printDelay) { notifier.webhook(notification, job.replyWebhook) }
        }
        return SubmitResult(job.jobId, "ACCEPTED", duplicate = false)
    }

    fun status(jobId: String): JobStatus? {
        val job = jobs[jobId] ?: return null
        val done = job.name.startsWith("poll-") && !clock().isBefore(job.at.plus(pollDelay))
        return JobStatus(jobId, job.name, if (done) "DONE" else "PENDING")
    }
}

/** `sha256=<hex>`: the HMAC-SHA256 of [body] keyed by [secret], as the customer's `X-Kraken-Signature` check wants it. */
fun sign(secret: String, body: ByteArray): String {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
    return "sha256=" + mac.doFinal(body).joinToString("") { "%02x".format(it) }
}
