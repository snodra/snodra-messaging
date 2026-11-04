package io.damoreira.kraken.examples.printer

import java.time.Duration
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A clock the test moves and a scheduler that runs what was scheduled when the test says so. */
private class Fixture(val pollDelay: Duration = Duration.ofSeconds(5)) {
    var now: Instant = Instant.parse("2026-11-03T10:00:00Z")
    val webhooks = mutableListOf<Notification>()
    val topic = mutableListOf<Notification>()
    val webhookUrls = mutableListOf<String?>()
    val topics = mutableListOf<String?>()
    var crashes = 0
    private val scheduled = mutableListOf<() -> Unit>()

    val printer = FakePrinter(
        clock = { now },
        notifier = object : Notifier {
            override fun webhook(n: Notification, url: String?) { webhooks += n; webhookUrls += url }
            override fun topic(n: Notification, topic: String?) { this@Fixture.topic += n; topics += topic }
        },
        later = { _, task -> scheduled += task },
        crash = { crashes++ },
        pollDelay = pollDelay,
    )

    fun runScheduled() {
        val tasks = scheduled.toList()
        scheduled.clear()
        tasks.forEach { it() }
    }
}

class FakePrinterTest {
    @Test fun `a webhook job notifies the webhook, later, with the delivery id printed-jobId`() {
        val f = Fixture()

        f.printer.submit("k1", JobRequest("g1", "webhook-ada"))
        assertTrue(f.webhooks.isEmpty(), "the printer takes a moment")
        f.runScheduled()

        assertEquals(listOf(Notification("printed-g1", "g1", "webhook-ada")), f.webhooks)
        assertTrue(f.topic.isEmpty())
    }

    @Test fun `without a reply target the printer answers its configured customer`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "webhook-ada"))
        f.printer.submit("k2", JobRequest("g2", "topic-ada"))
        f.runScheduled()

        assertEquals(listOf<String?>(null), f.webhookUrls)
        assertEquals(listOf<String?>(null), f.topics)
    }

    @Test fun `a job's reply target is where its notification goes`() {
        val f = Fixture()
        val webhook = "http://service-c:8080/printer/webhook"
        f.printer.submit("k1", JobRequest("banner-webhook-1", "webhook-1", replyWebhook = webhook, replyTopic = "printer.banner.events"))
        f.printer.submit("k2", JobRequest("banner-topic-1", "topic-1", replyWebhook = webhook, replyTopic = "printer.banner.events"))
        f.runScheduled()

        assertEquals(listOf(Notification("printed-banner-webhook-1", "banner-webhook-1", "webhook-1")), f.webhooks)
        assertEquals(listOf<String?>(webhook), f.webhookUrls)
        assertEquals(listOf(Notification("printed-banner-topic-1", "banner-topic-1", "topic-1")), f.topic)
        assertEquals(listOf<String?>("printer.banner.events"), f.topics)
    }

    @Test fun `a name with no known prefix is a webhook job`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "Ada"))
        f.runScheduled()

        assertEquals(1, f.webhooks.size)
    }

    @Test fun `a topic job produces to the printer topic`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "topic-ada"))
        f.runScheduled()

        assertEquals(listOf(Notification("printed-g1", "g1", "topic-ada")), f.topic)
        assertTrue(f.webhooks.isEmpty())
    }

    @Test fun `a poll job tells nobody and is pending until the poll delay has passed`() {
        val f = Fixture(pollDelay = Duration.ofSeconds(5))
        f.printer.submit("k1", JobRequest("g1", "poll-ada"))
        f.runScheduled()

        assertEquals(JobStatus("g1", "poll-ada", "PENDING"), f.printer.status("g1"))
        f.now = f.now.plusSeconds(4)
        assertEquals("PENDING", f.printer.status("g1")!!.status)
        f.now = f.now.plusSeconds(1)
        assertEquals(JobStatus("g1", "poll-ada", "DONE"), f.printer.status("g1"))
        assertTrue(f.webhooks.isEmpty() && f.topic.isEmpty())
    }

    @Test fun `a silent job accepts and never answers`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "silent-ada"))
        f.runScheduled()
        f.now = f.now.plus(Duration.ofHours(1))

        assertTrue(f.webhooks.isEmpty() && f.topic.isEmpty())
        assertEquals("PENDING", f.printer.status("g1")!!.status)
        assertEquals(0, f.crashes)
    }

    @Test fun `a crash job takes the process down before replying`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "crash-ada"))

        assertEquals(1, f.crashes)
        f.runScheduled()
        assertTrue(f.webhooks.isEmpty() && f.topic.isEmpty())
    }

    @Test fun `the same idempotency key twice prints once`() {
        val f = Fixture()
        val first = f.printer.submit("k1", JobRequest("g1", "webhook-ada"))
        val second = f.printer.submit("k1", JobRequest("g1", "webhook-ada"))
        f.runScheduled()

        assertEquals(false, first.duplicate)
        assertEquals(true, second.duplicate)
        assertEquals(1, f.webhooks.size)
    }

    @Test fun `a new key for the same job id is a new job request all the same`() {
        val f = Fixture()
        f.printer.submit("k1", JobRequest("g1", "webhook-ada"))
        f.printer.submit("k2", JobRequest("g1", "webhook-ada"))
        f.runScheduled()

        assertEquals(2, f.webhooks.size)
    }

    @Test fun `an unknown job has no status`() {
        assertNull(Fixture().printer.status("nope"))
    }
}

class WebhookSignatureTest {
    @Test fun `the signature is the HMAC-SHA256 of the body keyed by the secret, as sha256 prefixed hex`() {
        assertEquals(
            "sha256=9f69fed2d08d376e0365531041e58607a2b2120e6509f22664dbe31be53defc1",
            sign("test-secret", """{"deliveryId":"printed-g1"}""".toByteArray()),
        )
    }
}
