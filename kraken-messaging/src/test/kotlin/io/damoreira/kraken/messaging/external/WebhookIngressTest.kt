package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.saga.stateDoc
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaIndexes
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.StartSaga
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@MessageType("wh.Hook")
data class WhHook(val jobId: String, val status: String) : Event

@Saga(name = "wh.Job")
class WhSaga {
    var jobId: String? = null
    var hooks = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "jobId")
    fun on(e: WhHook) {
        jobId = e.jobId
        hooks++
    }
}

/** The ordinary handler of the webhook's event; fails while [failuresLeft] is positive. */
class WhHandlers {
    val seen = CopyOnWriteArrayList<String>()
    val failuresLeft = AtomicInteger()

    @EventHandler
    fun on(e: WhHook) {
        if (failuresLeft.getAndUpdate { if (it > 0) it - 1 else 0 } > 0) throw IllegalStateException("handler failed on purpose")
        seen += e.jobId
    }
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebhookIngressTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var mongo: MongoTemplate
    private lateinit var tx: TransactionTemplate
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("wh")
    private lateinit var handlers: WhHandlers
    private lateinit var ingress: ExternalEventIngress

    @BeforeAll
    fun start() {
        mongoContainer.start()
        val factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "webhook_ingress_test")
        mongo = MongoTemplate(factory)
        tx = TransactionTemplate(MongoTransactionManager(factory))
        SagaIndexes(mongo, Duration.ofDays(30), Duration.ofDays(7), Duration.ofDays(7)).afterPropertiesSet()
        listOf("processed_events", "saga_failures").forEach { if (!mongo.collectionExists(it)) mongo.createCollection(it) }
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @BeforeEach
    fun setUp() {
        listOf("sagas", "saga_dispatch", "deadlines", "saga_failures", "processed_events").forEach { mongo.remove(Query(), it) }
        handlers = WhHandlers()
        val registry = HandlerRegistry(resolver).also { it.register(handlers) }
        val runtime = SagaRuntime(
            types = SagaModelScanner(resolver, codec).scan(listOf(WhSaga::class)), mongo = mongo, tx = tx, codec = codec, typeResolver = resolver,
            beans = { error("no beans") },
            retry = SagaRetry(RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(1), multiplier = 1.0, maxBackoff = Duration.ofMillis(1))),
        )
        ingress = ExternalEventIngress(LocalEventDispatcher(registry, HandlerInvoker(registry, codec), tx, mongo, runtime), mongo, codec, resolver)
    }

    private fun sagaOf(jobId: String) = mongo.findAll(SagaDocument::class.java).singleOrNull { it.stateDoc.getString("jobId") == jobId }

    @Test
    fun `an accepted webhook event reaches a saga`() {
        val result = ingress.accept("printer", "evt-1", WhHook("j1", "done"))

        // synchronous: the saga and the ordinary handler have run by the time accept returned
        assertEquals(ExternalEventIngress.Result.ACCEPTED, result)
        assertEquals(1, sagaOf("j1")?.stateDoc?.getInteger("hooks"))
        assertEquals(listOf("j1"), handlers.seen.toList())
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("printer:evt-1")), "processed_events"), "the dedupe id is <source>:<dedupeKey>")
        assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("saga:wh.Job:printer:evt-1")), "processed_events"))
    }

    @Test
    fun `the same dedupe key twice is DUPLICATE and applied once`() {
        assertEquals(ExternalEventIngress.Result.ACCEPTED, ingress.accept("printer", "evt-2", WhHook("j2", "done")))
        assertEquals(ExternalEventIngress.Result.DUPLICATE, ingress.accept("printer", "evt-2", WhHook("j2", "done")))
        assertEquals(ExternalEventIngress.Result.ACCEPTED, ingress.accept("printer", "evt-3", WhHook("j2", "again")))
        // the same key of another source is another event
        assertEquals(ExternalEventIngress.Result.ACCEPTED, ingress.accept("carrier", "evt-2", WhHook("j2", "other")))

        assertEquals(3, sagaOf("j2")?.stateDoc?.getInteger("hooks"))
        assertEquals(listOf("j2", "j2", "j2"), handlers.seen.toList())
    }

    @Test
    fun `a failing handler fails the call and the retry of the same key is accepted`() {
        handlers.failuresLeft.set(1)

        assertFailsWith<IllegalStateException> { ingress.accept("printer", "evt-4", WhHook("j4", "done")) }
        assertEquals(null, sagaOf("j4"), "stage 1 failed: the saga stage did not run")
        assertEquals(ExternalEventIngress.Result.ACCEPTED, ingress.accept("printer", "evt-4", WhHook("j4", "done")), "a failed call is not a duplicate")
        assertEquals(1, sagaOf("j4")?.stateDoc?.getInteger("hooks"))
    }

    private fun hmac(secret: String, body: ByteArray): String =
        Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(secret.toByteArray(), "HmacSHA256")) }.doFinal(body).joinToString("") { "%02x".format(it) }

    private val body = """{"jobId":"j1"}""".toByteArray()
    private val verifier = WebhookSignatureVerifier()

    @Test
    fun `a valid HMAC verifies with the current and with the previous secret`() {
        val signedWithCurrent = mapOf("X-Kraken-Signature" to "sha256=" + hmac("new-secret", body))
        val signedWithPrevious = mapOf("X-Kraken-Signature" to "sha256=" + hmac("old-secret", body))

        assertTrue(verifier.verify("new-secret", "old-secret", signedWithCurrent, body))
        assertTrue(verifier.verify("new-secret", "old-secret", signedWithPrevious, body))
        assertFalse(verifier.verify("new-secret", null, signedWithPrevious, body), "the previous secret counts only when configured")
        // header names are case-insensitive, and the shared-secret header works for both secrets
        assertTrue(verifier.verify("new-secret", null, mapOf("x-kraken-signature" to "SHA256=" + hmac("new-secret", body).uppercase()), body))
        assertTrue(verifier.verify("new-secret", "old-secret", mapOf("x-kraken-webhook-secret" to "new-secret"), body))
        assertTrue(verifier.verify("new-secret", "old-secret", mapOf("X-KRAKEN-WEBHOOK-SECRET" to "old-secret"), body))
    }

    @Test
    fun `a wrong or missing signature does not verify`() {
        assertFalse(verifier.verify("s", null, emptyMap(), body), "no header")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Signature" to "sha256=" + hmac("other", body)), body), "wrong secret")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Signature" to "sha256=" + hmac("s", "tampered".toByteArray())), body), "another body")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Signature" to "sha256=zz"), body), "not hex")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Signature" to ""), body), "empty signature")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Webhook-Secret" to "nope"), body), "wrong shared secret")
        assertFalse(verifier.verify("s", null, mapOf("X-Kraken-Webhook-Secret" to ""), body), "empty shared secret")
        // a service with no secret configured accepts nothing, not even an empty header
        assertFalse(verifier.verify("", null, mapOf("X-Kraken-Webhook-Secret" to ""), body))
        assertFalse(verifier.verify("", "", mapOf("X-Kraken-Signature" to "sha256=648662a43b6085062599639fbf589c39bacfd8195b933196b10bd04138275d2a"), body), "the HMAC under an empty key")
    }

    @Test
    fun `the comparison is constant time`() {
        // By code path, not by timing: every comparison of a secret or a signature goes through the injected byte comparison
        // (MessageDigest.isEqual by default), never through String.equals or a short-circuit on the first differing byte.
        val compared = CopyOnWriteArrayList<Pair<String, String>>()
        val recording = WebhookSignatureVerifier { a, b ->
            compared += String(a) to String(b)
            java.security.MessageDigest.isEqual(a, b)
        }

        assertTrue(recording.verify("s3cret", "old", mapOf("X-Kraken-Webhook-Secret" to "s3cret"), body))
        assertEquals(listOf("s3cret" to "s3cret"), compared.toList())

        compared.clear()
        val good = hmac("old", body)
        assertTrue(recording.verify("s3cret", "old", mapOf("X-Kraken-Signature" to "sha256=$good"), body))
        assertEquals(listOf(hmac("s3cret", body) to good, good to good), compared.toList(), "the current secret's signature is compared first, then the previous one's")

        compared.clear()
        assertFalse(recording.verify("s3cret", null, mapOf("X-Kraken-Signature" to "sha256=ab"), body))
        assertEquals(1, compared.size, "a short signature is still compared, not rejected by length first")
    }
}
