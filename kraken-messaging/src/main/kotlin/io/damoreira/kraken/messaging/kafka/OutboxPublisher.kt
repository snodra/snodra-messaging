package io.damoreira.kraken.messaging.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Relays unsent `outbox` documents to Kafka in creation order, on a single daemon thread. */
class OutboxPublisher(
    private val mongo: MongoTemplate,
    private val kafka: KafkaTemplate<String, ByteArray>,
    private val topic: String,
    private val source: String,
    private val pollInterval: Duration,
    private val batchSize: Int,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(OutboxPublisher::class.java)
    private val lock = ReentrantLock()
    private val wake = lock.newCondition()

    @Volatile private var running = false
    private var thread: Thread? = null

    /** Sends one batch of pending documents sequentially; returns how many were sent. Stops at the first failure. */
    fun publishPendingNow(): Int {
        val pending = mongo.find(
            Query(Criteria.where("sentAt").isNull).with(Sort.by(Sort.Direction.ASC, "createdAt", "seq")).limit(batchSize),
            OutboxDocument::class.java,
        )
        var sent = 0
        for (doc in pending) {
            try {
                kafka.send(toRecord(doc)).get(30, TimeUnit.SECONDS)
            } catch (e: Exception) {
                if (e is InterruptedException) Thread.currentThread().interrupt()
                log.warn("Outbox send failed for {} ({}), attempt {}", doc.id, doc.type, doc.attempts + 1, e)
                mongo.updateFirst(byId(doc.id), Update().inc("attempts", 1), OutboxDocument::class.java)
                return sent
            }
            mongo.updateFirst(byId(doc.id), Update().set("sentAt", Instant.now()), OutboxDocument::class.java)
            sent++
        }
        return sent
    }

    private fun byId(id: String) = Query(Criteria.where("_id").`is`(id))

    private fun toRecord(doc: OutboxDocument): ProducerRecord<String, ByteArray> {
        val record = ProducerRecord<String, ByteArray>(
            topic, null, doc.key, doc.payload.toJson().toByteArray(Charsets.UTF_8),
        )
        val all = LinkedHashMap(doc.headers)
        all[EventHeaders.TYPE] = doc.type
        all[EventHeaders.ID] = doc.id
        all[EventHeaders.TIMESTAMP] = doc.createdAt.toString()
        all[EventHeaders.SOURCE] = source
        all.forEach { (k, v) -> record.headers().add(RecordHeader(k, v.toByteArray(Charsets.UTF_8))) }
        return record
    }

    override fun start() {
        lock.withLock {
            if (running) return
            running = true
            thread = Thread(::loop, "kraken-outbox-publisher").apply { isDaemon = true; start() }
        }
    }

    private fun loop() {
        while (running) {
            try {
                publishPendingNow()
            } catch (e: Exception) {
                log.warn("Outbox poll failed", e)
            }
            lock.withLock {
                if (running) wake.awaitNanos(pollInterval.toNanos())
            }
        }
    }

    override fun stop() {
        val t = lock.withLock {
            running = false
            wake.signalAll()
            thread.also { thread = null }
        } ?: return
        t.join(35_000)
        if (t.isAlive) t.interrupt()
    }

    override fun isRunning(): Boolean = running

    override fun getPhase(): Int = Int.MAX_VALUE - 100
}
