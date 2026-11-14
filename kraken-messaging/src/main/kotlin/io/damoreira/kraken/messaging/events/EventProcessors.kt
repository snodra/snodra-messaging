package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.messaging.control.ProcessorControl
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** The status of this instance's event processors, one entry per started processor (ADR 0022); never blocks. */
fun interface ProcessorStatusSource {
    fun processorStatuses(): List<ProcessorStatus>
}

/**
 * The event processors of this service (ADR 0022): at start, one [EventConsumer] (from [consumerFor]) per processor that runs
 * here ([LocalEventDispatcher.processors]: every `@ProcessingGroup` with a handler, and `default` when it has a handler or
 * sagas run), each in its own subscription of the event store; they start and stop together.
 *
 * [processorStatuses] builds the processors' status from their in-memory state and what their subscriptions report (never
 * blocking), so it never blocks the hub connection or a consumer. Each call also asks, without waiting, a single background
 * thread to refresh what needs the store: the dead-letter counts (every [deadLetterSample], and after a park).
 *
 * With a [control] (P6b, ADR 0023) the consumers are started by it, as their `processor_control` documents say (paused, with
 * their replay ranges, or not at all while a reset holds the lock), and follow the documents from then on.
 */
class EventProcessors(
    private val dispatcher: LocalEventDispatcher,
    private val consumerFor: (processor: String) -> EventConsumer,
    private val deadLetterSample: Duration = Duration.ofSeconds(30),
    private val clock: Clock = Clock.systemUTC(),
    private val control: ProcessorControl? = null,
) : SmartLifecycle, ProcessorStatusSource {
    private val log = LoggerFactory.getLogger(EventProcessors::class.java)
    @Volatile private var consumers: List<EventConsumer> = emptyList()
    @Volatile private var running = false
    private var executor: ExecutorService? = null
    private val refreshing = AtomicBoolean(false)
    @Volatile private var lastSampleAt = 0L

    /** The consumers created at the last start, `default` first. */
    fun consumers(): List<EventConsumer> = consumers

    @Synchronized
    override fun start() {
        if (running) return
        val created = dispatcher.processors().map(consumerFor)
        val up = mutableListOf<EventConsumer>()
        try {
            if (control != null) control.attach(created) else for (c in created) { c.start(); up += c }
        } catch (e: Exception) {
            (if (control != null) created else up).forEach { runCatching { it.stop() } } // all or nothing: a failed start leaves nothing half-running
            throw e
        }
        consumers = created
        executor = Executors.newSingleThreadExecutor { r -> Thread(r, "kraken-processor-status").apply { isDaemon = true } }
        lastSampleAt = 0L
        running = true
        if (created.isEmpty()) log.info("No event processor started: no @EventHandler and no saga in this service")
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        control?.detach()
        consumers.forEach { runCatching { it.stop() } }
        executor?.let { ex ->
            ex.shutdown()
            if (!ex.awaitTermination(2, TimeUnit.SECONDS)) ex.shutdownNow()
        }
        executor = null
    }

    override fun isRunning(): Boolean = running
    override fun isAutoStartup(): Boolean = true
    override fun getPhase(): Int = EventConsumer.PHASE

    override fun processorStatuses(): List<ProcessorStatus> {
        // A consumer kept out of its group by a reset's lock since the start reports STOPPED with the operation.
        val statuses = consumers.filter { it.wasStarted || it.operationId.isNotEmpty() }.map { it.status() }
        if (running) kick()
        return statuses
    }

    /** Starts a refresh on the background thread unless one is running; returns at once. */
    private fun kick() {
        val ex = executor ?: return
        if (!refreshing.compareAndSet(false, true)) return
        try {
            ex.execute {
                try {
                    refresh()
                } finally {
                    refreshing.set(false)
                }
            }
        } catch (_: java.util.concurrent.RejectedExecutionException) {
            refreshing.set(false) // stopping
        }
    }

    private fun refresh() {
        val now = clock.millis()
        val live = consumers.filter { it.isRunning }
        val force = now - lastSampleAt >= deadLetterSample.toMillis()
        if (force) lastSampleAt = now
        for (c in live) {
            try {
                c.sampleDeadLetters(force)
            } catch (e: Exception) {
                log.debug("Could not count the dead letters of processor {}: {}", c.processor, e.message)
            }
        }
    }
}
