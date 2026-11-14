package io.damoreira.kraken.messaging.jdbc

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The relay wake-up on PostgreSQL: `LISTEN kraken_outbox` on a connection of its own, and every notification an append's
 * commit delivers wakes the registered relays ([register]). The listening thread runs while at least one relay is registered.
 *
 * The connection comes from [connect]: a [dedicated] one (outside the pool) is kept until the last relay goes; one borrowed
 * from the pool is given back every [POOLED_CYCLE] and borrowed again, so the wake-up never holds a pooled connection for good.
 * A failure (the database restarted, the network) is logged once and retried with a backoff from [MIN_BACKOFF] to
 * [MAX_BACKOFF]; the relays wake up whenever listening starts again (an append may have committed meanwhile) and keep polling
 * anyway.
 */
internal class PostgresNotifications(private val connect: () -> Connection, private val dedicated: Boolean) {
    private val listeners = CopyOnWriteArrayList<Listener>()
    private val lock = Any()
    /** The running listening thread's stop flag (each thread its own, so a restarted one never revives a stopping one). */
    private var running: AtomicBoolean? = null

    private class Listener(val wake: () -> Unit)

    /** Wakes [wake] after every append that commits, until the handle returned is closed. */
    fun register(wake: () -> Unit): AutoCloseable {
        val listener = Listener(wake)
        synchronized(lock) {
            listeners += listener
            if (running == null) {
                val flag = AtomicBoolean(true)
                running = flag
                Thread({ loop(flag) }, "kraken-outbox-listen").apply { isDaemon = true; start() }
            }
        }
        return AutoCloseable {
            synchronized(lock) {
                if (listeners.remove(listener) && listeners.isEmpty()) {
                    running?.set(false)
                    running = null
                }
            }
        }
    }

    /** Whether the listening thread runs (for tests). */
    internal val listening: Boolean get() = synchronized(lock) { running != null }

    private fun wakeAll() {
        for (listener in listeners) runCatching { listener.wake() }
    }

    private fun loop(running: AtomicBoolean) {
        var backoff = MIN_BACKOFF
        var failing = false
        while (running.get()) {
            try {
                connect().use { c ->
                    if (!c.autoCommit) c.autoCommit = true
                    c.createStatement().use { it.execute("LISTEN ${PostgresDialect.CHANNEL}") }
                    if (failing) log.info("Listening for outbox appends again")
                    failing = false
                    backoff = MIN_BACKOFF
                    wakeAll() // an append may have committed while nobody listened
                    val pg = c.unwrap(PGConnection::class.java)
                    val until = System.nanoTime() + POOLED_CYCLE.toNanos()
                    while (running.get() && (dedicated || System.nanoTime() - until < 0)) {
                        val received = pg.getNotifications(WAIT_MILLIS)
                        if (received != null && received.isNotEmpty()) wakeAll()
                    }
                    c.createStatement().use { it.execute("UNLISTEN ${PostgresDialect.CHANNEL}") }
                }
            } catch (e: Exception) {
                if (!running.get()) break
                if (!failing) log.warn("Cannot listen for outbox appends (the relay polls meanwhile); retrying: {}", e.toString())
                failing = true
                try {
                    Thread.sleep(backoff.toMillis())
                } catch (_: InterruptedException) {
                    break
                }
                backoff = minOf(backoff.multipliedBy(2), MAX_BACKOFF)
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(PostgresNotifications::class.java)

        /** How long one wait for notifications lasts: also how soon the thread notices it should stop. */
        const val WAIT_MILLIS = 500

        /** How long a connection borrowed from the pool is kept before it is given back. */
        val POOLED_CYCLE: Duration = Duration.ofSeconds(30)
        val MIN_BACKOFF: Duration = Duration.ofMillis(200)
        val MAX_BACKOFF: Duration = Duration.ofSeconds(30)
    }
}
