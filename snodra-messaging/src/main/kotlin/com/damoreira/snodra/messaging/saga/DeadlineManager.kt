package com.damoreira.snodra.messaging.saga

import java.time.Duration
import java.time.Instant

/** Schedules and cancels the deadlines of the calling saga instance (saga-scoped); a handler parameter. */
interface DeadlineManager {
    /** Schedules a deadline named [name] to fire [after] from now; returns its id. */
    fun schedule(after: Duration, name: String, payload: Any? = null): String

    /** Schedules a deadline named [name] to fire at [at]; returns its id. */
    fun schedule(at: Instant, name: String, payload: Any? = null): String

    /** Cancels the deadline [deadlineId] (no effect when it already fired or was cancelled). */
    fun cancel(deadlineId: String)

    /** Cancels every scheduled deadline of this saga named [name]. */
    fun cancelAll(name: String)
}
