package io.damoreira.kraken.messaging.hub

/**
 * Permit accounting of one hub stream (flow control, ADR 0014): decides when the permits of completed handlers are
 * written back in a `Permits` message.
 *
 * Per stream it keeps `initial` (the `Register.initial_permits` sent on it), `received` (requests that arrived on it,
 * counted whatever the flow-control mode), `returned` (permits written in `Permits` messages, top-ups included) and
 * `pending` (handlers completed, permits not yet written). The hub therefore still holds `initial - received +
 * returned` permits for this stream. A top-up ([toppedUp], multi-hub: another hub went away) adds to `returned`: it is
 * written with `Permits.grant`, which raises the hub's cap on the session so the hub keeps it. (A hub that predates
 * the grant clamps it away; the ledger then over-counts, which only delays a grant by at most
 * [HubProtocol.PERMIT_FLUSH_DELAY].) On each completion, `pending += 1`, then:
 *
 * - the hub holds at most the low-water mark `min(PERMIT_BATCH, initial / 2)`: flush now (with one permit, every
 *   completion goes back at once instead of after [HubProtocol.PERMIT_FLUSH_DELAY]);
 * - else [HubProtocol.PERMIT_BATCH] are pending: flush now;
 * - else arm the [HubProtocol.PERMIT_FLUSH_DELAY] timer unless it is armed.
 *
 * A flush writes `Permits{count = pending}`, `returned += pending`, `pending = 0`; the timer flush does the same.
 * Completions before the handshake are held: [handshake] applies the same rule to them when the hub shares
 * flow control and drops them otherwise (no `Permits` are ever sent to such a hub). Thread-safe.
 */
internal class PermitLedger(private val initial: Int) {

    /** What the connection does next. */
    sealed interface Step {
        /** Write `Permits{count}` now. */
        data class Flush(val count: Int) : Step

        /** Arm the flush timer; call [timerFired] when it fires. */
        data object ArmTimer : Step

        /** Nothing to write now. */
        data object Wait : Step
    }

    private val lowWaterMark: Long = minOf(HubProtocol.PERMIT_BATCH, initial / 2).toLong()
    private var flowControl: Boolean? = null // null until the handshake
    private var received = 0L
    private var returned = 0L
    private var pending = 0
    private var timerArmed = false

    /** A request arrived on the stream. */
    @Synchronized fun requestReceived() {
        received++
    }

    /** A handler of a request from the stream completed. */
    @Synchronized fun handlerCompleted(): Step {
        if (flowControl == false) return Step.Wait
        pending++
        return if (flowControl == true) decide() else Step.Wait
    }

    /** The stream's handshake: whether the hub shares flow control. Decides the completions held until now. */
    @Synchronized fun handshake(flowControl: Boolean): Step {
        this.flowControl = flowControl
        if (!flowControl) {
            pending = 0
            return Step.Wait
        }
        return if (pending > 0) decide() else Step.Wait
    }

    /**
     * [count] extra permits are about to be written in a `Permits` message of their own (not paid for by handlers).
     * False (write nothing) unless the stream's handshake confirmed flow control.
     */
    @Synchronized fun toppedUp(count: Int): Boolean {
        if (flowControl != true || count <= 0) return false
        returned += count
        return true
    }

    /** The flush timer fired: the count to write now (0 = nothing). */
    @Synchronized fun timerFired(): Int {
        timerArmed = false
        return takePending()
    }

    private fun decide(): Step {
        val heldByHub = initial - received + returned // signed: never wraps
        return when {
            heldByHub <= lowWaterMark || pending >= HubProtocol.PERMIT_BATCH -> Step.Flush(takePending())
            timerArmed -> Step.Wait
            else -> {
                timerArmed = true
                Step.ArmTimer
            }
        }
    }

    private fun takePending(): Int = pending.also {
        returned += it
        pending = 0
    }
}
