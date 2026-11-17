package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.messaging.hub.PermitLedger.Step
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/** The flush rule of returned permits (ADR 0014), without a stream or a clock. */
class PermitLedgerTest {

    private fun ledger(initial: Int) = PermitLedger(initial)

    private fun PermitLedger.receive(n: Int) = repeat(n) { requestReceived() }

    @Test fun `with one initial permit every completion is flushed at once`() {
        val ledger = ledger(initial = 1)
        repeat(3) {
            ledger.receive(1)
            assertEquals(Step.Flush(1), ledger.handlerCompleted()) // the hub holds 0: no 50 ms wait
        }
        assertEquals(0, ledger.timerFired())
    }

    @Test fun `a larger initial batches 16 completions while the hub holds more than the low-water mark`() {
        val ledger = ledger(initial = 64) // low-water mark min(16, 64 / 2) = 16
        ledger.receive(40) // the hub holds 24
        assertEquals(Step.ArmTimer, ledger.handlerCompleted())
        repeat(14) { assertEquals(Step.Wait, ledger.handlerCompleted(), "completion ${it + 2}") }
        assertEquals(Step.Flush(16), ledger.handlerCompleted())
        assertEquals(0, ledger.timerFired()) // nothing left for the armed timer
    }

    @Test fun `the timer flushes what is pending and is armed again by the next completion`() {
        val ledger = ledger(initial = 64)
        ledger.receive(2)
        assertEquals(Step.ArmTimer, ledger.handlerCompleted())
        assertEquals(1, ledger.timerFired())
        assertEquals(Step.ArmTimer, ledger.handlerCompleted())
        assertEquals(1, ledger.timerFired())
    }

    @Test fun `once the hub holds no more than the low-water mark permits go back at once`() {
        val large = ledger(initial = 64)
        large.receive(47) // the hub holds 17
        assertEquals(Step.ArmTimer, large.handlerCompleted())
        large.receive(1) // the hub holds 16
        assertEquals(Step.Flush(2), large.handlerCompleted()) // the pending one goes along
        large.receive(1) // 64 - 49 + 2 = 17 again
        assertEquals(Step.Wait, large.handlerCompleted()) // the timer is still armed
        assertEquals(1, large.timerFired())

        val small = ledger(initial = 8) // low-water mark min(16, 4) = 4
        small.receive(3) // the hub holds 5
        assertEquals(Step.ArmTimer, small.handlerCompleted())
        small.receive(1) // 4
        assertEquals(Step.Flush(2), small.handlerCompleted())

        val two = ledger(initial = 2) // low-water mark 1
        two.receive(1) // the hub holds 1
        assertEquals(Step.Flush(1), two.handlerCompleted())
    }

    @Test fun `a top-up counts as held by the hub, an empty one is not written`() {
        assertEquals(false, ledger(initial = 4).toppedUp(0))

        val ledger = ledger(initial = 4) // low-water mark min(16, 4 / 2) = 2
        ledger.receive(4) // the hub holds 0
        assertEquals(true, ledger.toppedUp(4)) // the hub holds 4 again (Permits.grant raised its cap)
        assertEquals(Step.ArmTimer, ledger.handlerCompleted()) // above the low-water mark: batched, not flushed at once
    }
}
