package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.ControlRefusal
import com.damoreira.snodra.server.v1.ControlRefused
import com.damoreira.snodra.server.v1.ControlReply
import com.damoreira.snodra.server.v1.ControlRequest

/**
 * Serves the hub's `ControlRequest`s. [handle] is called on the stream's read
 * loop, in arrival order, and must return at once: the work runs elsewhere, and [reply] is called exactly once, from any
 * thread, with the answer (it goes back on the stream the request arrived on).
 */
fun interface ControlInbound {
    fun handle(request: ControlRequest, reply: (ControlReply) -> Unit)
}

/** The control of an instance without event processors: every request is `UNKNOWN_PROCESSOR`. */
val NO_PROCESSOR_CONTROL = ControlInbound { request, reply ->
    reply(
        ControlReply.newBuilder().setControlId(request.controlId).setRefused(
            ControlRefused.newBuilder().setCode(ControlRefusal.CONTROL_REFUSAL_UNKNOWN_PROCESSOR)
                .setMessage("no event processors run on this instance"),
        ).build(),
    )
}
