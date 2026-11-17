package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.ReplyPart
import com.damoreira.snodra.server.v1.ServerMessage

/**
 * One open subscription as the transport hands it to [HubBus] (P4c): the initial part is in, the updates follow. The
 * transport checks what it can see of the stream: the parts' `seq` runs 1, 2, 3, ... and a `ReplyEnd.parts` count is
 * what arrived; it refills the sender's credit as [next] consumes parts.
 */
interface HubSubscription {
    /** The initial result's part (`seq` 1), already received. */
    val initial: ReplyPart

    /**
     * The next message: a part (an update), or the terminal one (`ReplyEnd`, clean or with an error; or an `Error`).
     * Null once [cancel] was called (locally cancelled). There is no local deadline: a subscription may stay quiet.
     *
     * Consuming a part counts towards the credit refill: the previous part returned counts as consumed when this is
     * called again; the initial part counts as consumed as soon as [HubTransport.subscribe] returned it.
     *
     * @throws com.damoreira.snodra.messaging.HubConnectionLostException when the stream to the hub broke
     * @throws com.damoreira.snodra.messaging.InvalidMessageException `subscription <type> lost N part(s)` (the subscription is
     *   cancelled when it is still open)
     */
    suspend fun next(): ServerMessage?

    /** Writes `Cancel{id, reason}` to the hub once (unless the subscription ended or its stream broke) and closes [next]. */
    fun cancel(reason: String)
}

/** What [HubTransport.subscribe] found out before returning. */
sealed interface SubscribeResult {
    /** The initial part arrived: the subscription is open. */
    class Open(val subscription: HubSubscription) : SubscribeResult

    /** The hub answered with a terminal message before any part: an `Error` (a gate) or a `ReplyEnd{error}` (the initial failed). */
    class Closed(val terminal: ServerMessage) : SubscribeResult
}
