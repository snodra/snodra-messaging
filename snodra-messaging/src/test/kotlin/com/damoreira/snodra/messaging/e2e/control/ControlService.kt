package com.damoreira.snodra.messaging.e2e.control

import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.control.ResetContext
import com.damoreira.snodra.messaging.handler.DisallowReplay
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.ProcessingGroup
import com.damoreira.snodra.messaging.handler.ResetHandler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

@MessageType("p6b.ItemAdded")
data class ItemAdded(val itemId: String, val poison: Boolean = false) : Event

/** A service with a named processor `catalog` (a replayable projection, a reset handler and a `@DisallowReplay` notifier) and a `default` ledger that fails on poison. */
@SpringBootApplication(scanBasePackages = ["com.damoreira.snodra.messaging.e2e.control"])
class ControlService

@Component
@ProcessingGroup("catalog")
class Catalog {
    val seen = CopyOnWriteArrayList<String>()
    val resets = CopyOnWriteArrayList<ResetContext>()
    @EventHandler fun on(event: ItemAdded) { seen += event.itemId }
    @ResetHandler fun clear(ctx: ResetContext) { resets += ctx }
}

@Component
@ProcessingGroup("catalog")
class Notifier {
    val seen = CopyOnWriteArrayList<String>()
    @DisallowReplay @EventHandler fun on(event: ItemAdded) { seen += event.itemId }
}

@Component
class Ledger {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: ItemAdded) {
        if (event.poison) throw IllegalStateException("cannot book ${event.itemId}")
        seen += event.itemId
    }
}
