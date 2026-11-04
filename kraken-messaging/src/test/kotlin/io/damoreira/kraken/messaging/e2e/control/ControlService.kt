package io.damoreira.kraken.messaging.e2e.control

import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.control.ResetContext
import io.damoreira.kraken.messaging.handler.DisallowReplay
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import io.damoreira.kraken.messaging.handler.ResetHandler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

@MessageType("p6b.ItemAdded")
data class ItemAdded(val itemId: String, val poison: Boolean = false) : Event

/** A service with a named processor `catalog` (a replayable projection, a reset handler and a `@DisallowReplay` notifier) and a `default` ledger that fails on poison. */
@SpringBootApplication(scanBasePackages = ["io.damoreira.kraken.messaging.e2e.control"])
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
