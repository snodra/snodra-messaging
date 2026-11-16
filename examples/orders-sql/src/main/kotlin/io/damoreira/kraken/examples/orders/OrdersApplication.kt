package io.damoreira.kraken.examples.orders

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate

/**
 * A small service on PostgreSQL with the SQL message store (`kraken-messaging-jdbc`): its own table `orders` and the library's
 * `kraken_*` tables live in the same database, so an order row and the event announcing it commit in one transaction.
 */
@SpringBootApplication
class OrdersApplication

fun main(args: Array<String>) {
    runApplication<OrdersApplication>(*args)
}

/** Places an order. */
@MessageType("examples.orders.PlaceOrder")
data class PlaceOrder(val orderId: String, val item: String) : Command<OrderAccepted>

/** Reply to [PlaceOrder]. */
data class OrderAccepted(val orderId: String)

/** Published, through the outbox, in the transaction that stored the order. */
@MessageType("examples.orders.OrderPlaced")
data class OrderPlaced(val orderId: String, val item: String) : Event

@Component
class Orders(private val jdbc: JdbcClient, private val events: EventBus, transactions: PlatformTransactionManager) {
    private val log = LoggerFactory.getLogger(Orders::class.java)
    private val transaction = TransactionTemplate(transactions)

    /**
     * The order row and the outbox entry of [OrderPlaced] in one transaction of the application's DataSourceTransactionManager:
     * both commit, or (an empty item is refused after both were written) neither does.
     */
    @CommandHandler
    fun place(command: PlaceOrder, ctx: MessageContext): OrderAccepted = transaction.execute {
        jdbc.sql("INSERT INTO orders (id, item, status) VALUES (:id, :item, 'PLACED')").param("id", command.orderId).param("item", command.item).update()
        events.publishBlocking(OrderPlaced(command.orderId, command.item), key = command.orderId)
        require(command.item.isNotBlank()) { "an order needs an item" }
        log.info("Placed order {} (correlationId {})", command.orderId, ctx.correlationId)
        OrderAccepted(command.orderId)
    }

    /** Confirms a placed order: the update and the processed marker of the event commit together (the library's transaction). */
    @EventHandler
    fun confirm(event: OrderPlaced) {
        jdbc.sql("UPDATE orders SET status = 'CONFIRMED' WHERE id = :id").param("id", event.orderId).update()
        log.info("Confirmed order {}", event.orderId)
    }
}
