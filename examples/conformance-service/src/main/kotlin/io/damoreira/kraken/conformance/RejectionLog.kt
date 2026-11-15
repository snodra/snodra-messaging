package io.damoreira.kraken.conformance

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubConnections
import org.slf4j.LoggerFactory

/**
 * The latest `Register` refusal the library exposes. kraken-messaging keeps it on each connection
 * ([HubConnection.lastRejection]), but clears it once a later stream is acknowledged ("a rejection no longer describes a hub
 * that has answered since"); it also logs every distinct refusal at `ERROR` ("Hub rejected the registration of ..."). The
 * contract allows either means: [latest] is the connection's own while one holds it, else the last one logged.
 */
object RejectionLog {
    @Volatile private var logged: String? = null

    /** Listens to the library's hub connection log (called once logging is set up and before the hub connections start). */
    fun install() {
        val context = LoggerFactory.getILoggerFactory() as? LoggerContext ?: return
        val appender = object : AppenderBase<ILoggingEvent>() {
            override fun append(event: ILoggingEvent) {
                if (event.message.startsWith("Hub rejected the registration")) {
                    // "Hub rejected the registration of {} ({}): {} {}; retrying in {}": the code and the hub's description.
                    val args = event.argumentArray.orEmpty()
                    logged = if (args.size >= 4) "${args[2]}: ${args[3]}" else event.formattedMessage
                }
            }
        }
        appender.context = context
        appender.name = "conformance-rejections"
        appender.start()
        (context.getLogger(HubConnection::class.java) as Logger).addAppender(appender)
    }

    fun latest(connections: HubConnections): String? =
        connections.connections().firstNotNullOfOrNull { c -> c.lastRejection.value?.let { "${it.code.name}: ${it.description}" } } ?: logged
}
