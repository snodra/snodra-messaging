package com.damoreira.snodra.examples.b

import com.damoreira.snodra.examples.api.Row
import com.damoreira.snodra.examples.api.Rows
import com.damoreira.snodra.messaging.handler.QueryHandler
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * service-b's share of the shared streaming query `examples.Rows` (service-c owns it too): streams the rows 1..count,
 * each naming this service and instance, paced by the collector's credits. With `failAt > 0` the flow throws on
 * reaching that row, so the collector sees `HANDLER_ERROR` after `failAt - 1` rows.
 */
@Component
class RowsHandler(
    @Value("\${snodra.messaging.service-name:service-b}") private val service: String = "service-b",
) {
    private val instance: String = System.getenv("HOSTNAME") ?: "local" // the container id under Docker
    private val log = LoggerFactory.getLogger(RowsHandler::class.java)

    @QueryHandler(shared = true)
    fun handle(q: Rows): Flow<Row> = flow {
        log.info("B: streaming Rows count={} failAt={}", q.count, q.failAt)
        for (n in 1..q.count) {
            if (q.failAt > 0 && n == q.failAt) throw IllegalStateException("Rows failed at row $n")
            emit(Row(service, instance, n))
        }
        log.info("B: streamed Rows count={}", q.count)
    }
}
