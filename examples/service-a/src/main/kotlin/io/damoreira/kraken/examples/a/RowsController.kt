package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.Row
import io.damoreira.kraken.examples.api.Rows
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.StreamOptions
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.time.Duration

/** A collected stream as JSON: the rows in the order they arrived, how many, and the service that streamed them. */
data class RowsResult(val rows: List<Row>, val count: Int, val service: String?)

/**
 * The caller side of the shared streaming query `examples.Rows` (owned by service-b and service-c): one instance of
 * either owner streams the rows under this collector's credits; the whole stream is collected, then returned.
 */
@RestController
class RowsController(private val queries: QueryBus) {
    private val log = LoggerFactory.getLogger(RowsController::class.java)

    /**
     * `count` rows from one owner; `credit` is the credit window (`StreamOptions.initialCredit`), `maxResults` lets the
     * hub end the stream after that many rows (cancelling the handler), `take` cancels the collection after that many
     * rows (a client-side cancel: `Cancel` to the hub, the handler is cancelled), `failAt` makes the handler fail on
     * that row.
     */
    @GetMapping("/stream/rows")
    suspend fun rows(
        @RequestParam(defaultValue = "10") count: Int,
        @RequestParam(defaultValue = "64") credit: Int,
        @RequestParam(defaultValue = "0") maxResults: Int,
        @RequestParam(defaultValue = "0") take: Int,
        @RequestParam(defaultValue = "0") failAt: Int,
    ): RowsResult {
        if (count < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "count must not be negative")
        if (credit < 1) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "credit must be at least 1")
        if (maxResults < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "maxResults must not be negative")
        if (take < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "take must not be negative")
        if (failAt < 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "failAt must not be negative")
        val options = StreamOptions(timeout = IDLE_TIMEOUT, maxResults = maxResults, initialCredit = credit)
        var flow = queries.stream(Rows(count, failAt), options)
        if (take > 0) flow = flow.take(take)
        val rows = flow.toList()
        val service = rows.firstOrNull()?.service
        log.info(
            "A: streamed Rows count={} credit={} maxResults={} take={} -> {} rows from {} ({})",
            count, credit, maxResults, take, rows.size, service, rows.firstOrNull()?.instance,
        )
        return RowsResult(rows, rows.size, service)
    }

    companion object {
        /** The idle timeout of the stream: the hub ends it with `TIMEOUT` when no row arrives within it. */
        val IDLE_TIMEOUT: Duration = Duration.ofSeconds(5)
    }
}
