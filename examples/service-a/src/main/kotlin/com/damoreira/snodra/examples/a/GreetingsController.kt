package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.CountGreetings
import com.damoreira.snodra.examples.api.GreetingAccepted
import com.damoreira.snodra.examples.api.GreetingCount
import com.damoreira.snodra.examples.api.GreetingDelivered
import com.damoreira.snodra.examples.api.SendGreeting
import com.damoreira.snodra.messaging.CommandBus
import com.damoreira.snodra.messaging.QueryBus
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

data class SendGreetingRequest(val name: String)

/** The caller side: every request here becomes a command or a query on the hub. */
@RestController
class GreetingsController(
    private val commands: CommandBus,
    private val queries: QueryBus,
    private val listener: GreetingsListener,
) {
    private val log = LoggerFactory.getLogger(GreetingsController::class.java)

    @PostMapping("/greetings")
    suspend fun send(@RequestBody body: SendGreetingRequest): GreetingAccepted {
        if (body.name.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "name must not be blank")
        val accepted = commands.send(SendGreeting(body.name))
        log.info("A: sent SendGreeting name={} -> accepted id={}", body.name, accepted.id)
        return accepted
    }

    @GetMapping("/greetings/{name}/count")
    suspend fun count(@PathVariable name: String): GreetingCount {
        val count = queries.send(CountGreetings(name))
        log.info("A: asked CountGreetings name={} -> count={}", name, count.count)
        return count
    }

    @GetMapping("/greetings/delivered")
    fun delivered(): List<GreetingDelivered> = listener.delivered
}
