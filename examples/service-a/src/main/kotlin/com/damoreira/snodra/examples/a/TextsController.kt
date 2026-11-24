package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.Reverse
import com.damoreira.snodra.examples.api.Reversed
import com.damoreira.snodra.examples.api.Shout
import com.damoreira.snodra.examples.api.Shouted
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

data class ShoutRequest(val text: String)

/** The caller side of the texts domain: both requests go over the hub to service-c. */
@RestController
class TextsController(
    private val commands: CommandBus,
    private val queries: QueryBus,
    private val listener: TextsListener,
) {
    private val log = LoggerFactory.getLogger(TextsController::class.java)

    @PostMapping("/shouts")
    suspend fun shout(@RequestBody body: ShoutRequest): Shouted {
        if (body.text.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "text must not be blank")
        val shouted = commands.send(Shout(body.text))
        log.info("A: sent Shout text={} -> {}", body.text, shouted.text)
        return shouted
    }

    @GetMapping("/reverse/{text}")
    suspend fun reverse(@PathVariable text: String): Reversed {
        if (text.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "text must not be blank")
        val reversed = queries.send(Reverse(text))
        log.info("A: asked Reverse text={} -> {}", text, reversed.text)
        return reversed
    }

    @GetMapping("/texts/delivered")
    fun delivered(): List<TextEvent> = listener.delivered
}
