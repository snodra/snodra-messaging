package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.Counter
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

/** What drives service-b's side of `examples.WatchCounter` from outside: `curl -X POST localhost:8092/counters/x/increment`. */
@RestController
class CounterController(private val counters: CounterHandler) {
    @PostMapping("/counters/{name}/increment")
    fun increment(@PathVariable name: String): Counter {
        if (name.isBlank()) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "name must not be blank")
        return counters.increment(name)
    }
}
