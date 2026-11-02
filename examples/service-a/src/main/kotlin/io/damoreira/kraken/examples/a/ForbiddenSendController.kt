package io.damoreira.kraken.examples.a

import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/**
 * A query nobody handles and the sandbox's ACL (`kraken/infra/scripts/dev-hub-pki.sh`) deliberately does not list in
 * service-a's `send` patterns. It lives here, not in `examples-api`, because no other service has anything to do with it.
 */
@MessageType("examples.ForbiddenProbe")
data class ForbiddenProbe(val note: String = "") : Query<ForbiddenProbeAnswer>

data class ForbiddenProbeAnswer(val note: String)

/**
 * `GET /forbidden-send`: sends [ForbiddenProbe], a type outside this service's `send` patterns. Against a hub in
 * `enforce` mode the hub answers `PERMISSION_DENIED` before routing anything, which the library raises as
 * `AccessDeniedException` and [MessagingErrorAdvice] maps to 403 (the smoke's step 21). Against a hub without access
 * control nobody owns the type, so it ends as the usual 503 `NoHandlerException`.
 */
@RestController
class ForbiddenSendController(private val queries: QueryBus) {
    private val log = LoggerFactory.getLogger(ForbiddenSendController::class.java)

    @GetMapping("/forbidden-send")
    suspend fun forbiddenSend(): ForbiddenProbeAnswer {
        log.info("A: sending ForbiddenProbe, a type outside this service's send patterns")
        return queries.send(ForbiddenProbe())
    }
}
