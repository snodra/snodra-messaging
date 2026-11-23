package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.Pong
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.handler.RequiresAuthentication
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.security.oauth2.jwt.JwtDecoder
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class GuardedPing {
    @CommandHandler @RequiresAuthentication("hasRole('admin')")
    fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

class GuardedCount {
    @QueryHandler @RequiresAuthentication
    fun handle(query: GetCount): Count = Count(1)
}

class OpenPing {
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

/** A handler that requires an authenticated caller needs a JWKS (a `JwtDecoder`) to start; nothing else does. */
class RequiresAuthenticationGuardTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java))
        .withPropertyValues("snodra.messaging.service-name=orders-service", "snodra.messaging.type-prefix=test")

    private fun messages(failure: Throwable) = generateSequence(failure) { it.cause }.mapNotNull { it.message }.joinToString(" | ")

    @Test fun `a guarded handler without a JWKS fails the startup, naming every guarded handler`() {
        runner.withBean(GuardedPing::class.java).withBean(GuardedCount::class.java).run { context ->
            val failure = assertNotNull(context.startupFailure)
            val message = messages(failure)
            assertTrue(
                "GuardedCount.handle, GuardedPing.handle require an authenticated caller (@RequiresAuthentication) but no JWKS is configured" in message,
                message,
            )
            assertTrue("spring.security.oauth2.resourceserver.jwt.jwk-set-uri" in message, message)
        }
    }

    @Test fun `a guarded handler registered after startup without a JWKS fails and is not registered`() {
        runner.withBean(OpenPing::class.java).run { context ->
            assertNull(context.startupFailure)
            val registry = context.getBean(com.damoreira.snodra.messaging.handler.HandlerRegistry::class.java)
            val e = kotlin.test.assertFailsWith<IllegalStateException> { registry.register(GuardedCount()) }
            assertTrue("GuardedCount.handle requires an authenticated caller" in e.message.orEmpty(), e.message)
            assertEquals(emptySet(), registry.queryTypes(), "nothing of the refused bean stays registered")
        }
    }

    @Test fun `with a JwtDecoder a guarded handler starts`() {
        runner.withBean(GuardedPing::class.java).withBean(JwtDecoder::class.java, { TestJwt.decoder() }).run { context ->
            assertNull(context.startupFailure)
        }
    }

    @Test fun `without a guarded handler no JWKS is needed`() {
        runner.withBean(OpenPing::class.java).run { context ->
            assertNull(context.startupFailure)
            assertEquals(1, context.getBean(com.damoreira.snodra.messaging.handler.HandlerRegistry::class.java).commandTypes().size)
        }
    }
}
