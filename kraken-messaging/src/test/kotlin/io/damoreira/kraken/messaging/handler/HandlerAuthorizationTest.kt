package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.KrakenForwardedAuthentication
import io.damoreira.kraken.messaging.security.MessagePrincipal
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

data class OwnedThing(val ownerId: String)

class HandlerAuthorizationTest {
    private val label = "Bean.handle"

    private fun identity(userId: String?, vararg roles: String): Identity {
        val principal = MessagePrincipal(userId, roles.toSet(), verified = true)
        return Identity(principal, KrakenForwardedAuthentication("tok", principal))
    }

    private fun ctx(userId: String?, vararg roles: String) =
        MessageContext("m-1", "test.Ping", "c-1", userId, roles.toSet(), null, emptyMap())

    private fun compile(source: String) = HandlerAuthorizations.compile(source, label)

    @Test fun `default expression grants authenticated and rejects anonymous`() {
        val auth = compile("isAuthenticated()")

        assertIs<AuthorizationDecision.Granted>(auth.check(identity("u-1", "kraken-user"), Ping("x"), ctx("u-1"), label))
        val denied = assertIs<AuthorizationDecision.Unauthenticated>(auth.check(null, Ping("x"), ctx(null), label))
        assertEquals("handler Bean.handle requires authentication", denied.message)
    }

    @Test fun `hasRole matches realm role and denies otherwise`() {
        val auth = compile("hasRole('kraken-user')")

        assertIs<AuthorizationDecision.Granted>(auth.check(identity("u-1", "kraken-user"), Ping("x"), ctx("u-1"), label))
        val denied = assertIs<AuthorizationDecision.Denied>(auth.check(identity("u-1", "other"), Ping("x"), ctx("u-1"), label))
        assertEquals("handler Bean.handle: access denied", denied.message)
    }

    @Test fun `hasAnyRole and hasAuthority work`() {
        val id = identity("u-1", "kraken-user")

        assertIs<AuthorizationDecision.Granted>(compile("hasAnyRole('x', 'kraken-user')").check(id, Ping("x"), ctx("u-1"), label))
        assertIs<AuthorizationDecision.Granted>(compile("hasAuthority('ROLE_kraken-user')").check(id, Ping("x"), ctx("u-1"), label))
        assertIs<AuthorizationDecision.Denied>(compile("hasAuthority('ROLE_nope')").check(id, Ping("x"), ctx("u-1"), label))
    }

    @Test fun `message and ctx variables are available`() {
        val auth = compile("hasRole('kraken-user') and #message.ownerId == #ctx.userId")
        val id = identity("u-1", "kraken-user")

        assertIs<AuthorizationDecision.Granted>(auth.check(id, OwnedThing("u-1"), ctx("u-1", "kraken-user"), label))
        assertIs<AuthorizationDecision.Denied>(auth.check(id, OwnedThing("u-2"), ctx("u-1", "kraken-user"), label))
        assertIs<AuthorizationDecision.Granted>(compile("principal.userId == 'u-1'").check(id, Ping("x"), ctx("u-1"), label))
    }

    @Test fun `evaluation error is Denied with handler label`() {
        val denied = assertIs<AuthorizationDecision.Denied>(
            compile("#message.missing == 1").check(identity("u-1"), Ping("x"), ctx("u-1"), label),
        )

        assertTrue(denied.message.startsWith("handler Bean.handle: access denied"), denied.message)
    }

    @Test fun `invalid expression fails compile with label`() {
        val e = assertThrows<IllegalStateException> { compile("hasRole(") }

        assertTrue(e.message!!.contains("Bean.handle"), e.message)
        assertTrue(e.message!!.contains("invalid @RequiresAuthentication expression"), e.message)
    }

    @Test fun `throwing method call is Denied with handler label`() {
        val denied = assertIs<AuthorizationDecision.Denied>(
            compile("#message.text.substring(99) == 'x'").check(identity("u-1"), Ping("x"), ctx("u-1"), label),
        )

        assertTrue(denied.message.startsWith("handler Bean.handle: access denied"), denied.message)
    }

    @Test fun `non boolean result is Denied`() {
        assertIs<AuthorizationDecision.Denied>(compile("'yes'").check(identity("u-1"), Ping("x"), ctx("u-1"), label))
        assertIs<AuthorizationDecision.Denied>(compile("#message.text").check(identity("u-1"), Ping("x"), ctx("u-1"), label))
    }
}
