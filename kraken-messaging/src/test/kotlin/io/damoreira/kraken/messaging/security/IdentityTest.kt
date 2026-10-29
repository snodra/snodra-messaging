package io.damoreira.kraken.messaging.security

import io.damoreira.kraken.messaging.fixtures.TestJwt
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class IdentityTest {

    @AfterEach
    fun clear() = SecurityContextHolder.clearContext()

    private fun install(auth: Authentication?) {
        SecurityContextHolder.getContext().authentication = auth
    }

    @Test
    fun `local identity from JwtAuthenticationToken maps sub and realm roles`() {
        val jwt = TestJwt.decoder().decode(TestJwt.token("u-1", listOf("kraken-user", "kraken-admin")))
        // As a resource server builds it: with authorities, hence authenticated. The one-arg constructor is not.
        val auth = JwtAuthenticationToken(jwt, listOf(SimpleGrantedAuthority("ROLE_kraken-user")))
        install(auth)

        val identity = assertNotNull(LocalIdentity.current())

        assertEquals("u-1", identity.userId)
        assertEquals(setOf("kraken-user", "kraken-admin"), identity.roles)
        assertTrue(identity.principal.verified)
        val normalized = assertIs<KrakenForwardedAuthentication>(identity.authentication)
        assertEquals(jwt.tokenValue, normalized.token)
    }

    @Test
    fun `local identity from forwarded authentication is passed through`() {
        val principal = MessagePrincipal("u-2", setOf("kraken-user"), verified = false)
        install(KrakenForwardedAuthentication("tok", principal))

        val identity = assertNotNull(LocalIdentity.current())

        assertEquals(principal, identity.principal)
    }

    @Test
    fun `local identity is null for anonymous or missing authentication`() {
        install(null)
        assertNull(LocalIdentity.current())

        install(AnonymousAuthenticationToken("key", "anonymousUser", listOf(SimpleGrantedAuthority("ROLE_ANONYMOUS"))))
        assertNull(LocalIdentity.current())
    }

    @Test
    fun `local identity from other authenticated token uses name and ROLE_ authorities`() {
        install(
            UsernamePasswordAuthenticationToken(
                "bob",
                "pw",
                listOf(SimpleGrantedAuthority("ROLE_kraken-admin"), SimpleGrantedAuthority("SCOPE_read")),
            ),
        )

        val identity = assertNotNull(LocalIdentity.current())

        assertEquals("bob", identity.userId)
        assertEquals(setOf("kraken-admin"), identity.roles)
        assertTrue(identity.principal.verified)
    }

    @Test
    fun `forwarded authentication exposes ROLE_ authorities and is authenticated`() {
        val auth = KrakenForwardedAuthentication(null, MessagePrincipal("u-3", setOf("kraken-user"), verified = false))

        assertTrue(auth.isAuthenticated)
        assertEquals(setOf("ROLE_kraken-user"), auth.authorities.map { it.authority }.toSet())
        assertNull(auth.token)
    }

    @Test fun `local identity evaluates realm roles even when the app maps scopes only`() {
        val jwt = TestJwt.decoder().decode(TestJwt.token("u-1", listOf("kraken-admin")))
        install(JwtAuthenticationToken(jwt, listOf(SimpleGrantedAuthority("SCOPE_profile")))) // Spring's default converter shape

        val identity = assertNotNull(LocalIdentity.current())

        assertEquals(setOf("ROLE_kraken-admin"), identity.authentication.authorities.map { it.authority }.toSet())
        assertTrue(identity.authentication.isAuthenticated)
    }
}
