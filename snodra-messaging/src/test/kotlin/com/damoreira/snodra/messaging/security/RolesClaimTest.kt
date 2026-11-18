package com.damoreira.snodra.messaging.security

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The roles claim is configuration: a dotted path into the claims, or `scope`. */
class RolesClaimTest {
    @Test
    fun `the default claim is roles`() {
        assertEquals(setOf("snodra-user", "snodra-admin"), RolesClaim.read(mapOf("roles" to listOf("snodra-user", "snodra-admin")), "roles"))
    }

    @Test
    fun `a dotted path reads a nested claim`() {
        val claims = mapOf("realm_access" to mapOf("roles" to listOf("snodra-user")), "roles" to listOf("ignored"))
        assertEquals(setOf("snodra-user"), RolesClaim.read(claims, "realm_access.roles"))
    }

    @Test
    fun `scope is space separated`() {
        assertEquals(setOf("orders.read", "orders.write"), RolesClaim.read(mapOf("scope" to "orders.read orders.write"), "scope"))
    }

    @Test
    fun `an absent claim yields no roles`() {
        assertTrue(RolesClaim.read(mapOf("sub" to "u-1"), "roles").isEmpty())
        assertTrue(RolesClaim.read(mapOf("realm_access" to "not-an-object"), "realm_access.roles").isEmpty())
    }
}
