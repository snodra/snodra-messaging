package com.damoreira.snodra.messaging.rabbitmq

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** The client the add-on requires: exactly 1.13.1, with every member it uses beyond the public API. */
class RabbitClientCheckTest {
    /** Loads like the test's own class loader, but has no [missing] class (a client version without it). */
    private class Without(private val missing: String) : ClassLoader(RabbitClientCheckTest::class.java.classLoader) {
        override fun loadClass(name: String, resolve: Boolean): Class<*> =
            if (name == missing) throw ClassNotFoundException(name) else super.loadClass(name, resolve)
    }

    @Test
    fun `the client on the classpath passes the check`() {
        assertEquals(listOf("1.13.1", "1.13.1"), RabbitClientCheck.versions(), "the jar's Implementation-Version, then its pom.properties")
        RabbitClientCheck.verify()
    }

    @Test
    fun `another version fails naming the property that fixes it`() {
        val failure = assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify(listOf("1.6.0")) }
        for (text in listOf("1.13.1 exactly", "1.6.0", "extra[\"rabbit-stream-client.version\"] = \"1.13.1\"", "<rabbit-stream-client.version>1.13.1</rabbit-stream-client.version>")) {
            assertTrue(text in failure.message!!, failure.message)
        }
        val unknown = assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify(emptyList()) }
        assertTrue("of an unknown version" in unknown.message!!, unknown.message)
    }

    @Test
    fun `the version is checked before any member is looked up`() {
        // Another version whose classes differ: the version message, never a class-loading error from the member list.
        val failure = assertThrows(IllegalStateException::class.java) {
            RabbitClientCheck.verify(listOf("1.6.0"), members = { error("the members were built before the version was checked") })
        }
        assertTrue("1.6.0" in failure.message!!, failure.message)
        val withoutCodec = Without("com.rabbitmq.stream.codec.InternalCodec")
        val versionFirst = assertThrows(IllegalStateException::class.java) { RabbitClientCheck.verify(listOf("1.6.0"), loader = withoutCodec) }
        assertTrue("1.6.0" in versionFirst.message!!, versionFirst.message)
    }

    @Test
    fun `a class missing from the client fails the check naming it, not with a class-loading error`() {
        val failure = assertThrows(IllegalStateException::class.java) {
            RabbitClientCheck.verify(listOf("1.13.1"), loader = Without("com.rabbitmq.stream.impl.Client\$MessageIgnoredListener"))
        }
        assertTrue("messageIgnoredListener" in failure.message!!, failure.message)
        assertTrue("lacks" in failure.message!!, failure.message)
    }

    @Test
    fun `a member missing from the client fails the check`() {
        val missing = RabbitClientCheck.Member("com.rabbitmq.stream.impl.Client", "serverAdvertisedHostname", declared = true)
        val failure = assertThrows(IllegalStateException::class.java) {
            RabbitClientCheck.verify(listOf(RabbitClientCheck.REQUIRED), members = { RabbitClientCheck.used() + missing })
        }
        assertTrue("serverAdvertisedHostname" in failure.message!!, failure.message)
    }

    @Test
    fun `a jar without Implementation-Version is read from its pom properties`() {
        val pom = "artifactId=stream-client\ngroupId=com.rabbitmq\nversion=1.13.1\n"
        assertEquals(listOf("1.13.1"), RabbitClientCheck.versions(null) { pom.byteInputStream() }, "a shaded or merged jar")
        RabbitClientCheck.verify(RabbitClientCheck.versions(null) { pom.byteInputStream() })
        assertEquals(listOf("1.6.0", "1.13.1"), RabbitClientCheck.versions("1.6.0") { pom.byteInputStream() })
        RabbitClientCheck.verify(listOf("1.6.0", "1.13.1")) // one of them says 1.13.1
        assertEquals(emptyList<String>(), RabbitClientCheck.versions(null) { null })
    }
}
