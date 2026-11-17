package com.damoreira.snodra.messaging.rabbitmq

import com.rabbitmq.stream.impl.Client
import java.io.InputStream
import java.util.Properties

/**
 * The RabbitMQ Stream Java client this add-on runs on, checked when a [RabbitEventStore] is built: the add-on speaks the client's
 * low-level `com.rabbitmq.stream.impl.Client` (an internal package that changes between versions) and reads two of its
 * package-private methods reflectively, so it requires exactly [REQUIRED] — and Spring Boot's dependency management sets another
 * version (`rabbit-stream-client.version`, 1.6.0 in Boot 4.1) in a service that uses it.
 *
 * The version is checked first; only then are the members the add-on uses looked up, by name, so a class another client
 * version lacks is reported in the check's message, never as a class-loading error.
 */
internal object RabbitClientCheck {
    const val REQUIRED = "1.13.1"

    /** Where a Maven-built jar (and a shaded or merged one that kept it) says its version. */
    const val POM = "META-INF/maven/com.rabbitmq/stream-client/pom.properties"

    /**
     * A member of the client the add-on uses: a method of the class named [type] named [name] taking [parameters] (class names;
     * `byte`, `int`, `long` for primitives, a `[]` suffix for arrays), [declared] when not public.
     */
    class Member(val type: String, val name: String, val parameters: List<String> = emptyList(), val declared: Boolean = false) {
        override fun toString() = "$type.$name(${parameters.joinToString { it.substringAfterLast('.') }})"
    }

    private const val CLIENT = "com.rabbitmq.stream.impl.Client"
    private const val PARAMETERS = "$CLIENT\$ClientParameters"

    /** What the add-on uses beyond the public API, each checked by [verify]. */
    fun used(): List<Member> = listOf(
        Member(CLIENT, RabbitConnector.ADVERTISED_HOST, declared = true),
        Member(CLIENT, RabbitConnector.ADVERTISED_PORT, declared = true),
        Member(CLIENT, "streamStats", listOf("java.lang.String")),
        Member(CLIENT, "queryOffset", listOf("java.lang.String", "java.lang.String")),
        Member(CLIENT, "storeOffset", listOf("java.lang.String", "java.lang.String", "long")),
        Member(CLIENT, "subscribe", listOf("byte", "java.lang.String", "com.rabbitmq.stream.OffsetSpecification", "int", "java.util.Map")),
        Member(CLIENT, "credit", listOf("byte", "int")),
        Member(CLIENT, "unsubscribe", listOf("byte")),
        Member(CLIENT, "declarePublisher", listOf("byte", "java.lang.String", "java.lang.String")),
        Member(CLIENT, "publish", listOf("byte", "java.util.List", "java.util.function.ToLongFunction")),
        Member(CLIENT, "partitions", listOf("java.lang.String")),
        Member(CLIENT, "metadata", listOf("java.lang.String[]")),
        Member(PARAMETERS, "messageIgnoredListener", listOf("$CLIENT\$MessageIgnoredListener")),
        Member(PARAMETERS, "consumerUpdateListener", listOf("$CLIENT\$ConsumerUpdateListener")),
        Member(PARAMETERS, "rpcTimeout", listOf("java.time.Duration")),
        Member("com.rabbitmq.stream.codec.InternalCodec", "messageBuilder"),
    )

    /**
     * What the classpath says the client's version is: its jar's `Implementation-Version`, then its [POM] (a shaded or merged jar
     * may have lost the first).
     */
    fun versions(
        implementationVersion: String? = Client::class.java.`package`?.implementationVersion,
        pom: () -> InputStream? = { Client::class.java.getResourceAsStream("/$POM") },
    ): List<String> {
        val fromPom = runCatching { pom()?.use { Properties().apply { load(it) }.getProperty("version") } }.getOrNull()
        return listOfNotNull(implementationVersion, fromPom?.trim()?.takeIf { it.isNotEmpty() })
    }

    /**
     * Throws [IllegalStateException] naming the fix when none of [versions] is [REQUIRED], or (checked only then) a member of
     * [members] is missing from the classes [loader] loads.
     */
    fun verify(
        versions: List<String> = versions(),
        members: () -> List<Member> = ::used,
        loader: ClassLoader = RabbitClientCheck::class.java.classLoader,
    ) {
        if (REQUIRED !in versions) {
            val found = versions.distinct().joinToString(" / ").ifEmpty { "of an unknown version" }
            throw IllegalStateException(fix("com.rabbitmq:stream-client $found is on the classpath"))
        }
        val missing = members().filter { m -> runCatching { find(m, loader) }.isFailure }
        if (missing.isNotEmpty()) throw IllegalStateException(fix("com.rabbitmq:stream-client lacks $missing"))
    }

    private fun find(m: Member, loader: ClassLoader) {
        val type = type(m.type, loader)
        val parameters = m.parameters.map { type(it, loader) }.toTypedArray()
        if (m.declared) type.getDeclaredMethod(m.name, *parameters) else type.getMethod(m.name, *parameters)
    }

    private fun type(name: String, loader: ClassLoader): Class<*> = when {
        name.endsWith("[]") -> java.lang.reflect.Array.newInstance(type(name.removeSuffix("[]"), loader), 0).javaClass
        name == "byte" -> java.lang.Byte.TYPE
        name == "int" -> java.lang.Integer.TYPE
        name == "long" -> java.lang.Long.TYPE
        else -> Class.forName(name, false, loader)
    }

    private fun fix(what: String) =
        "snodra-messaging-rabbitmq needs com.rabbitmq:stream-client $REQUIRED exactly, but $what. Spring Boot's dependency management " +
            "sets its own version: set rabbit-stream-client.version to $REQUIRED (Gradle: extra[\"rabbit-stream-client.version\"] = \"$REQUIRED\"; " +
            "Maven: <properties><rabbit-stream-client.version>$REQUIRED</rabbit-stream-client.version></properties>)."
}
