package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.impl.Client
import java.util.function.ToLongFunction

/**
 * The RabbitMQ Stream Java client this add-on runs on, checked when a [RabbitEventStore] is built: the add-on speaks the client's
 * low-level `com.rabbitmq.stream.impl.Client` (an internal package that changes between versions) and reads two of its
 * package-private methods reflectively, so it requires exactly [REQUIRED] — and Spring Boot's dependency management sets another
 * version (`rabbit-stream-client.version`, 1.6.0 in Boot 4.1) in a service that uses it.
 */
internal object RabbitClientCheck {
    const val REQUIRED = "1.13.1"

    /** A member of the client the add-on uses: a method of [type] named [name] taking [parameters], [declared] when not public. */
    class Member(val type: Class<*>, val name: String, val parameters: List<Class<*>>, val declared: Boolean = false) {
        override fun toString() = "${type.name}.$name(${parameters.joinToString { it.simpleName }})"
    }

    /** What the add-on uses beyond the public API, each checked by [verify]. */
    val USED: List<Member> = listOf(
        Member(Client::class.java, RabbitConnector.ADVERTISED_HOST, emptyList(), declared = true),
        Member(Client::class.java, RabbitConnector.ADVERTISED_PORT, emptyList(), declared = true),
        Member(Client::class.java, "streamStats", listOf(String::class.java)),
        Member(Client::class.java, "queryOffset", listOf(String::class.java, String::class.java)),
        Member(Client::class.java, "storeOffset", listOf(String::class.java, String::class.java, Long::class.javaPrimitiveType!!)),
        Member(
            Client::class.java, "subscribe",
            listOf(Byte::class.javaPrimitiveType!!, String::class.java, OffsetSpecification::class.java, Int::class.javaPrimitiveType!!, Map::class.java),
        ),
        Member(Client::class.java, "credit", listOf(Byte::class.javaPrimitiveType!!, Int::class.javaPrimitiveType!!)),
        Member(Client::class.java, "unsubscribe", listOf(Byte::class.javaPrimitiveType!!)),
        Member(Client::class.java, "declarePublisher", listOf(Byte::class.javaPrimitiveType!!, String::class.java, String::class.java)),
        Member(Client::class.java, "publish", listOf(Byte::class.javaPrimitiveType!!, List::class.java, ToLongFunction::class.java)),
        Member(Client::class.java, "partitions", listOf(String::class.java)),
        Member(Client::class.java, "metadata", listOf(Array<String>::class.java)),
        Member(Client.ClientParameters::class.java, "messageIgnoredListener", listOf(Client.MessageIgnoredListener::class.java)),
        Member(Client.ClientParameters::class.java, "consumerUpdateListener", listOf(Client.ConsumerUpdateListener::class.java)),
        Member(Client.ClientParameters::class.java, "rpcTimeout", listOf(java.time.Duration::class.java)),
        Member(Class.forName("com.rabbitmq.stream.codec.InternalCodec"), "messageBuilder", emptyList()),
    )

    /** The version of the client on the classpath (its jar's `Implementation-Version`). */
    val found: String? get() = Client::class.java.`package`?.implementationVersion

    /** Throws [IllegalStateException] naming the fix when [version] is not [REQUIRED] or a member of [members] is missing. */
    fun verify(version: String? = found, members: List<Member> = USED) {
        if (version != REQUIRED) throw IllegalStateException(fix("com.rabbitmq:stream-client ${version ?: "of an unknown version"} is on the classpath"))
        val missing = members.filter { m ->
            runCatching {
                if (m.declared) m.type.getDeclaredMethod(m.name, *m.parameters.toTypedArray()) else m.type.getMethod(m.name, *m.parameters.toTypedArray())
            }.isFailure
        }
        if (missing.isNotEmpty()) throw IllegalStateException(fix("com.rabbitmq:stream-client lacks $missing"))
    }

    private fun fix(what: String) =
        "kraken-messaging-rabbitmq needs com.rabbitmq:stream-client $REQUIRED exactly, but $what. Spring Boot's dependency management " +
            "sets its own version: set rabbit-stream-client.version to $REQUIRED (Gradle: extra[\"rabbit-stream-client.version\"] = \"$REQUIRED\"; " +
            "Maven: <properties><rabbit-stream-client.version>$REQUIRED</rabbit-stream-client.version></properties>)."
}
