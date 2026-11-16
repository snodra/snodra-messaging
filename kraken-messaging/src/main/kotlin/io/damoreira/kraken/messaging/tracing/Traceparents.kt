package io.damoreira.kraken.messaging.tracing

/** The W3C `traceparent` values the trace floor forwards (version `00`): the others are dropped, never passed on. */
object Traceparents {
    private val FORMAT = Regex("^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$")
    private val ZERO_TRACE = "0".repeat(32)
    private val ZERO_PARENT = "0".repeat(16)

    /**
     * [value] when it is a valid W3C `traceparent`: `00-<32 hex>-<16 hex>-<2 hex>`, lower case, neither the trace id nor the
     * parent id all zeros; else null.
     */
    fun valid(value: String?): String? {
        val match = value?.let(FORMAT::matchEntire) ?: return null
        val (traceId, parentId) = match.destructured
        return value.takeIf { traceId != ZERO_TRACE && parentId != ZERO_PARENT }
    }
}
