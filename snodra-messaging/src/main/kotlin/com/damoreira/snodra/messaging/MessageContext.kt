package com.damoreira.snodra.messaging

/** Metadata of the message currently being handled; optionally the second handler parameter. */
data class MessageContext(
    val messageId: String,
    val type: String,
    val correlationId: String,
    val userId: String?,
    val roles: Set<String>,
    val authorization: String?,
    val headers: Map<String, String>,
)
