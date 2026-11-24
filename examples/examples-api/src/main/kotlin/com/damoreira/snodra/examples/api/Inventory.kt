package com.damoreira.snodra.examples.api

import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.api.Query

/*
 * A shared query type: service-b (Kotlin) and service-c both own it, so a point query goes to one
 * of them and a scatter-gather asks both. Service-c keeps a mirror of these types; keep both
 * in step.
 */

/** Shared query owned by service-b and service-c: how many of [item] the answering instance holds. */
@MessageType("examples.Inventory")
data class Inventory(val item: String) : Query<InventoryLevel>

/**
 * Reply to [Inventory]: the answering [service], its [instance] (the container's host name) and the [count] it holds
 * of [item] (a fixed number per service); [answered] counts the Inventory queries that instance answered so far.
 */
data class InventoryLevel(val service: String, val instance: String, val item: String, val count: Long, val answered: Long)
