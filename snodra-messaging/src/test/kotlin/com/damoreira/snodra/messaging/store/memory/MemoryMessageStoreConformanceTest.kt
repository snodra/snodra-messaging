package com.damoreira.snodra.messaging.store.memory

import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.test.store.MessageStoreConformanceTest

/** The message-store conformance kit on the in-memory message store, no container: a new store (empty state) per test. */
class MemoryMessageStoreConformanceTest : MessageStoreConformanceTest() {
    override fun store(): MessageStore = MemoryMessageStore()
}
