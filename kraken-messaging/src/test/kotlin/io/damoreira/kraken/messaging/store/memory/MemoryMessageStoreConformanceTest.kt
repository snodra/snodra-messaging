package io.damoreira.kraken.messaging.store.memory

import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.test.store.MessageStoreConformanceTest

/** The message-store conformance kit on the in-memory message store, no container: a new store (empty state) per test. */
class MemoryMessageStoreConformanceTest : MessageStoreConformanceTest() {
    override fun store(): MessageStore = MemoryMessageStore()
}
