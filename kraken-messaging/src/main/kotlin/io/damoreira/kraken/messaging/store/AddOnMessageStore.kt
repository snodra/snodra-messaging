package io.damoreira.kraken.messaging.store

/**
 * A [MessageStore] an add-on's auto-configuration provides (the MongoDB one, the SQL one, a third party's). An add-on's store
 * steps back for any other [MessageStore] bean (the application's own), but not for another add-on's: two add-ons on the
 * classpath without `kraken.messaging.store.type` fail the startup naming both, instead of one of them winning silently. The
 * implementing class declares its name as a constant `NAME` (`const val NAME` in its companion object), which that message
 * shows.
 */
interface AddOnMessageStore : MessageStore
