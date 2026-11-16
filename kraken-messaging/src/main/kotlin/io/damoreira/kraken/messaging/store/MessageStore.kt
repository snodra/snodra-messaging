package io.damoreira.kraken.messaging.store

import org.springframework.transaction.PlatformTransactionManager

/**
 * Where the library keeps its state (ADR 0028): the outbox, the processed-event markers, the dead letters, the processors'
 * control documents, the external calls and the sagas, all in the service's own database. One add-on per database
 * implements every store, so a service never needs a second database for any of them.
 *
 * **Transactions are Spring's.** The core runs a `TransactionTemplate` over [transactions], and every store method that
 * writes in the caller's transaction ([OutboxStore.append], [ProcessedEventStore.record], [DeadLetterStore.deleteInTransaction],
 * the saga ports) joins the transaction active on the calling thread, so the outbox entry and the marker commit or roll
 * back with the service's own writes. Spring binds that transaction to the thread: an implementation runs its work on the
 * calling thread (a `suspend` method never switches dispatchers to reach the database). With no active transaction such a
 * write commits on its own.
 *
 * **Times are the application's.** A store stamps `sentAt`, leases, `updatedAt` and the like with the application clock (the
 * JVM's), never the database's, so the margins the conformance kit uses hold whatever the database's own clock says.
 *
 * Payloads cross these interfaces as JSON bytes, never as a database's own document type. `MessageStoreConformanceTest`
 * (in `kraken-messaging-test`) is the behaviour every implementation must pass.
 */
interface MessageStore {
    /** The add-on's name, for logs and the startup report (`mongo`, `jdbc`, `memory`, or a third party's). */
    val name: String

    val outbox: OutboxStore

    val processedEvents: ProcessedEventStore

    val deadLetters: DeadLetterStore

    val processorControl: ProcessorControlStore

    val externalCalls: ExternalCallStore

    val sagas: SagaStores

    /** The transaction manager of the service's database, which the stores above join. */
    val transactions: PlatformTransactionManager

    /**
     * Creates what the stores need in the database (indexes, tables), idempotently. The core calls it once at startup, after
     * every bean exists and before any lifecycle starts, unless `kraken.messaging.store.init-schema` is false (then the
     * service applies the schema itself). A store without a schema keeps the default, which does nothing.
     */
    fun initSchema() = Unit

    /**
     * With `kraken.messaging.store.init-schema` false: fails (an [IllegalStateException] saying what to apply) unless the database
     * has what the stores need. The core calls it once at startup, after every bean exists (so a bean that migrates the schema
     * while it is created always comes first) and before any lifecycle starts. A store without a schema keeps the default.
     */
    fun checkSchema() = Unit
}
