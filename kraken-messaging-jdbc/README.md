# kraken-messaging-jdbc: the SQL message store (PostgreSQL and MySQL)

The message store add-on (ADR 0028) for services whose database is **PostgreSQL 13 or later** or **MySQL 8.0 or later
(InnoDB)**: the outbox, the processed-event markers, the dead letters, the processor control, the external calls and the sagas
live in tables `kraken_*` of the service's own database, written through plain JDBC (Spring's `JdbcClient`, no JPA, no ORM) in
the service's own transaction. One module, one dialect per database (`PostgresDialect`, `MySqlDialect`).

```kotlin
dependencies {
    implementation("io.damoreira.kraken:kraken-messaging:0.1.0-SNAPSHOT")
    implementation("io.damoreira.kraken:kraken-messaging-jdbc:0.1.0-SNAPSHOT")
    runtimeOnly("org.postgresql:postgresql")   // or com.mysql:mysql-connector-j
}
```

With a `DataSource` in the application (Boot's `spring.datasource.*`) that is all: the store is created, its schema applied at
startup and its retention run every minute. The Rust twin (`kraken-messaging-sqlx`) reads and writes the same tables, so a
Kotlin service and a Rust service can share one database layout.

## Requirements

| | PostgreSQL | MySQL |
|---|---|---|
| Server | 13 or later | 8.0 or later, InnoDB (MariaDB is not supported yet) |
| Driver | `org.postgresql:postgresql` | `com.mysql:mysql-connector-j`, with its default `useAffectedRows=false` |
| Claims | `SELECT ... FOR UPDATE SKIP LOCKED` (9.5+) | `SELECT ... FOR UPDATE SKIP LOCKED` and `NOWAIT` (8.0+) |
| Relay wake-up | `LISTEN kraken_outbox` / `NOTIFY` | none: the relay polls |
| Tested on | `postgres:17.7` | `mysql:8.4.7` |

The startup fails, naming what is supported, on another database, on MariaDB, and on an older server: `The kraken message store
needs PostgreSQL 13.0 or later; this server is PostgreSQL 12.22`.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `kraken.messaging.store.type` | the only one | `jdbc` chooses this store when another one is available too (the MongoDB add-on, say); naming another store (`mongo`, `memory`) makes this one step back. Two stores without the setting fail the startup. |
| `kraken.messaging.store.init-schema` | `true` | Apply the schema at startup (see [Schema](#schema)); `false` when the service runs the migration itself. |
| `kraken.messaging.store.jdbc.dialect` | detected | `postgresql` or `mysql`; unset, the dialect is the DataSource's database product. Another value fails the startup naming the supported ones. |
| `kraken.messaging.store.jdbc.purge-interval` | `1m` | How often the retention runs ([Retention](#retention)); `0` turns it off (the service purges itself). |
| `kraken.messaging.outbox.retention`, `idempotency.retention`, `external.calls.retention`, `sagas.ended-retention`, `sagas.dispatch-retention`, `sagas.failed-retention`, `deadlines.retention`, `events.dead-letter.retention` | the core's | What the purge keeps, as the MongoDB add-on's TTL indexes. |

## Transactions

The store **joins the service's transaction**. Its writes that belong with the service's (an outbox entry, a processed marker,
a dead letter's deletion in a retry, the external calls, the saga state) run on the connection Spring binds to the thread for
the `DataSource`. Both of Spring's JDBC-capable managers bind it:

- `DataSourceTransactionManager`: plain JDBC, `JdbcTemplate` / `JdbcClient`, jOOQ, Exposed;
- `JpaTransactionManager`: JPA (Hibernate) over the same `DataSource`.

So `@Transactional` business writes, the outbox entry and the marker commit together or roll back together, whichever manager
the service runs. The store's transaction manager (`MessageStore.transactions`, which the library's own transactions use) is the
application's: Boot's auto-configured one, or the service's own bean. Only an application without any transaction manager gets
one from this add-on, a `DataSourceTransactionManager`, which then backs its `@Transactional` methods too (the MongoDB add-on
decides the same way).

- A transaction of **another** manager active on the thread (a second DataSource's, a MongoDB one, JTA) does not hold this
  DataSource's connection: a joined write then fails with `IllegalStateException` ("A transaction is active, but not on the
  message store's DataSource") instead of committing beside it.
- Without a transaction, each write commits on its own.
- A write from a transaction's `afterCommit` callback commits on its own (the transaction it came from has committed).
- What must commit on its own whatever the caller has (dead letters but their deletion in a retry, processor control, the
  relays' and schedulers' claims, the purge) runs in a transaction of its own on a second connection from the same pool.

**Conflicts** surface as the exceptions the saga runtime retries: a duplicate key as `DuplicateKeyException`, a lock that is not
available, a deadlock or a serialization failure as a `TransientDataAccessException` (`CannotAcquireLockException`,
`PessimisticLockingFailureException`). Two transactions writing one saga never wait for each other: an update locks the saga's row
with `FOR UPDATE NOWAIT`, and the start of a saga with a live start key takes, on PostgreSQL, the transaction-scoped advisory lock
`pg_try_advisory_xact_lock(1263684430, hashtext(start_key))` and, on MySQL, locks the live row with that key `FOR UPDATE NOWAIT`;
the loser fails at once with `CannotAcquireLockException` (or a deadlock on MySQL), and the runtime retries it.

## Competing relays and schedulers

A claim (the outbox relay's, the deadline scheduler's, the saga relay's failure notices) runs in a transaction of its own:
`SELECT ... ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED` picks the rows that are free, already the claimer's or whose lease passed,
then an `UPDATE` makes them the claimer's until its lease ends (`owner`, `lease_until`; `claimed_by`, `claimed_until`;
`notice_next_at`). A concurrent claim skips the rows this one locked and, once it committed, no longer matches them; a crashed
claimer's rows are claimed again after its lease. The outbox is claimed in `(created_at, seq, id)` order, and an entry the
claimer still holds comes back in its place, so an entry whose send failed is retried before the later ones of its key (order
per key, for what one instance publishes). The saga dispatch lease (`kraken_saga_dispatch_leases`) is one row per saga, taken by an
upsert conditional on the lease having passed.

Times are the application's clock, never the database's `now()`, at millisecond precision.

## Relay wake-up

**PostgreSQL:** every append runs `SELECT pg_notify('kraken_outbox', '')` in its transaction, so the notification is delivered when
it commits and never after a rollback (PostgreSQL folds the notifications of one transaction into one). While an outbox relay
runs it listens: `LISTEN kraken_outbox` on a connection of its own, and every notification makes it claim at once instead of at
its next poll (`kraken.messaging.outbox.poll-interval`). The connection is a dedicated one built from Boot's
`JdbcConnectionDetails` (outside the pool) when the application has them, else one borrowed from the pool for 30 s at a time and
given back, so a pooled connection is never held for good. A lost connection is logged once and listened again with a backoff
(200 ms to 30 s); meanwhile the relay polls.

**MySQL** has no notification: the relay polls, and `OutboxStore.onAppend` does nothing.

## Schema

The schema files are on the classpath: `db/kraken/postgresql/V1__kraken.sql` and `db/kraken/mysql/V1__kraken.sql`. With
`kraken.messaging.store.init-schema` on (the default), the store applies version 1 at startup **unless the database has it**
(`kraken_schema` holds version 1 or later: then the database is only read). Instances starting together apply it once: the first
takes a lock (PostgreSQL `pg_advisory_lock`, MySQL `GET_LOCK('kraken_schema', 60)`), the others wait and find it applied. On
PostgreSQL the file runs in one transaction; on MySQL each DDL statement commits on its own, and the file is idempotent
(`CREATE TABLE IF NOT EXISTS`, the version row inserted unless it exists), so a run interrupted halfway completes the next time.

**Running the migration yourself** (Flyway, Liquibase, a DBA): set `kraken.messaging.store.init-schema=false` and point the tool at
the same file. The files carry Flyway's naming (`V1__kraken.sql`), and this module does not depend on Flyway:

```yaml
spring:
  flyway:
    locations: classpath:db/migration,classpath:db/kraken/postgresql   # or db/kraken/mysql
kraken:
  messaging:
    store:
      init-schema: false
```

A tool that keeps its own history works as well (Liquibase: a `sqlFile` change set over the file; the file is idempotent, so a
change set that runs it twice is harmless). Later schema versions come as `V2__...` files next to it.

Identifiers and positions compare by their bytes: `COLLATE "C"` on PostgreSQL, `utf8mb4_bin` tables on MySQL. No name needs
quoting in either database. Times are `TIMESTAMPTZ(3)` on PostgreSQL and `DATETIME(3)` holding UTC on MySQL.

### Payloads

The outbox payload and every header map are the database's JSON type (`jsonb`, `JSON`): an outbox payload is a JSON object
whose integers fit in a signed 64-bit integer (the store refuses anything else before it writes), and both types keep such
integers exact (`jsonb` as `numeric`, MySQL as `BIGINT`), so the JSON type costs nothing and lets an operator query the outbox.
What the library must read back exactly as written is text: the saga state (`BigDecimal` scales, key order), the saga
dispatch, deadline and failure payloads, the external calls' reply and error, and the dead letters' payload (which may not be
JSON at all: an INVALID letter's; a payload that is not UTF-8 goes to `payload_binary`). A JSON column normalises spacing and key
order (and, on MySQL, decimals), so those are `TEXT` / `MEDIUMTEXT`.

### Tables

`kraken_schema`: the schema versions applied (`version` PK, `applied_at`).

`kraken_outbox`: events waiting for the relay (ADR 0005).

| Column | PostgreSQL / MySQL | |
|---|---|---|
| `id` | `VARCHAR(512)` PK | the event id |
| `type`, `event_key` | `TEXT` | the event type, its order key (null: none) |
| `headers`, `payload` | `JSONB` / `JSON` | header map; the event (a JSON object) |
| `created_at`, `seq` | timestamp, `BIGINT` | the order: `(created_at, seq)` |
| `attempts` | `INTEGER` | failed sends |
| `sent_at`, `failed_at` | timestamp | sent; given up on (both null: pending) |
| `last_error` | `TEXT` | the last send failure, for diagnosis |
| `owner`, `lease_until` | `TEXT`, timestamp | the relay holding it, until when (null: free) |

Indexes: PostgreSQL `kraken_outbox_pending (created_at, seq) WHERE sent_at IS NULL AND failed_at IS NULL` and
`kraken_outbox_sent (sent_at) WHERE sent_at IS NOT NULL`; MySQL `kraken_outbox_pending (sent_at, failed_at, created_at, seq)`.

`kraken_processed_events`: the processed markers (`processor:<name>:<eventId>`, `saga:<type>:<eventId>`, `ingress:...`).

| Column | | |
|---|---|---|
| `id` | `VARCHAR(512)` PK | **one marker per id**: what makes a redelivered event a duplicate |
| `type`, `processed_at` | `TEXT`, timestamp | the event type; when handled (the retention counts from it) |
| `processor`, `source`, `segment`, `position` | `VARCHAR(200)` | a stage-1 marker's processor and record (null on the others) |
| `position_num` | `BIGINT` | the position as a number when it is a canonical decimal one (null otherwise) |

Indexes: `kraken_processed_events_range (processor, source, segment, position_num)` (a reset deletes a range by value; partial
`WHERE processor IS NOT NULL` on PostgreSQL), `kraken_processed_events_processed_at (processed_at)`.

`kraken_dead_letters` (ADR 0022): `id` `VARCHAR(512)` PK (`<processor>:<source>:<segment>:<position>`, parked once); `processor`,
`source`, `segment`, `position` `VARCHAR(200)`; `position_num` `BIGINT`; `event_key` `VARCHAR(512)`; `event_id`, `event_type`
`TEXT`; `headers` JSON; `payload` `TEXT`/`MEDIUMTEXT`; `payload_binary` `BYTEA`/`LONGBLOB`; `reason` `VARCHAR(32)` (`HANDLER_FAILED`,
`SEQUENCED`, `INVALID`); `error_type`, `error_message`, `blocked_by` `TEXT`; `attempts` `INTEGER`; `parked_at`, `last_attempt_at`,
`expire_at` timestamps; `replay` `BOOLEAN`. Indexes: `kraken_dead_letters_key (processor, event_key, position_num)` (a key's head:
decimal positions by value first, then the others by their bytes, ties by id), `kraken_dead_letters_parked (processor, parked_at)`,
`kraken_dead_letters_expire (expire_at)`. A letter past its `expire_at` is gone at once for every read (it no longer blocks its
key), and the purge deletes it.

`kraken_processor_control` (ADR 0023): `processor` `VARCHAR(200)` PK (one row per processor); `paused` `BOOLEAN`, `paused_at`,
`paused_by`; the reset operation holding the lock: `op_id`, `op_kind`, `op_lease_until`, `op_prepared_at` (all null: free); `replay`
JSON (the open ranges, `[{"source","segment","from","until"}]` strings, in order); `updated_at`.

`kraken_external_calls`: `id` `VARCHAR(512)` PK (the idempotency key: **one record per key**); `status` `VARCHAR(32)`
(`IN_PROGRESS`, `SUCCEEDED`, `FAILED`, `UNKNOWN`); `reply`, `error` text (JSON as written); `attempts`; `updated_at` (retention);
`lease_until`, `owner` (the call in progress). Index `kraken_external_calls_updated (updated_at)`.

`kraken_sagas` (ADR 0021): `id` `VARCHAR(512)` PK; `type` `VARCHAR(200)`; `state` text (JSON as written); `start_key`
`VARCHAR(512)`; `ended` `BOOLEAN`, `ended_at`; `version` `BIGINT` (optimistic, checked by every update); `created_at`,
`updated_at`; `live_start_key`, a stored generated column (`CASE WHEN ended THEN NULL ELSE start_key END`) with the unique index
`kraken_sagas_live_start_key`: **one live saga per start key** (an ended saga frees it; several nulls are allowed). Indexes
`kraken_sagas_type_ended (type, ended)`, `kraken_sagas_ended_at (ended_at)`.

`kraken_saga_associations`: `saga_id` (FK to `kraken_sagas`, `ON DELETE CASCADE`), `idx` (the association's place, from 0),
`assoc_key` `VARCHAR(200)`, `assoc_value` `VARCHAR(512)`; PK `(saga_id, idx)`; index `kraken_saga_associations_kv (assoc_key,
assoc_value)`: what routes an event to the live sagas of a type.

`kraken_saga_dispatch`: `id` `VARCHAR(512)` PK; `saga_id`, `seq` (order per saga); `kind` (`COMMAND`, `EVENT`); `type`; `payload` text;
`headers` JSON; `routing_key`; `created_at`; `dispatched_at`; `attempts`, `next_attempt_at`; `failed_at`, `failure_reason`,
`failure_message`; `notice_pending` `BOOLEAN`, `notice_attempts`, `notice_next_at` (the failure notice). Indexes `(saga_id, seq)`,
the pending ones (PostgreSQL partial `(saga_id, seq) WHERE dispatched_at IS NULL AND failed_at IS NULL`; MySQL `(dispatched_at,
failed_at, saga_id, seq)`), the notices (`notice_next_at` where pending; MySQL `(notice_pending, notice_next_at)`), and the
retention's `dispatched_at` and `failed_at`.

`kraken_saga_dispatch_leases`: `saga_id` `VARCHAR(512)` PK (**one dispatch relay per saga**), `owner`, `lease_until`.

`kraken_deadlines`: `id` `VARCHAR(512)` PK; `saga_type`; `saga_id`; `name` `VARCHAR(200)`; `payload` text, `payload_type`; `due_at`;
`correlation_id`; `status` (`SCHEDULED`, `FIRED`, `CANCELLED`); `claimed_by`, `claimed_until`; `finished_at`. Indexes
`kraken_deadlines_due (status, due_at)`, `kraken_deadlines_saga (saga_id, status)`, `kraken_deadlines_finished (finished_at)`.

`kraken_saga_failures`: `id` `VARCHAR(512)` PK (`<sagaType>:<eventId>`); `saga_type` `VARCHAR(200)`; `event_id`, `event_type`;
`payload` text; `headers` JSON; `error`; `attempts`; `first_failed_at`, `last_failed_at`. Indexes `(saga_type, last_failed_at)`,
`(last_failed_at)`.

## Retention

The MongoDB add-on expires documents through TTL indexes; SQL has none, so this add-on runs a purge every
`kraken.messaging.store.jdbc.purge-interval` (`JdbcMessageStore.purgeExpired()`, in batches of 1 000 rows, each its own
transaction): sent outbox entries, processed markers, external calls, ended sagas (their associations with them), dispatched and
failed saga-dispatch entries, fired or cancelled deadlines and saga failures older than their retention, expired dead letters and
dispatch leases that passed. Pending outbox entries, live sagas and scheduled deadlines are never purged. Every instance runs
it; concurrent purges delete disjoint rows or nothing.

## Limits

- Column sizes: ids and keys up to 512 characters, names (processors, sources, segments, positions, saga types, deadline names)
  up to 200: what fits MySQL's 3 072-byte index keys in `utf8mb4`. A longer value fails its write.
- PostgreSQL's `jsonb` refuses the character `\u0000` in a string: an outbox payload holding one cannot be appended there.
- On MySQL under `READ COMMITTED` (no gap locks), two transactions starting one saga with the same start key at the same time
  make the second wait for the first (then fail as a duplicate) instead of failing at once; under the default
  `REPEATABLE READ` the loser fails at once.
- A write that commits on its own (a dead letter parked, a processor's pause) made inside the service's transaction takes a
  second connection from the pool for its own short transaction: size the pool for it.
- The listening connection of the PostgreSQL wake-up is one per store; with no `JdbcConnectionDetails` it is one of the pool's
  connections most of the time while a relay runs.

## Tests

`PostgresMessageStoreConformanceTest` and `MySqlMessageStoreConformanceTest` run the store conformance kit
(`MessageStoreConformanceTest`, every check) on `postgres:17.7` and `mysql:8.4.7`; the transaction-join tests
(`joins_datasource_transaction`, `joins_jpa_transaction`), the schema, purge and wake-up (`notify_wakes_relay_before_poll`) tests
run on both databases (the wake-up on PostgreSQL). `examples/orders-sql` is a service on PostgreSQL with this store and an
end-to-end test through the hub.
