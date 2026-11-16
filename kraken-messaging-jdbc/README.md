# kraken-messaging-jdbc: the SQL message store (PostgreSQL and MySQL)

The message store add-on (ADR 0028) for services whose database is **PostgreSQL 13 or later** or **MySQL 8.0.17 or later
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
| Server | 13 or later | 8.0.17 or later, InnoDB, a STRICT `sql_mode` (`STRICT_TRANS_TABLES`, the default, or `STRICT_ALL_TABLES`); MariaDB is not supported yet |
| Driver | `org.postgresql:postgresql` | `com.mysql:mysql-connector-j`, with its default `useAffectedRows=false` |
| Connection pool | at least 2 connections | at least 2 connections |
| Claims | `SELECT ... FOR UPDATE SKIP LOCKED` | `SELECT ... FOR UPDATE SKIP LOCKED` and `NOWAIT` |
| Relay wake-up | `LISTEN kraken_outbox` / `NOTIFY` | none: the relay polls |
| Tested on | `postgres:17.7` | `mysql:8.4.7` |

The startup fails, naming what is supported, on another database, on MariaDB, on an older server (`The kraken message store
needs PostgreSQL 13.0 or later; this server is PostgreSQL 12.22`), on MySQL without a STRICT `sql_mode` (the database would
truncate instead of refusing), and on a HikariCP pool of one connection (a write that commits on its own inside the service's
transaction takes a second one; with one it would wait for itself).

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `kraken.messaging.store.type` | the only one | `jdbc` chooses this store when another store add-on is on the classpath too (the MongoDB one, say); naming another store (`mongo`, `memory`) makes this one step back. Two store add-ons without the setting fail the startup, naming both. A `MessageStore` bean the application defines itself, of any type (a `JdbcMessageStore` of its own included), wins over every add-on. |
| `kraken.messaging.store.init-schema` | `true` | Apply the schema at startup (see [Schema](#schema)); `false` when the service runs the migration itself: then the startup checks the schema is there (version 1 or later). |
| `kraken.messaging.store.jdbc.dialect` | detected | `postgresql` or `mysql`; unset, the dialect is the DataSource's database product. Set, it is used whatever the product name (a compatible database), and the server version is checked only when the product is the dialect's own. Another value fails the startup naming the supported ones. |
| `kraken.messaging.store.jdbc.notify` | `true` | PostgreSQL: appends wake the outbox relays up at once (`LISTEN`/`NOTIFY`, [Relay wake-up](#relay-wake-up)); `false`: the relays poll. |
| `kraken.messaging.store.jdbc.purge-interval` | `1m` | How often the retention runs ([Retention](#retention)); `0` turns it off (the service purges itself). |
| `kraken.messaging.outbox.retention`, `idempotency.retention`, `external.calls.retention`, `sagas.ended-retention`, `sagas.dispatch-retention`, `sagas.failed-retention`, `deadlines.retention`, `events.dead-letter.retention` | the core's | What the purge keeps, as the MongoDB add-on's TTL indexes. |

A third-party store add-on follows the same convention: declare its `MessageStore` `@Bean` on its `@AutoConfiguration` class (or a
class nested in it), annotated `@ConditionalOnNoApplicationMessageStore`. A store declared elsewhere (a class the auto-configuration
`@Import`s, say) counts as the application's own: the other add-ons step back for it, and the two-add-ons check does not see it.

## Transactions

The store **joins the service's transaction**. Its writes that belong with the service's (an outbox entry, a processed marker,
a dead letter's deletion in a retry, the external calls, the saga state) run on the connection Spring binds to the thread for
the `DataSource`. Both of Spring's JDBC-capable managers bind it:

- `DataSourceTransactionManager`: plain JDBC, `JdbcTemplate` / `JdbcClient`, jOOQ, Exposed;
- `JpaTransactionManager`: JPA (Hibernate) over the same `DataSource`.

So `@Transactional` business writes, the outbox entry and the marker commit together or roll back together, whichever manager
the service runs. The store's transaction manager (`MessageStore.transactions`, which the library's own transactions use) is the
application's: Boot's auto-configured one (the JPA one in a JPA application), or the service's own bean. Only an application
without any transaction manager gets one from this add-on, a `DataSourceTransactionManager`, which then backs its
`@Transactional` methods too (the MongoDB add-on decides the same way).

- **Without a transaction**, each store call runs in a short transaction of its own, never on a bare connection: a pool
  configured `auto-commit=false` (common with JPA) would otherwise roll the write back when the connection returns.
- A transaction of **another** manager active on the thread (a second DataSource's, a MongoDB one, JTA) does not hold this
  DataSource's connection: a joined write then fails with `IllegalStateException` ("A transaction is active, but not on the
  message store's DataSource") instead of committing beside it.
- A write made once the service's transaction ended (its after-completion phase, where a
  `@TransactionalEventListener(phase = AFTER_COMMIT)` runs) runs in a transaction of its own and commits, whatever the pool.
- A write from a raw `TransactionSynchronization.afterCommit()` callback runs on the connection of the transaction that committed
  (Spring gives no sign that it ended): with a pool in auto-commit (the default) it commits on its own; with a pool configured
  `auto-commit=false` it is rolled back when the connection returns. Run such a write in a new transaction
  (`Propagation.REQUIRES_NEW`), or from a `@TransactionalEventListener(AFTER_COMMIT)`.
- What must commit on its own whatever the caller has (dead letters but their deletion in a retry, processor control, the
  relays' and schedulers' claims, the purge) runs in a transaction of its own, at READ COMMITTED, on a second connection from the
  same pool.

**Conflicts** surface as the exceptions the saga runtime retries: a duplicate key as `DuplicateKeyException`, a lock that is not
available, a deadlock or a serialization failure as a `TransientDataAccessException` (`CannotAcquireLockException`,
`PessimisticLockingFailureException`). Two transactions writing one saga never wait long for each other: an update locks the
saga's row with `FOR UPDATE NOWAIT`; the start of a saga with a live start key takes, on PostgreSQL, the transaction-scoped
advisory lock `pg_try_advisory_xact_lock(1263684430, hashtext(start_key_hash))` (another transaction's uncommitted insert is
invisible there), and on MySQL inserts at once and waits at most 1 s (`innodb_lock_wait_timeout` for that statement) for another
transaction's uncommitted start of the same key. The loser fails with `CannotAcquireLockException` and the runtime retries it. On
MySQL that timeout rolls back the statement only (`innodb_rollback_on_timeout` OFF, the default); with it ON MySQL rolls the whole
transaction back, which the runtime retries the same way.
No locking read ever looks for a row that is not there, so starts of different keys never deadlock on MySQL's gap locks.
Error messages name a saga's type and a key's hash, never a start key or an association (they may be personal data).

## Competing relays and schedulers

**A database (or schema) holds the stores of one logical service**: its instances share it, and the claims below spread the work
over them. Two different services use separate databases or schemas: the processed-event markers, the processor control, the
dead letters' key blocking and the deadline and failure-notice claims carry no service name, so two services in one schema would
see each other's rows.

A claim (the outbox relay's, the deadline scheduler's, the saga relay's failure notices) runs in a transaction of its own at
READ COMMITTED: `SELECT ... ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED` picks the rows that are free, already the claimer's or
whose lease passed, then an `UPDATE` makes them the claimer's until its lease ends (`owner`, `lease_until`; `claimed_by`,
`claimed_until`; `notice_next_at`). A concurrent claim skips the rows this one locked and, once it committed, no longer matches
them; a crashed claimer's rows are claimed again after its lease. READ COMMITTED matters on MySQL: at REPEATABLE READ a locking
read keeps every row it scanned locked, matching or not, so one relay's open claim would hide the entries another relay holds
from that relay's own claim, and it would send a later entry of a key before its failed earlier one. The outbox is claimed in
`(created_at, seq, id)` order, and an entry the claimer still holds comes back in its place, so an entry whose send failed is
retried before the later ones of its key (order per key, for what one instance publishes). The saga dispatch lease
(`kraken_saga_dispatch_leases`) is one row per saga: PostgreSQL takes it by an upsert conditional on the lease having passed;
MySQL takes over a passed lease by a conditional `UPDATE`, else inserts it, and a claim that loses a race is told no.

Times are the application's clock, never the database's `now()`, at millisecond precision.

## Relay wake-up

**PostgreSQL:** every append runs `SELECT pg_notify('kraken_outbox', '')` in its transaction, so the notification is delivered when
it commits and never after a rollback (PostgreSQL folds the notifications of one transaction into one). While an outbox relay
runs it listens: `LISTEN kraken_outbox` on a connection of its own, and every notification makes it claim at once instead of at
its next poll (`kraken.messaging.outbox.poll-interval`).

- The listening connection is opened outside the pool, built like the pool's: from a HikariCP DataSource its `jdbc-url`,
  `username`, `password` and `data-source-properties` (so SSL settings carry over, in the URL or as properties); from another
  DataSource Boot's `JdbcConnectionDetails` (URL, user, password; what the URL carries applies). Without either it borrows a
  pooled connection for 30 s at a time and gives it back, so a pooled connection is never held for good.
- A connection quiet for 30 s is checked with `SELECT 1` under a 10 s network timeout, so a half-open connection is noticed. A
  lost connection is logged once and listened again with a backoff (200 ms to 30 s); meanwhile the relay polls.
- Notifying transactions serialise briefly at their commit on PostgreSQL's notification queue: under a very high rate of
  appending transactions, turn the wake-up off (`kraken.messaging.store.jdbc.notify=false`) and let the relays poll.
- **PgBouncer in transaction mode** cannot carry `LISTEN` (it needs a session): turn the wake-up off there. Schema init takes a
  session advisory lock too (waiting at most 60 s): apply the schema through a direct connection, or yourself with
  `init-schema=false`.

**MySQL** has no notification: the relay polls, and `OutboxStore.onAppend` does nothing.

## Schema

The schema files are on the classpath: `db/kraken/postgresql/V1__kraken.sql` and `db/kraken/mysql/V1__kraken.sql`.

**The version rule** (both libraries): schema versions are additive. `V1__kraken.sql` is frozen once released (its bytes never
change); a later change is `V2__kraken.sql`, `V3__...`, each applied over the one before. A library that knows version n works on
a database at version n or later and refuses one below n. `kraken_schema` lists the versions applied.

With `kraken.messaging.store.init-schema` on (the default), the store applies the versions it knows at startup **unless the
database has them** (then the database is only read). Instances starting together apply them once: the first takes a lock
(PostgreSQL `pg_try_advisory_lock`, MySQL `GET_LOCK('kraken_schema', 60)`, both waiting at most 60 s), the others wait and find
them applied. On PostgreSQL a file runs in one transaction; on MySQL each DDL statement commits on its own, and the files are
idempotent (`CREATE TABLE IF NOT EXISTS`, the version row inserted unless it exists), so a run interrupted halfway completes the
next time. Without the right to create tables the startup fails saying to apply the files or grant the right.

**Running the migration yourself with Flyway**: give kraken's files a Flyway of their own, with its own history table, next to
the service's Flyway (whose `V1` would otherwise clash with kraken's `V1`: "more than one migration with version 1"), and turn
init-schema off. Kraken's later versions (`V2__kraken.sql`, ...) land in that same separate history. Flyway is the service's
dependency, through Spring Boot's Flyway starter (which also provides the `FlywayMigrationInitializer` the bean below waits for);
this module does not depend on it:

```kotlin
implementation("org.springframework.boot:spring-boot-starter-flyway")
implementation("org.flywaydb:flyway-database-postgresql") // or org.flywaydb:flyway-mysql
```

Declare it as a bean of its own type, **never a `Flyway` bean**: a `Flyway` bean turns Spring Boot's Flyway auto-configuration
off, and the service's own migrations would stop running. Run it after the service's own migrations (they find the database
without kraken's tables), as this bean does by asking for Boot's `FlywayMigrationInitializer` first:

```kotlin
/** Kraken's schema files under a Flyway history of their own, after the application's own migrations. */
@Component
class KrakenSchemaMigration(
    private val dataSource: DataSource,
    private val applicationMigrations: ObjectProvider<FlywayMigrationInitializer>, // org.springframework.boot.flyway.autoconfigure
) : InitializingBean {
    override fun afterPropertiesSet() {
        applicationMigrations.ifAvailable { } // the application's own migrations run first
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/kraken/postgresql") // or classpath:db/kraken/mysql
            .table("kraken_schema_history")
            .baselineOnMigrate(true) // the database already holds the service's tables
            .baselineVersion("0")
            .load()
            .migrate()
    }
}
```

```yaml
kraken:
  messaging:
    store:
      init-schema: false   # the startup then checks the schema is there, once every bean exists (after this one)
```

With init-schema off the store's schema check runs once every bean exists, never while the store bean is created, so a bean that
migrates while it is created (this one, or any other) always comes first.

Liquibase is not covered: use Flyway as above, or keep init-schema on.

Identifiers and positions compare by their bytes: `COLLATE "C"` on PostgreSQL, `utf8mb4_0900_bin` (NO PAD: trailing spaces count)
on MySQL. No name needs quoting in either database. Times are `TIMESTAMPTZ(3)` on PostgreSQL and `DATETIME(3)` holding UTC on
MySQL.

### Bounds and hashes

Every bounded column has the same bound on both databases and in both libraries, counted in characters (Unicode code points, as
`VARCHAR(n)` counts them). The store checks a value against its bound **before writing** and refuses one over it with an
`IllegalArgumentException` naming the field and its length (not the value), which no retry fixes; the database never truncates
(MySQL's STRICT mode backs that up).

| Value | Bound | Why | Columns |
|---|---|---|---|
| processor name | 64 | the library validates 48 (`@ProcessingGroup`), the hub keeps 64 bytes | `processor` of `kraken_processed_events`, `kraken_dead_letters`, `kraken_processor_control` |
| source | 255 | a Kafka topic name is at most 249 | `source` of markers and dead letters |
| segment | 128 | a Kafka partition number | `segment` of markers and dead letters |
| position | 128 | a Kafka offset is at most 19 digits | `position` of markers and dead letters |
| type name | 255 | event and message types, saga types, deadline names | `kraken_outbox.type`, `kraken_sagas.type`, `kraken_saga_dispatch.type`, `kraken_deadlines.saga_type` and `name`, `kraken_saga_failures.saga_type` |
| library id | 255 | the ids the library builds (UUIDs; a dispatch entry's `<sagaId>:<seq>`) | `kraken_outbox.id`, `kraken_sagas.id`, `kraken_saga_associations.saga_id`, `kraken_saga_dispatch.id` and `saga_id`, `kraken_saga_dispatch_leases.saga_id`, `kraken_deadlines.id` and `saga_id` |
| dead-letter id | 578 | `<processor>:<source>:<segment>:<position>` at the bounds above (2 312 bytes of `utf8mb4`: under MySQL's 3 072-byte index keys) | `kraken_dead_letters.id` |
| free text | 16 KiB of UTF-8, **cut** (not refused) | error messages | `kraken_outbox.last_error`, `kraken_dead_letters.error_type` and `error_message`, `kraken_saga_dispatch.failure_message`, `kraken_saga_failures.error` |
| everything else | none | content | `TEXT` on PostgreSQL, `MEDIUMTEXT` (16 MiB) on MySQL, JSON columns |

Values with no bound that are unique or looked up are found through the **lowercase hex SHA-256 of their UTF-8 bytes**, a
`CHAR(64)` column that carries the primary key, the unique index or the lookup; the value itself is kept whole in a text column
next to it:

| Table | Hash column | Of |
|---|---|---|
| `kraken_processed_events` | `id_hash` (PK) | the marker id (`processor:<name>:<eventId>`, `saga:<type>:<eventId>`, `ingress:<source>:<key>`) |
| `kraken_external_calls` | `id_hash` (PK) | the idempotency key |
| `kraken_saga_failures` | `id_hash` (PK) | `<sagaType>:<eventId>` |
| `kraken_sagas` | `start_key_hash`; `live_start_key_hash` (generated, unique) | the start key |
| `kraken_saga_associations` | `assoc_hash` (indexed) | the key's UTF-8 bytes, one zero byte, the value's UTF-8 bytes |
| `kraken_dead_letters` | `event_key_hash` (indexed with `processor`, `position_num`) | the record key |

### Payloads

The outbox payload and every header map are the database's JSON type (`jsonb`, `JSON`): an outbox payload is a JSON object whose
integers fit in a signed 64-bit integer, at most 100 deep (the store refuses anything else before it writes), and both types keep such integers
exact (`jsonb` as `numeric`, MySQL as `BIGINT`). **Decimals are not kept exact on MySQL**: its `JSON` stores a number with a
fraction as a double (`jsonb` keeps it exact) — the same contract as the MongoDB add-on, whose documents keep doubles. An
operator can query the outbox. What the library must read back exactly as written is text: the saga state (`BigDecimal` scales,
key order), the saga dispatch, deadline and failure payloads, the external calls' reply and error, and the dead letters' payload
(which may not be JSON at all: an INVALID letter's; a payload that is not UTF-8, or that holds U+0000, goes to `payload_binary`).

**Depth.** MySQL refuses a JSON document whose `JSON_DEPTH` exceeds 100 (a scalar or an empty array or object is 1, a non-empty
one is 1 + its deepest member: `{"a":1}` is 2; checked on MySQL 8.4.7: depth 100 is stored, 101 refused). The payload contract
is the same on every message store, so the core's check (`checkOutboxPayload`) refuses a payload deeper than 100 with an
`IllegalArgumentException` before any store sees it, on PostgreSQL and the other stores too.

### Tables

`kraken_schema`: the schema versions applied (`version` PK, `applied_at` UTC).

`kraken_outbox`: events waiting for the relay (ADR 0005).

| Column | PostgreSQL / MySQL | |
|---|---|---|
| `id` | `VARCHAR(255)` PK | the event id |
| `type` | `VARCHAR(255)` | the event type |
| `event_key` | `TEXT` / `MEDIUMTEXT` | its order key (null: none) |
| `headers`, `payload` | `JSONB` / `JSON` | header map; the event (a JSON object) |
| `created_at`, `seq` | timestamp, `BIGINT` | the order: `(created_at, seq)` |
| `attempts` | `INTEGER` | failed sends |
| `sent_at`, `failed_at` | timestamp | sent; given up on (both null: pending) |
| `last_error` | `TEXT` | the last send failure, cut to 16 KiB |
| `owner`, `lease_until` | text, timestamp | the relay holding it, until when (null: free) |

Indexes: PostgreSQL `kraken_outbox_pending (created_at, seq) WHERE sent_at IS NULL AND failed_at IS NULL` and
`kraken_outbox_sent (sent_at) WHERE sent_at IS NOT NULL`; MySQL `kraken_outbox_pending (sent_at, failed_at, created_at, seq)`.

`kraken_processed_events`: `id_hash` `CHAR(64)` PK (**one marker per id**: what makes a redelivered event a duplicate), `id` text,
`type` text, `processed_at` (the retention counts from it), and a stage-1 marker's `processor` `VARCHAR(64)`, `source`
`VARCHAR(255)`, `segment` `VARCHAR(128)`, `position` `VARCHAR(128)`, `position_num` `BIGINT` (the position as a number when it
is a canonical decimal one). Indexes `kraken_processed_events_range (processor, source, segment, position_num)` (a reset deletes a
range by value; partial `WHERE processor IS NOT NULL` on PostgreSQL), `kraken_processed_events_processed_at (processed_at)`.

`kraken_dead_letters` (ADR 0022): `id` `VARCHAR(578)` PK (parked once); `processor` `VARCHAR(64)`, `source` `VARCHAR(255)`,
`segment`, `position` `VARCHAR(128)`; `position_num` `BIGINT`; `event_key` text and `event_key_hash` `CHAR(64)`; `event_id`,
`event_type` text; `headers` JSON; `payload` text; `payload_binary` `BYTEA`/`LONGBLOB`; `reason` `VARCHAR(32)` (`HANDLER_FAILED`,
`SEQUENCED`, `INVALID`); `error_type`, `error_message` `TEXT` (cut to 16 KiB); `blocked_by` text; `attempts`; `parked_at`,
`last_attempt_at`, `expire_at`; `replay` `BOOLEAN`. Indexes `kraken_dead_letters_key (processor, event_key_hash, position_num)` (a
key's head: decimal positions by value first, then the others by their bytes, ties by id), `kraken_dead_letters_parked (processor,
parked_at)`, `kraken_dead_letters_expire (expire_at)`. A letter past its `expire_at` is gone at once for every read (it no longer
blocks its key), and the purge deletes it.

`kraken_processor_control` (ADR 0023): `processor` `VARCHAR(64)` PK; `paused`, `paused_at`, `paused_by`; the reset operation
holding the lock: `op_id`, `op_kind`, `op_lease_until`, `op_prepared_at` (all null: free); `replay` JSON (the open ranges,
`[{"source","segment","from","until"}]` strings, in order); `updated_at`.

`kraken_external_calls`: `id_hash` `CHAR(64)` PK (**one record per idempotency key**), `id` text (the key); `status`
(`IN_PROGRESS`, `SUCCEEDED`, `FAILED`, `UNKNOWN`); `reply`, `error` text (JSON as written); `attempts`; `updated_at` (retention);
`lease_until`, `owner`. Index `kraken_external_calls_updated (updated_at)`.

`kraken_sagas` (ADR 0021): `id` `VARCHAR(255)` PK; `type` `VARCHAR(255)`; `state` text (JSON as written); `start_key` text and
`start_key_hash` `CHAR(64)`; `ended` `BOOLEAN`, `ended_at`; `version` `BIGINT` (optimistic, checked by every update); `created_at`,
`updated_at`; `live_start_key_hash`, a stored generated column (`CASE WHEN ended THEN NULL ELSE start_key_hash END`) with the
unique index `kraken_sagas_live_start_key`: **one live saga per start key** (an ended saga frees it). Indexes
`kraken_sagas_type_ended (type, ended)`, `kraken_sagas_ended_at (ended_at)`.

`kraken_saga_associations`: `saga_id` (FK to `kraken_sagas`, `ON DELETE CASCADE`), `idx` (the association's place, from 0),
`assoc_key`, `assoc_value` text, `assoc_hash` `CHAR(64)`; PK `(saga_id, idx)`; index `kraken_saga_associations_hash (assoc_hash)`:
what routes an event to the live sagas of a type (the lookup compares the key and the value too).

`kraken_saga_dispatch`: `id` `VARCHAR(255)` PK; `saga_id` `VARCHAR(255)`, `seq` (order per saga); `kind` (`COMMAND`, `EVENT`);
`type` `VARCHAR(255)`; `payload` text; `headers` JSON; `routing_key`; `created_at`; `dispatched_at`; `attempts`, `next_attempt_at`;
`failed_at`, `failure_reason`, `failure_message` (cut to 16 KiB); `notice_pending` `BOOLEAN`, `notice_attempts`, `notice_next_at`.
Indexes `(saga_id, seq)`, the pending ones (PostgreSQL partial `(saga_id, seq) WHERE dispatched_at IS NULL AND failed_at IS NULL`;
MySQL `(dispatched_at, failed_at, saga_id, seq)`), the notices (`notice_next_at` where pending; MySQL `(notice_pending,
notice_next_at)`), and the retention's `dispatched_at` and `failed_at`.

`kraken_saga_dispatch_leases`: `saga_id` `VARCHAR(255)` PK (**one dispatch relay per saga**), `owner`, `lease_until`.

`kraken_deadlines`: `id` `VARCHAR(255)` PK; `saga_type`, `name` `VARCHAR(255)`; `saga_id` `VARCHAR(255)`; `payload`, `payload_type`
text; `due_at`; `correlation_id`; `status` (`SCHEDULED`, `FIRED`, `CANCELLED`); `claimed_by`, `claimed_until`; `finished_at`.
Indexes `kraken_deadlines_due (status, due_at)`, `kraken_deadlines_saga (saga_id, status)`, `kraken_deadlines_finished (finished_at)`.

`kraken_saga_failures`: `id_hash` `CHAR(64)` PK, `id` text (`<sagaType>:<eventId>`); `saga_type` `VARCHAR(255)`; `event_id`,
`event_type`; `payload` text; `headers` JSON; `error` `TEXT` (cut to 16 KiB); `attempts`; `first_failed_at`, `last_failed_at`.
Indexes `(saga_type, last_failed_at)`, `(last_failed_at)`.

## Retention

The MongoDB add-on expires documents through TTL indexes; SQL has none, so this add-on runs a purge every
`kraken.messaging.store.jdbc.purge-interval` (`JdbcMessageStore.purgeExpired()`, in batches of 1 000 rows, each its own
transaction): sent outbox entries, processed markers, external calls, ended sagas (their associations with them), dispatched and
failed saga-dispatch entries, fired or cancelled deadlines and saga failures older than their retention, expired dead letters and
dispatch leases that passed. Pending outbox entries, live sagas and scheduled deadlines are never purged. Every instance runs
it; concurrent purges delete disjoint rows or nothing.

## Limits

- The character U+0000 is refused in every value the store writes (text, JSON strings, header maps), with the same
  `IllegalArgumentException` on both databases: PostgreSQL cannot store it in text or `jsonb`.
- The listening connection of the PostgreSQL wake-up is one per store; without a HikariCP DataSource or `JdbcConnectionDetails`
  it is one of the pool's connections most of the time while a relay runs.

## Tests

`PostgresMessageStoreConformanceTest` and `MySqlMessageStoreConformanceTest` run the store conformance kit
(`MessageStoreConformanceTest`, every check) on `postgres:17.7` and `mysql:8.4.7`. On both databases too: the transaction-join
tests (`joins_datasource_transaction`, `joins_jpa_transaction`), the bounds and hashes, the lock behaviours (an open claim,
concurrent locks of a new processor, lease takeovers, concurrent saga starts), a pool in `auto-commit=false`, the schema (version
rule, concurrent init, missing rights), the Flyway setup above, and the purge; the wake-up (`notify_wakes_relay_before_poll`) on
PostgreSQL. `examples/orders-sql` is a service on PostgreSQL with this store and an end-to-end test through the hub.
