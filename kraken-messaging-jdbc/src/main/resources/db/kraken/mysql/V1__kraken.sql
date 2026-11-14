-- kraken-messaging message store, schema version 1 (MySQL 8.0 or later, InnoDB).
-- Shared by the Kotlin add-on (kraken-messaging-jdbc) and the Rust one (kraken-messaging-sqlx): both read and write these
-- tables the same way. Idempotent: it can run again over itself, and Flyway or Liquibase can run it as a migration.
-- Times are UTC instants at millisecond precision in DATETIME(3), stamped by the application (never NOW()).
-- utf8mb4_bin: identifiers and positions are compared and ordered by their bytes, case and accents included.

CREATE TABLE IF NOT EXISTS kraken_schema (
    version    INT         NOT NULL PRIMARY KEY,
    applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- The outbox (ADR 0005): events appended in the service's transaction, relayed to the event store afterwards.
CREATE TABLE IF NOT EXISTS kraken_outbox (
    id          VARCHAR(512) NOT NULL PRIMARY KEY,
    type        TEXT         NOT NULL,
    event_key   TEXT         NULL,
    headers     JSON         NOT NULL,
    payload     JSON         NOT NULL,
    created_at  DATETIME(3)  NOT NULL,
    seq         BIGINT       NOT NULL,
    attempts    INT          NOT NULL DEFAULT 0,
    sent_at     DATETIME(3)  NULL,
    failed_at   DATETIME(3)  NULL,
    last_error  TEXT         NULL,
    owner       TEXT         NULL,
    lease_until DATETIME(3)  NULL,
    INDEX kraken_outbox_pending (sent_at, failed_at, created_at, seq)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Processed-event markers: one per id (processor:<name>:<eventId>, saga:<type>:<eventId>, ingress:<source>:<key>, ...).
-- A stage-1 marker also carries its processor, source, segment and position (position_num: the position as a number when
-- it is a canonical decimal one, so a reset deletes a range by value).
CREATE TABLE IF NOT EXISTS kraken_processed_events (
    id           VARCHAR(512) NOT NULL PRIMARY KEY,
    type         TEXT         NOT NULL,
    processed_at DATETIME(3)  NOT NULL,
    processor    VARCHAR(200) NULL,
    source       VARCHAR(200) NULL,
    segment      VARCHAR(200) NULL,
    position     VARCHAR(200) NULL,
    position_num BIGINT       NULL,
    INDEX kraken_processed_events_range (processor, source, segment, position_num),
    INDEX kraken_processed_events_processed_at (processed_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Dead letters (ADR 0022), sequenced per (processor, key). id = <processor>:<source>:<segment>:<position>.
CREATE TABLE IF NOT EXISTS kraken_dead_letters (
    id              VARCHAR(512) NOT NULL PRIMARY KEY,
    processor       VARCHAR(200) NOT NULL,
    source          VARCHAR(200) NOT NULL,
    segment         VARCHAR(200) NOT NULL,
    position        VARCHAR(200) NOT NULL,
    position_num    BIGINT       NULL,
    event_key       VARCHAR(512) NULL,
    event_id        TEXT         NULL,
    event_type      TEXT         NULL,
    headers         JSON         NOT NULL,
    payload         MEDIUMTEXT   NULL,
    payload_binary  LONGBLOB     NULL,
    reason          VARCHAR(32)  NOT NULL,
    error_type      TEXT         NOT NULL,
    error_message   TEXT         NOT NULL,
    blocked_by      TEXT         NULL,
    attempts        INT          NOT NULL,
    parked_at       DATETIME(3)  NOT NULL,
    last_attempt_at DATETIME(3)  NOT NULL,
    expire_at       DATETIME(3)  NOT NULL,
    replay          BOOLEAN      NOT NULL DEFAULT FALSE,
    INDEX kraken_dead_letters_key (processor, event_key, position_num),
    INDEX kraken_dead_letters_parked (processor, parked_at),
    INDEX kraken_dead_letters_expire (expire_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- One row per event processor (P6b, ADR 0023): its pause, the reset operation holding its lock, its open replay ranges
-- (a JSON array of {"source","segment","from","until"} strings, in order).
CREATE TABLE IF NOT EXISTS kraken_processor_control (
    processor      VARCHAR(200) NOT NULL PRIMARY KEY,
    paused         BOOLEAN      NOT NULL DEFAULT FALSE,
    paused_at      DATETIME(3)  NULL,
    paused_by      TEXT         NULL,
    op_id          TEXT         NULL,
    op_kind        TEXT         NULL,
    op_lease_until DATETIME(3)  NULL,
    op_prepared_at DATETIME(3)  NULL,
    replay         JSON         NOT NULL,
    updated_at     DATETIME(3)  NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Idempotent external calls and ingress (@IdempotentHandler). reply and error are JSON text, kept as written.
CREATE TABLE IF NOT EXISTS kraken_external_calls (
    id          VARCHAR(512) NOT NULL PRIMARY KEY,
    status      VARCHAR(32)  NOT NULL,
    reply       MEDIUMTEXT   NULL,
    error       MEDIUMTEXT   NULL,
    attempts    INT          NOT NULL,
    updated_at  DATETIME(3)  NOT NULL,
    lease_until DATETIME(3)  NULL,
    owner       TEXT         NULL,
    INDEX kraken_external_calls_updated (updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Saga instances (ADR 0021). state is the saga as JSON text, kept as written. live_start_key is the start key while the
-- saga is live: its unique index lets one live saga hold a start key.
CREATE TABLE IF NOT EXISTS kraken_sagas (
    id             VARCHAR(512) NOT NULL PRIMARY KEY,
    type           VARCHAR(200) NOT NULL,
    state          MEDIUMTEXT   NOT NULL,
    start_key      VARCHAR(512) NULL,
    ended          BOOLEAN      NOT NULL,
    ended_at       DATETIME(3)  NULL,
    version        BIGINT       NOT NULL,
    created_at     DATETIME(3)  NOT NULL,
    updated_at     DATETIME(3)  NOT NULL,
    live_start_key VARCHAR(512) GENERATED ALWAYS AS (CASE WHEN ended THEN NULL ELSE start_key END) STORED,
    UNIQUE INDEX kraken_sagas_live_start_key (live_start_key),
    INDEX kraken_sagas_type_ended (type, ended),
    INDEX kraken_sagas_ended_at (ended_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- A saga's associations k = v, in order (idx from 0): what routes events to it.
CREATE TABLE IF NOT EXISTS kraken_saga_associations (
    saga_id     VARCHAR(512) NOT NULL,
    idx         INT          NOT NULL,
    assoc_key   VARCHAR(200) NOT NULL,
    assoc_value VARCHAR(512) NOT NULL,
    PRIMARY KEY (saga_id, idx),
    INDEX kraken_saga_associations_kv (assoc_key, assoc_value),
    CONSTRAINT kraken_saga_associations_saga FOREIGN KEY (saga_id) REFERENCES kraken_sagas (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- The saga-side outbox: commands and events a saga recorded, sent in seq order per saga.
CREATE TABLE IF NOT EXISTS kraken_saga_dispatch (
    id              VARCHAR(512) NOT NULL PRIMARY KEY,
    saga_id         VARCHAR(512) NOT NULL,
    seq             BIGINT       NOT NULL,
    kind            VARCHAR(16)  NOT NULL,
    type            TEXT         NOT NULL,
    payload         MEDIUMTEXT   NOT NULL,
    headers         JSON         NOT NULL,
    routing_key     TEXT         NULL,
    created_at      DATETIME(3)  NOT NULL,
    dispatched_at   DATETIME(3)  NULL,
    attempts        INT          NOT NULL,
    next_attempt_at DATETIME(3)  NULL,
    failed_at       DATETIME(3)  NULL,
    failure_reason  VARCHAR(32)  NULL,
    failure_message TEXT         NULL,
    notice_pending  BOOLEAN      NULL,
    notice_attempts INT          NULL,
    notice_next_at  DATETIME(3)  NULL,
    INDEX kraken_saga_dispatch_saga_seq (saga_id, seq),
    INDEX kraken_saga_dispatch_pending (dispatched_at, failed_at, saga_id, seq),
    INDEX kraken_saga_dispatch_notice (notice_pending, notice_next_at),
    INDEX kraken_saga_dispatch_failed (failed_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- One dispatch relay instance per saga at a time.
CREATE TABLE IF NOT EXISTS kraken_saga_dispatch_leases (
    saga_id     VARCHAR(512) NOT NULL PRIMARY KEY,
    owner       TEXT         NOT NULL,
    lease_until DATETIME(3)  NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Saga deadlines. payload is JSON text (any JSON value), kept as written.
CREATE TABLE IF NOT EXISTS kraken_deadlines (
    id             VARCHAR(512) NOT NULL PRIMARY KEY,
    saga_type      TEXT         NOT NULL,
    saga_id        VARCHAR(512) NOT NULL,
    name           VARCHAR(200) NOT NULL,
    payload        MEDIUMTEXT   NULL,
    payload_type   TEXT         NULL,
    due_at         DATETIME(3)  NOT NULL,
    correlation_id TEXT         NOT NULL,
    status         VARCHAR(16)  NOT NULL,
    claimed_by     TEXT         NULL,
    claimed_until  DATETIME(3)  NULL,
    finished_at    DATETIME(3)  NULL,
    INDEX kraken_deadlines_due (status, due_at),
    INDEX kraken_deadlines_saga (saga_id, status),
    INDEX kraken_deadlines_finished (finished_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

-- Saga events parked after their last attempt. id = <sagaType>:<eventId>.
CREATE TABLE IF NOT EXISTS kraken_saga_failures (
    id              VARCHAR(512) NOT NULL PRIMARY KEY,
    saga_type       VARCHAR(200) NOT NULL,
    event_id        TEXT         NOT NULL,
    event_type      TEXT         NOT NULL,
    payload         MEDIUMTEXT   NOT NULL,
    headers         JSON         NOT NULL,
    error           TEXT         NOT NULL,
    attempts        INT          NOT NULL,
    first_failed_at DATETIME(3)  NOT NULL,
    last_failed_at  DATETIME(3)  NOT NULL,
    INDEX kraken_saga_failures_type (saga_type, last_failed_at),
    INDEX kraken_saga_failures_last (last_failed_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;

INSERT INTO kraken_schema (version) VALUES (1) ON DUPLICATE KEY UPDATE version = version;
