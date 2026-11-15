-- Raft state: one row, saved before the node replies to any other node
CREATE TABLE IF NOT EXISTS raft_metadata (
    id            INT PRIMARY KEY CHECK (id = 1),
    current_term  BIGINT NOT NULL DEFAULT 0,
    voted_for     TEXT,
    commit_index  BIGINT NOT NULL DEFAULT 0,
    last_applied  BIGINT NOT NULL DEFAULT 0
);
INSERT INTO raft_metadata (id) VALUES (1) ON CONFLICT DO NOTHING;

-- The replicated log, hash-chained so any change to a past entry is detectable
CREATE TABLE IF NOT EXISTS raft_log (
    log_index   BIGINT PRIMARY KEY,
    term        BIGINT NOT NULL,
    command     JSONB  NOT NULL,
    prev_hash   TEXT   NOT NULL,
    entry_hash  TEXT   NOT NULL
);

-- State derived from committed log entries (rebuilt from the log if they disagree)
CREATE TABLE IF NOT EXISTS farms (
    farm_id  TEXT PRIMARY KEY,
    name     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_limits (
    day       DATE PRIMARY KEY,
    limit_l   BIGINT NOT NULL CHECK (limit_l >= 0)
);

CREATE TABLE IF NOT EXISTS allocations (
    day        DATE NOT NULL,
    farm_id    TEXT NOT NULL REFERENCES farms(farm_id),
    amount_l   BIGINT NOT NULL CHECK (amount_l >= 0),
    PRIMARY KEY (day, farm_id)
);

CREATE TABLE IF NOT EXISTS consumption_records (
    event_id      UUID PRIMARY KEY,
    farm_id       TEXT NOT NULL REFERENCES farms(farm_id),
    seq           BIGINT NOT NULL,
    day           DATE NOT NULL,
    amount_l      BIGINT NOT NULL CHECK (amount_l > 0),
    log_index     BIGINT NOT NULL,
    UNIQUE (farm_id, seq)
);

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id   UUID PRIMARY KEY,
    day           DATE NOT NULL,
    sender        TEXT NOT NULL REFERENCES farms(farm_id),
    recipient     TEXT NOT NULL REFERENCES farms(farm_id),
    amount_l      BIGINT NOT NULL CHECK (amount_l > 0),
    status        TEXT NOT NULL,
    log_index     BIGINT
);

CREATE TABLE IF NOT EXISTS farm_balances (
    day        DATE NOT NULL,
    farm_id    TEXT NOT NULL REFERENCES farms(farm_id),
    allocated  BIGINT NOT NULL DEFAULT 0,
    received   BIGINT NOT NULL DEFAULT 0,
    sent       BIGINT NOT NULL DEFAULT 0,
    consumed   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, farm_id),
    CHECK (consumed <= allocated + received - sent)
);
-- Every command carries a unique cmd_id; its result is stored so a retried command is applied only once
CREATE TABLE IF NOT EXISTS applied_commands (
    cmd_id     TEXT PRIMARY KEY,
    log_index  BIGINT NOT NULL,
    result     JSONB  NOT NULL
);