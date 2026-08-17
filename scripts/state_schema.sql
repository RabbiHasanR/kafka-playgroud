-- Durable consumer fold state (spec 003, D1 and D6).
--
-- One row per (consumer group, order). NOT per partition and NOT per event:
--
--   * The partition is absent from the key on purpose. 002 keyed folds by partition
--     because ownership was the lesson; storing that here would durably recreate the
--     failure 003 removes — a partition moving between members would strand its rows.
--     It is not stored as a plain column either, because it is derivable from order_id
--     by the producer's hash, and a stored copy is a fact that can disagree.
--
--   * group_id IS in the key, so the three services' memories stay independent exactly
--     as their offsets are (R3.2). One service replaying cannot disturb another.
--
--   * This is a fold, not an event log. order-lifecycle is the event log, and storing
--     events here would make Postgres a worse Kafka (D12).
--
-- Applied two ways from this one file (D11): mounted into the container's
-- /docker-entrypoint-initdb.d/, and by scripts/apply_state_schema.sh. The mount runs
-- ONLY when the data volume is empty, so the script is the path that works after the
-- first `docker compose up`.

CREATE TABLE IF NOT EXISTS order_fold (
    -- Identity: which service's memory, and which order.
    group_id       text        NOT NULL,
    order_id       text        NOT NULL,

    -- The fold itself. These advance only when a higher sequence arrives, which is
    -- what makes a redelivery a no-op (R3.11).
    last_sequence  integer     NOT NULL,
    -- Nullable and text rather than an enum: it mirrors OrderFold.state, which is
    -- `OrderState | None`. A database enum would be a second copy of events.py's
    -- lifecycle vocabulary, needing a migration every time that grows.
    state          text,
    last_event_id  text        NOT NULL,

    -- Deliveries handled, incremented on EVERY delivery including one whose fold write
    -- was a no-op (R3.13). handled_count > last_sequence is the dual-write problem's
    -- residue, as a number — the thing spec 008 exists to drive to zero.
    handled_count  integer     NOT NULL DEFAULT 0,

    updated_at     timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (group_id, order_id)
);

-- No secondary index. The read and the write are both primary-key operations, and the
-- one inspection query this feature needs — handled_count > last_sequence — scans a
-- table with tens of rows.
