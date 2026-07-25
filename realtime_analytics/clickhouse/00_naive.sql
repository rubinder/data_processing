-- Stage 0 -- the naive schema (the "before" picture).
--
-- This is what you get when a service serializes its event payload and the
-- table mirrors the wire format one-to-one:
--
--   * every column is String, including both timestamps and all numerics;
--   * the interesting measures (latency, tokens, sentiment) live inside a
--     JSON blob, so reading one of them means reading all of them;
--   * ORDER BY tuple() -- no primary index, so ClickHouse cannot skip a
--     single granule and every query is a full scan;
--   * no partitioning, so retention means DELETE mutations rather than an
--     instant DROP PARTITION;
--   * default LZ4 compression with no per-column codecs.
--
-- It is not a strawman: it reads and writes correctly, and at small volume it
-- is fast enough that nobody notices. It falls over exactly when the dataset
-- outgrows the page cache.

CREATE TABLE IF NOT EXISTS events_v0_naive
(
    event_id        String,
    conversation_id String,
    account_id      String,
    user_id         String,
    agent_id        String,
    event_type      String,
    channel         String,
    locale          String,
    model           String,
    intent          String,
    event_ts        String,   -- ISO-8601 text: must be parsed on every read
    ingest_ts       String,
    payload         String    -- {"latency_ms":..., "tokens":..., ...}
)
ENGINE = MergeTree
ORDER BY tuple();
