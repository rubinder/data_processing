-- Stage 2 -- sorting key / primary key design.
--
-- Identical columns to stage 1; the only change is ORDER BY. In ClickHouse
-- the sorting key *is* the physical row order, and the primary index is a
-- sparse mark every index_granularity rows. A WHERE clause on a sorting-key
-- prefix therefore turns into a range scan over marks, and everything else
-- is skipped without being read.
--
-- Why (account_id, event_ts):
--
--   * account_id leads because ~every customer-facing query is scoped to one
--     tenant. A leading tenant column also keeps a tenant's rows physically
--     contiguous, which makes per-tenant reads, deletes, and GDPR-style
--     erasure cheap.
--   * event_ts is second because within a tenant every dashboard asks for a
--     time window. Sorted-by-time inside a tenant means a 7-day window is one
--     contiguous mark range.
--   * event_type is deliberately NOT in the key. It looks tempting (five
--     values, very low cardinality) but placing it before event_ts would
--     shatter each tenant's time ordering into five interleaved runs, so a
--     time-window query without an event_type filter would have to scan five
--     disjoint ranges. Low cardinality alone does not earn a key position --
--     being in the WHERE clause of the dominant query does.
--   * The classic "order by ascending cardinality" rule is a tiebreaker, not
--     the primary rule. The primary rule is: match the filter prefix of the
--     query you must make fast.
--
-- PRIMARY KEY is left implicit (== ORDER BY). Splitting them is worthwhile
-- when you want a long sorting key for compression but a short primary index
-- in memory; at this width the index is already small.

CREATE TABLE IF NOT EXISTS events_v2_sorted
(
    event_id          UUID,
    conversation_id   UUID,
    account_id        LowCardinality(String),
    user_id           String CODEC(ZSTD(1)),
    agent_id          LowCardinality(String),
    event_type        LowCardinality(String),
    channel           LowCardinality(String),
    locale            LowCardinality(String),
    model             LowCardinality(String),
    intent            LowCardinality(String),
    event_ts          DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    ingest_ts         DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    latency_ms        UInt32 CODEC(T64, ZSTD(1)),
    prompt_tokens     UInt32 CODEC(T64, ZSTD(1)),
    completion_tokens UInt32 CODEC(T64, ZSTD(1)),
    sentiment         Float32 CODEC(ZSTD(1)),
    resolved          UInt8 CODEC(ZSTD(1)),
    escalation_reason LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (account_id, event_ts);
