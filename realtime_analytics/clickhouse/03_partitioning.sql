-- Stage 3 -- partitioning.
--
-- Same columns and sorting key as stage 2, plus PARTITION BY toYYYYMM().
--
-- Partitioning is not a second index. Its job is to let the query planner
-- discard whole directories before the primary index is consulted, and to
-- give operations an O(1) unit of work.
--
-- Why monthly, and not the "obvious" daily:
--
-- This started as weekly, on the reasoning that a 7-day dashboard window
-- should map onto one partition. Measuring it (benchmarks/bench_partitioning.py,
-- results in benchmarks/results/partitioning.md) showed that reasoning was
-- wrong, because it only counted the pruning benefit and ignored the cost.
--
-- Every surviving part is work the query must do: its index is consulted and
-- its granules are read separately. At 20M rows the *same* tenant query, over
-- the *same* 147k rows, cost:
--
--     no partitioning   1 part    7.2 ms
--     monthly           3 parts  10.6 ms
--     weekly           14 parts  19.6 ms
--     daily            91 parts  79.8 ms
--
-- That is roughly 0.8 ms of fixed cost per part. Finer partitioning did cut
-- rows read on the cross-tenant query (4.7M -> 1.6M from none to weekly), but
-- the per-part overhead ate the entire gain: wall time was flat at ~25 ms and
-- then 3.5x worse at daily.
--
-- So: partition as coarsely as retention allows, and let the sorting key do
-- the pruning. Monthly keeps the part count in single digits at 90-day
-- retention while still making expiry a DROP PARTITION.
--
-- What generalizes is the method, not the number. At billions of events per
-- day each daily partition is enormous and the per-part overhead is
-- negligible against the work inside it, so daily becomes correct. The rule
-- is to measure both sides -- pruning benefit *and* per-part cost -- rather
-- than reaching for daily by reflex.
--
-- PARTITION BY account_id was rejected outright. With thousands of tenants it
-- produces thousands of tiny partitions and destroys merge efficiency --
-- tenant isolation belongs in the sorting key, not the partition key.
--
-- The operational payoff is bigger than the query payoff. Retention becomes
-- ALTER TABLE ... DROP PARTITION (a metadata operation, instant, no merge
-- pressure) instead of a DELETE mutation that rewrites every affected part.
-- 10_production.sql expresses the same policy declaratively with TTL; it is
-- deliberately omitted here so the benchmark dataset is never expired
-- mid-run.

CREATE TABLE IF NOT EXISTS events_v3_partitioned
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
PARTITION BY toYYYYMM(event_ts)
ORDER BY (account_id, event_ts);
