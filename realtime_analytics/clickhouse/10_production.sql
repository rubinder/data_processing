-- The shipped schema: everything the tuning stages established, applied to
-- the live table that Kafka/Flink writes into and the API reads from.
--
-- Differences from stage 4/5, and why:
--
-- 1. ReplacingMergeTree instead of MergeTree, with event_id last in the
--    sorting key. Both ingest paths (the Python consumer and Flink's JDBC
--    sink) are at-least-once, so a crash between insert and offset commit
--    replays rows. ReplacingMergeTree collapses rows that are identical on
--    the whole sorting key, so a replayed event eventually disappears.
--
--    Eventually is the operative word: dedup happens on merge, not on insert.
--    A query that must be exact before merges catch up needs FINAL (slow) or
--    an explicit GROUP BY. This is also why the aggregate views below are
--    exposed to the same caveat -- see the note above mv_conversation_daily.
--
-- 2. The minmax index on latency_ms from stage 4 is gone. It was measured,
--    it pruned nothing, and an index that never prunes is pure write-side
--    cost. Keeping it "just in case" is how ingest slowly gets more
--    expensive for no read benefit.
--
-- 3. TTL. Raw events expire after 90 days; the aggregates are kept for two
--    years. That asymmetry is the single strongest argument for materialized
--    views over projections here -- a projection cannot outlive its parent
--    part, so it cannot have a longer retention than the raw data.
--
-- Single-node by default. For the two-shard cluster in ../clickhouse_deployment,
-- see the ON CLUSTER notes in README.md: the local tables become
-- ReplicatedReplacingMergeTree, each view is attached to the *local* table on
-- every node, and reads go through Distributed tables.

CREATE TABLE IF NOT EXISTS conversation_events
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
    escalation_reason LowCardinality(String),

    INDEX idx_conversation conversation_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_user         user_id         TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (account_id, event_ts, event_id)
TTL toDateTime(event_ts) + INTERVAL 90 DAY;

-- Landing table for the Flink 1-minute windowed rollup. This is the "right
-- now" operational view; the daily/hourly views below are the durable ones.
-- Its own TTL is short because a minute-grain table is only interesting while
-- it is fresh.
CREATE TABLE IF NOT EXISTS conversation_minute_agg
(
    account_id     LowCardinality(String),
    window_start   DateTime,
    event_type     LowCardinality(String),
    events         UInt64,
    conversations  UInt64,
    escalations    UInt64,
    resolutions    UInt64,
    avg_latency_ms Float64,
    max_latency_ms UInt32
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (account_id, window_start, event_type)
TTL window_start + INTERVAL 14 DAY;

-- ---------------------------------------------------------------------------
-- Aggregate layer. Identical in shape to 05_materialized_views.sql, sourced
-- from conversation_events.
--
-- Caveat worth stating plainly: a materialized view fires once per inserted
-- block. If an at-least-once replay re-inserts a block, ReplacingMergeTree
-- will eventually dedup the raw table but the aggregate has already counted
-- the rows twice. Two mitigations are in play:
--   * ClickHouse deduplicates *identical* insert blocks by checksum
--     (insert_deduplicate, on by default for Replicated tables), which covers
--     the exact-replay case that at-least-once actually produces;
--   * the aggregates are rebuildable from the raw table per partition, which
--     is the backstop for any drift, and is what the benchmark's backfill
--     path exercises.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS conversation_daily
(
    account_id      LowCardinality(String),
    day             Date,
    events          SimpleAggregateFunction(sum, UInt64),
    conversations   SimpleAggregateFunction(sum, UInt64),
    resolutions     SimpleAggregateFunction(sum, UInt64),
    escalations     SimpleAggregateFunction(sum, UInt64),
    agent_responses SimpleAggregateFunction(sum, UInt64),
    latency_state   AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt32),
    sentiment_state AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (account_id, day)
TTL day + INTERVAL 730 DAY
SETTINGS index_granularity = 128;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_conversation_daily
TO conversation_daily
AS
SELECT
    account_id,
    toDate(event_ts)                                       AS day,
    count()                                                AS events,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'resolution')                     AS resolutions,
    countIf(event_type = 'escalation')                     AS escalations,
    countIf(event_type = 'agent_response')                 AS agent_responses,
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(
        latency_ms, event_type = 'agent_response')         AS latency_state,
    avgState(sentiment)                                    AS sentiment_state
FROM conversation_events
GROUP BY account_id, day;

CREATE TABLE IF NOT EXISTS agent_hourly
(
    account_id        LowCardinality(String),
    hour              DateTime,
    model             LowCardinality(String),
    channel           LowCardinality(String),
    responses         SimpleAggregateFunction(sum, UInt64),
    slow_responses    SimpleAggregateFunction(sum, UInt64),
    prompt_tokens     SimpleAggregateFunction(sum, UInt64),
    completion_tokens SimpleAggregateFunction(sum, UInt64),
    latency_state     AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (account_id, hour, model, channel)
TTL hour + INTERVAL 730 DAY
SETTINGS index_granularity = 512;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agent_hourly
TO agent_hourly
AS
SELECT
    account_id,
    toStartOfHour(event_ts)                        AS hour,
    model,
    channel,
    count()                                        AS responses,
    countIf(latency_ms > 5000)                     AS slow_responses,
    sum(prompt_tokens)                             AS prompt_tokens,
    sum(completion_tokens)                         AS completion_tokens,
    quantilesTDigestState(0.5, 0.95, 0.99)(latency_ms) AS latency_state
FROM conversation_events
WHERE event_type = 'agent_response'
GROUP BY account_id, hour, model, channel;

CREATE TABLE IF NOT EXISTS platform_daily
(
    day             Date,
    events          SimpleAggregateFunction(sum, UInt64),
    conversations   SimpleAggregateFunction(sum, UInt64),
    resolutions     SimpleAggregateFunction(sum, UInt64),
    escalations     SimpleAggregateFunction(sum, UInt64),
    latency_state   AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt32)
)
ENGINE = AggregatingMergeTree
ORDER BY day
TTL day + INTERVAL 730 DAY
SETTINGS index_granularity = 128;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_platform_daily
TO platform_daily
AS
SELECT
    toDate(event_ts)                                       AS day,
    count()                                                AS events,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'resolution')                     AS resolutions,
    countIf(event_type = 'escalation')                     AS escalations,
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(
        latency_ms, event_type = 'agent_response')         AS latency_state
FROM conversation_events
GROUP BY day;
