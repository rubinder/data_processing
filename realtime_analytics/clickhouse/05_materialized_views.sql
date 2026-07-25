-- Stage 5 -- incremental materialized views (pre-aggregation).
--
-- Stages 1-4 make the scan cheaper. This stage removes the scan.
--
-- A ClickHouse materialized view is an INSERT trigger, not a cached query: as
-- each block lands in the source table the view's SELECT runs over *that
-- block only* and the result is written to a target table. Cost is paid once
-- at write time, in the streaming path, instead of on every dashboard load.
--
-- The target is AggregatingMergeTree so that partial states from different
-- insert blocks (and different shards) collapse on merge. Two column styles
-- are used:
--
--   * SimpleAggregateFunction(sum, UInt64) for counts. Sums are associative
--     and need no intermediate state, so the column stores a plain number and
--     merges add them. Cheaper to store and read than a full state.
--   * AggregateFunction(quantilesTDigest, ...) for latency. Quantiles are not
--     associative -- you cannot average two p95s -- so the t-digest sketch
--     itself is stored and merged with quantilesTDigestMerge at read time.
--     t-digest is an approximation with high accuracy in the tails, which is
--     exactly where p95/p99 live. Use quantilesExactState only if you need
--     exact answers and can afford states that grow with cardinality.
--
-- Two views, matching the two dashboards that are actually loaded:
--   mv_conversation_daily -- the tenant overview (volume, resolution rate,
--                            escalation rate, latency percentiles per day)
--   mv_agent_hourly       -- the operational view, sliced by model and
--                            channel, used for latency SLO tracking
--
-- Caveats that matter in production, and are covered in the README:
--   * A view fires only on rows inserted *after* it is created. Existing data
--     must be backfilled explicitly (INSERT INTO ... SELECT, ideally in
--     partition-sized chunks) -- benchmarks/bench_clickhouse.py does this.
--   * A view does not see mutations. ALTER TABLE ... DELETE on the source
--     leaves the aggregate untouched; corrections need a rebuild of the
--     affected partitions.
--   * On a sharded cluster the view must be attached to the *local* table on
--     every node, never to the Distributed table, or rows are aggregated
--     twice. Reads then go through a Distributed table over the target.

-- index_granularity is deliberately far below the 8192 default.
--
-- The default is tuned for tables with hundreds of millions of rows, where a
-- coarse sparse index keeps memory small. An aggregate table holds a few tens
-- of thousands of rows, so one default granule can be a large fraction of the
-- whole table -- and every one of those rows carries a fat t-digest state
-- column. Measured effect: reading a 7-row answer was pulling ~5,800 rows and
-- ~1 MB off disk, which made the "pre-aggregated" query slower than scanning
-- the raw events. Dropping to 128 makes granule reads precise.
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
FROM events_v4_indexed
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
FROM events_v4_indexed
WHERE event_type = 'agent_response'
GROUP BY account_id, hour, model, channel;

-- Third view: the platform-wide (cross-tenant) rollup.
--
-- It would be tempting to answer "all tenants, last 7 days" from
-- conversation_daily by dropping the account_id filter. That works, but it
-- has to read and merge one t-digest state per (account, day) -- 200 accounts
-- x 7 days = 1,400 sketches -- and sketch merging is expensive enough that
-- the measured result was *slower* than scanning 1.5M raw rows.
--
-- The fix is not a better index; it is a view whose key matches the query.
-- Aggregating to day alone makes the same answer 7 rows and 7 merges. The
-- general rule: a materialized view accelerates the grouping it was keyed
-- for, and a query that groups more coarsely than the view still pays for
-- the view's finer granularity.
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
FROM events_v4_indexed
GROUP BY day;
