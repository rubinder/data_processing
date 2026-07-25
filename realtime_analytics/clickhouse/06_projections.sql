-- Stage 6 (alternative) -- projections.
--
-- A projection is a second physical copy of the data, maintained inside the
-- same table, with its own sort order or its own pre-aggregation. The query
-- planner picks it automatically: you keep querying the base table and it
-- transparently reads the cheaper copy.
--
-- This stage exists to compare the two ways of solving the same two problems:
--
--   needle lookup  : bloom filter (stage 4)   vs  proj_conversation
--   pre-aggregation: materialized view (st. 5) vs  proj_daily
--
-- Trade-offs, measured in the README:
--
--   Projection wins on ergonomics. There is one table name, no backfill
--   script, no risk of a dashboard querying the raw table by mistake, and
--   ALTER TABLE ... MATERIALIZE PROJECTION backfills existing parts for you.
--   Projections are also consistent with the base table by construction --
--   they are rewritten by the same merges.
--
--   Materialized view wins on control. The target table is a real table, so
--   it can have its own partitioning, TTL, and retention (keep 2 years of
--   daily aggregates while raw events expire at 90 days -- impossible with a
--   projection, which dies with its parent part). A view can also read from
--   several sources, apply filters that reduce the state dramatically, and be
--   rebuilt partition-by-partition without touching the raw table.
--
--   Storage: a _normal projection duplicates the columns it lists, so it is
--   the expensive option; an aggregate projection is usually tiny.
--
-- Rule of thumb used here: aggregate projections for "same table, different
-- rollup", materialized views when the rollup needs a different lifecycle
-- from the raw data. Raw events expire at 90 days here while the aggregates
-- are kept for two years, so the shipped design (10_production.sql) uses
-- materialized views.

CREATE TABLE IF NOT EXISTS events_v6_projected
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

    -- Re-sorted copy of just the columns the support console renders.
    PROJECTION proj_conversation
    (
        SELECT
            event_id, conversation_id, account_id, user_id, event_type,
            event_ts, latency_ms, model, channel, intent, escalation_reason
        ORDER BY conversation_id
    ),

    -- Pre-aggregated copy answering the tenant dashboard from the base table.
    PROJECTION proj_daily
    (
        SELECT
            account_id,
            toDate(event_ts) AS day,
            count(),
            countIf(event_type = 'conversation_started'),
            countIf(event_type = 'resolution'),
            countIf(event_type = 'escalation'),
            quantilesTDigestIf(0.5, 0.95, 0.99)(
                latency_ms, event_type = 'agent_response')
        GROUP BY account_id, day
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (account_id, event_ts);
