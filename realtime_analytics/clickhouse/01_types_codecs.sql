-- Stage 1 -- physical types, LowCardinality, and per-column codecs.
--
-- Only the storage layout changes here. The sorting key is still tuple(), so
-- this stage isolates one variable: how much of the naive query's cost was
-- reading and parsing badly typed columns rather than scanning too many rows.
--
-- Decisions:
--   * DateTime64(3) instead of ISO text. Milliseconds matter for agent
--     latency, and a native timestamp removes parseDateTimeBestEffort from
--     the hot path -- that function is the single most expensive thing in the
--     naive query.
--   * Delta + ZSTD on timestamps. Event streams arrive in near-monotonic
--     time order, so consecutive deltas are tiny and compress hard.
--   * T64 on the numeric measures. T64 transposes the bit planes of a
--     bounded integer range; latency and token counts never approach
--     UInt32's range, so the high bytes are all zeros and vanish.
--   * LowCardinality(String) for the dimensions. Under a few thousand
--     distinct values, ClickHouse stores a dictionary index instead of the
--     string, which shrinks both storage and GROUP BY key comparisons.
--   * UUID instead of String for ids: 16 fixed bytes rather than 36 text
--     bytes plus a length prefix.
--   * The JSON blob is unpacked into real columns, so a query that needs
--     latency reads only the latency column.

CREATE TABLE IF NOT EXISTS events_v1_typed
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
ORDER BY tuple();
