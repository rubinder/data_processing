-- Stage 4 -- data skipping indexes for high-cardinality filters.
--
-- The sorting key can only accelerate filters on its own prefix. The support
-- console also needs the opposite access pattern: "pull up this one
-- conversation", where conversation_id is a UUID with tens of millions of
-- distinct values. Putting it in the sorting key is not an option -- it would
-- destroy the tenant/time ordering that the dashboard depends on.
--
-- A data skipping index solves this without a second copy of the table. It
-- stores a small summary per N granules and lets the reader discard granule
-- ranges whose summary proves no match can exist.
--
--   * idx_conversation -- bloom_filter(0.01) on conversation_id.
--     A conversation's events are written together in time, so its rows land
--     in a handful of adjacent granules. The bloom filter rejects everything
--     else. 1% false positive rate is the right trade here: a false positive
--     costs one wasted granule read, while a smaller rate costs memory on
--     every part.
--     GRANULARITY 1 -> one bloom filter per 8192-row granule: the finest,
--     most selective (and largest) setting, which is what a needle lookup
--     wants.
--
--   * idx_user -- same reasoning for per-end-user history lookups.
--
--   * idx_latency -- minmax on latency_ms. This one is included precisely
--     because it does NOT work, and the benchmark measures that. See the
--     "negative result" section of the README: agent latency's slow tail is
--     spread uniformly across granules, so essentially every granule's max
--     exceeds any interesting threshold and nothing is skipped. Skipping
--     indexes pay off only when the indexed column correlates with physical
--     row order; minmax on an uncorrelated column is pure write-side cost.
--
-- Skipping indexes are not free: they are built on insert and on merge, and
-- they add bytes per part. Adding one is a write-amplification decision as
-- much as a read decision.

CREATE TABLE IF NOT EXISTS events_v4_indexed
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
    INDEX idx_user         user_id         TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_latency      latency_ms      TYPE minmax             GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (account_id, event_ts);
