-- Landing path for the Flink 1-minute rollup.
--
-- Required by the PyFlink job: Flink writes each closed event-time window to
-- the `conversation.minute_agg` topic, and this pair moves those rows into the
-- SummingMergeTree table created in 10_production.sql.
--
-- Unlike 20_kafka_ingest.sql, this is not an alternative to anything -- there
-- is no other producer of this topic, so applying it is safe alongside either
-- raw-ingest path.
--
-- The division of labour is the point:
--   Flink       does event-time windowing with watermarks, which ClickHouse
--               cannot express -- it has no notion of a late event.
--   ClickHouse  does storage, ingest batching, and every aggregate that can be
--               computed incrementally on insert.

CREATE TABLE IF NOT EXISTS kafka_minute_agg
(
    account_id     String,
    window_start   DateTime,
    event_type     String,
    events         UInt64,
    conversations  UInt64,
    escalations    UInt64,
    resolutions    UInt64,
    avg_latency_ms Float64,
    max_latency_ms UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'analytics-kafka:9092',
    kafka_topic_list = 'conversation.minute_agg',
    kafka_group_name = 'clickhouse-minute-agg-ingest',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 8192,
    date_time_input_format = 'best_effort',
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_minute_agg
TO conversation_minute_agg
AS
SELECT
    account_id,
    window_start,
    event_type,
    events,
    conversations,
    escalations,
    resolutions,
    avg_latency_ms,
    max_latency_ms
FROM kafka_minute_agg;
