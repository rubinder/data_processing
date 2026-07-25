-- ClickHouse-native streaming ingest, using the Kafka table engine.
--
-- A `Kafka` engine table is a consumer, not storage: selecting from it reads
-- messages once and they are gone. The durable pattern is always the triple
--
--     Kafka engine table  ->  materialized view  ->  MergeTree table
--
-- where the view is what actually moves rows across. ClickHouse commits Kafka
-- offsets only after the destination block is written, which makes the whole
-- path at-least-once with no extra coordination.
--
-- Why this exists at all: Flink's official JDBC connector has no ClickHouse
-- dialect at any released version, so a Flink -> JDBC -> ClickHouse sink
-- cannot be built without writing a custom JdbcDialectFactory in Java. Handing
-- rows over Kafka is both the supported path and the better one -- ClickHouse
-- batches on its own terms via kafka_max_block_size, which avoids the
-- one-part-per-flush problem that makes naive streaming sinks dangerous.
--
-- ---------------------------------------------------------------------------
-- IMPORTANT: this file provides an ALTERNATIVE raw-ingest path to the Python
-- consumer in realtime_analytics/consumer.py. Running both at once means two
-- consumer groups reading the same topic, so every event is inserted twice.
-- Pick one. The consumer is the simpler default; this is the option that needs
-- no process outside the database.
-- ---------------------------------------------------------------------------

-- Raw events straight off the topic the producer writes to.
CREATE TABLE IF NOT EXISTS kafka_conversation_events
(
    event_id          UUID,
    conversation_id   UUID,
    account_id        String,
    user_id           String,
    agent_id          String,
    event_type        String,
    channel           String,
    locale            String,
    model             String,
    intent            String,
    event_ts          DateTime64(3),
    ingest_ts         DateTime64(3),
    latency_ms        UInt32,
    prompt_tokens     UInt32,
    completion_tokens UInt32,
    sentiment         Float32,
    resolved          UInt8,
    escalation_reason String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'analytics-kafka:9092',
    kafka_topic_list = 'conversation.events',
    kafka_group_name = 'clickhouse-raw-ingest',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    -- Block size is the ingest batching control, and the equivalent of the
    -- Python consumer's --batch-size: it bounds how many rows become one part.
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 65536,
    -- The producer emits 'YYYY-MM-DD HH:MM:SS.mmm'; the default 'basic'
    -- datetime parser rejects the fractional seconds.
    date_time_input_format = 'best_effort',
    -- Tolerate a producer that adds a field before the schema is updated,
    -- rather than stalling the whole partition on it.
    input_format_skip_unknown_fields = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_conversation_events
TO conversation_events
AS
SELECT
    event_id, conversation_id, account_id, user_id, agent_id, event_type,
    channel, locale, model, intent, event_ts, ingest_ts, latency_ms,
    prompt_tokens, completion_tokens, sentiment, resolved, escalation_reason
FROM kafka_conversation_events;
