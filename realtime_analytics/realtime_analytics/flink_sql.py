"""Flink SQL DDL and statements, kept separate from the job wiring.

Splitting the SQL out means the interesting decisions -- watermark strategy,
connector options, sink batching -- are plain strings that unit tests can
assert on without starting a JobManager. The same split is used by
``flink_applications/cdc_sql.py`` elsewhere in this repo.
"""

#: How far out of order events may arrive before the watermark passes them by.
#: Tuned in the README from the observed producer/ingest lag distribution.
DEFAULT_MAX_OUT_OF_ORDERNESS_S = 30


def kafka_source_ddl(
    topic: str,
    bootstrap: str,
    group_id: str,
    max_out_of_orderness_s: int = DEFAULT_MAX_OUT_OF_ORDERNESS_S,
    startup_mode: str = "group-offsets",
) -> str:
    """Kafka source over the raw conversation event stream.

    The watermark is the load-bearing decision here.

    ``WATERMARK FOR event_ts AS event_ts - INTERVAL 'N' SECOND`` declares
    "once I have seen an event at time T, I assert nothing older than T-N is
    still coming". Windows fire on that assertion, so N trades latency against
    completeness directly:

    * N too small -> windows close before stragglers land, and those events
      are counted as late and dropped. The escalation rate on the dashboard
      reads low, and nobody notices because the number is plausible.
    * N too large -> every windowed result is delayed by N, and window state
      is held N longer, costing memory.

    N is therefore set from the measured lag distribution, not from taste:
    pick roughly the p99 of (ingest_ts - event_ts). The producer here injects
    lateness up to 600s deliberately so the choice can be validated instead of
    assumed -- see the watermark section of the README.

    ``scan.topic-partition-discovery.interval`` matters in production: without
    it, partitions added to the topic after job start are never consumed, and
    the job silently processes a shrinking share of traffic.

    ``properties.auto.offset.reset`` is not optional with
    ``scan.startup.mode = 'group-offsets'``. On the very first deploy the
    consumer group has no committed offsets, and without a reset policy the
    job dies on startup with::

        NoOffsetForPartitionException: Undefined offset with no reset policy
        for partitions: [conversation.events-5, ...]

    It fails only on a brand-new group, so it survives every restart during
    development and then breaks the first deploy into a fresh environment.
    ``earliest`` is the right default for analytics: on a new group, process
    the retained backlog rather than silently skipping it.
    """
    return f"""
CREATE TABLE conversation_events (
    event_id          STRING,
    conversation_id   STRING,
    account_id        STRING,
    user_id           STRING,
    agent_id          STRING,
    event_type        STRING,
    channel           STRING,
    locale            STRING,
    model             STRING,
    intent            STRING,
    event_ts          TIMESTAMP(3),
    ingest_ts         TIMESTAMP(3),
    latency_ms        INT,
    prompt_tokens     INT,
    completion_tokens INT,
    sentiment         FLOAT,
    resolved          TINYINT,
    escalation_reason STRING,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '{max_out_of_orderness_s}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'properties.group.id' = '{group_id}',
    'scan.startup.mode' = '{startup_mode}',
    'properties.auto.offset.reset' = 'earliest',
    'scan.topic-partition-discovery.interval' = '5min',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL',
    'json.ignore-parse-errors' = 'true'
)
"""


def minute_agg_sink_ddl(
    topic: str,
    bootstrap: str,
) -> str:
    """Kafka sink for the 1-minute rollup, consumed by ClickHouse.

    **Why Kafka and not a JDBC sink.** The obvious design is a JDBC sink
    straight into ClickHouse, and that is what this module tried first. It
    does not work: Flink's official ``flink-connector-jdbc`` has no ClickHouse
    dialect at *any* released version (checked 3.1.2, 3.2.0, 3.3.0, 3.4.0),
    so the job fails at submit time with::

        Could not find any jdbc dialect factory that can handle url
        'jdbc:clickhouse://...' that implements JdbcDialectFactory

    Supplying the ClickHouse JDBC *driver* does not help -- Flink needs a
    dialect class, not just a driver. The options are to write and build a
    custom ``JdbcDialectFactory`` in Java, or to hand the rows to ClickHouse
    over Kafka and let its native ``Kafka`` table engine ingest them.

    The second is both simpler and better, and it is the standard ClickHouse
    streaming pattern:

    * ClickHouse batches on its own terms (``kafka_max_block_size``), so the
      "one part per flush" hazard that makes JDBC sinks dangerous disappears;
    * offsets are committed by ClickHouse only after the block is written, so
      the ingest path no longer depends on Flink's JDBC sink being
      at-least-once with no transactional coordination;
    * back-pressure is expressed as consumer lag, which is already the metric
      being alerted on.

    The rollup is an append-only stream (each event-time window emits once
    when the watermark passes it), so a plain ``kafka`` sink is correct here;
    an updating aggregate would need ``upsert-kafka`` instead.
    """
    return f"""
CREATE TABLE minute_agg_sink (
    account_id     STRING,
    window_start   TIMESTAMP(3),
    event_type     STRING,
    events         BIGINT,
    conversations  BIGINT,
    escalations    BIGINT,
    resolutions    BIGINT,
    avg_latency_ms DOUBLE,
    max_latency_ms INT
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '{bootstrap}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL'
)
"""


def minute_agg_sql(window_minutes: int = 1) -> str:
    """Event-time tumbling window rollup, emitted per account and event type.

    Uses a windowing table-valued function (TUMBLE(...)) rather than the
    legacy group-window syntax: TVFs are the supported path in Flink 1.18 and
    they let the planner apply local-global aggregation, which is what keeps a
    skewed key space (one enormous tenant) from pinning a single subtask.
    """
    return f"""
INSERT INTO minute_agg_sink
SELECT
    account_id,
    window_start,
    event_type,
    COUNT(*)                                                     AS events,
    SUM(CASE WHEN event_type = 'conversation_started'
             THEN 1 ELSE 0 END)                                  AS conversations,
    SUM(CASE WHEN event_type = 'escalation' THEN 1 ELSE 0 END)   AS escalations,
    SUM(CASE WHEN event_type = 'resolution' THEN 1 ELSE 0 END)   AS resolutions,
    -- COALESCE, not a bare AVG: a window with no agent_response yields NULL,
    -- and ClickHouse's non-nullable Float64 column rejects a JSON null.
    COALESCE(AVG(CASE WHEN event_type = 'agent_response'
             THEN CAST(latency_ms AS DOUBLE) END), 0.0)          AS avg_latency_ms,
    MAX(latency_ms)                                              AS max_latency_ms
FROM TABLE(
    TUMBLE(TABLE conversation_events,
           DESCRIPTOR(event_ts),
           INTERVAL '{window_minutes}' MINUTE)
)
GROUP BY account_id, event_type, window_start, window_end
"""
