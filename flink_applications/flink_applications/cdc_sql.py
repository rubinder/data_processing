"""Pure SQL builders for the CDC impressions job.

Kept free of any ``pyflink`` import so the DDL/query construction can be unit
tested without a Flink runtime installed.
"""

SOURCE_TABLE = "cdc_events"
SINK_TABLE = "page_type_counts"

# Value formats the source table supports. ``avro-confluent`` is the default in
# cdc_impressions.py; ``json`` matches connectors/postgres-source-json.json.
SOURCE_FORMATS = ("json", "avro-confluent")


def _format_options(fmt: str, schema_registry_url: str) -> str:
    if fmt == "json":
        return """
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        """.strip()
    if fmt == "avro-confluent":
        # Confluent wire format: each record carries the *writer* schema id,
        # which Flink resolves against the registry. The *reader* schema is
        # derived from the column list below, not fetched, so the DDL is the
        # contract this job depends on.
        return f"""
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{schema_registry_url}'
        """.strip()
    raise ValueError(
        f"unsupported format {fmt!r}; expected one of {SOURCE_FORMATS}"
    )


def source_ddl(
    topic: str,
    bootstrap: str,
    group: str,
    fmt: str = "json",
    schema_registry_url: str = "http://debezium-schema-registry:8081",
) -> str:
    """Kafka source DDL for the unwrapped Debezium events topic.

    ``event_time`` is derived from the Debezium source timestamp and carries a
    bounded-out-of-orderness watermark so windowing is event-time based.

    Column types follow what Debezium emits for the Postgres types in
    debezium_deployment/init_db.sql after ExtractNewRecordState: SMALLINT ->
    int, BIGSERIAL -> long, UUID/CHAR -> string, DATE -> int (days since
    epoch, ``io.debezium.time.Date``), ``__source_ts_ms`` -> long.

    Schema evolution with ``fmt='avro-confluent'``: Flink builds the Avro
    reader schema from this column list (every nullable column becomes a
    ``["null", T]`` union with ``default: null``) and resolves it against the
    writer schema referenced by each record. Consequences:

    - a column *added* at the source is not in the reader schema and is
      ignored;
    - a column *dropped or renamed* at the source is still in the reader
      schema but absent from the writer schema, so it resolves to its null
      default instead of failing;
    - the job therefore keeps working across ADD / RENAME / DROP as long as
      the columns AGG_QUERY actually needs (page_type, impression_id, __op,
      __source_ts_ms) are not the ones removed.

    ``event_minute`` and ``event_second`` are declared only to make the
    second point observable: debezium_deployment/schema_changes.sql renames
    the former and drops the latter, after which they read as NULL here.
    """
    return f"""
        CREATE TABLE {SOURCE_TABLE} (
            event_id BIGINT,
            user_id STRING,
            impression_id STRING,
            page_type INT,
            event_type STRING,
            event_date INT,
            event_hour INT,
            event_minute INT,
            event_second INT,
            __op STRING,
            __source_ts_ms BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(__source_ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = '{group}',
            'scan.startup.mode' = 'earliest-offset',
            {_format_options(fmt, schema_registry_url)}
        )
    """


def sink_ddl(topic: str, bootstrap: str) -> str:
    """upsert-kafka sink DDL for the windowed per-page_type counts.

    Columns mirror AGG_QUERY's output. The primary key is the aggregation key
    (page_type + window bounds): every update the aggregate emits for a window
    becomes an upsert on that key, and a retraction becomes a tombstone, so a
    downstream reader that materialises the latest value per key sees exactly
    one row per (page_type, window).
    """
    return f"""
        CREATE TABLE {SINK_TABLE} (
            page_type INT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            event_count BIGINT,
            distinct_impressions BIGINT,
            PRIMARY KEY (page_type, window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """


# Tumbling 1-minute count of non-delete events per page_type. Deletes
# (Debezium __op = 'd') are excluded from the counts.
AGG_QUERY = f"""
    SELECT
        page_type,
        window_start,
        window_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT impression_id) AS distinct_impressions
    FROM TABLE(
        TUMBLE(TABLE {SOURCE_TABLE}, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    WHERE __op <> 'd'
    GROUP BY page_type, window_start, window_end
"""


def insert_query(sink_table: str = SINK_TABLE) -> str:
    """``INSERT INTO <sink> <AGG_QUERY>`` for a streaming INSERT."""
    return f"INSERT INTO {sink_table}\n{AGG_QUERY}"


INSERT_QUERY = insert_query()
