"""PyFlink streaming job that consumes Debezium CDC for impression events.

This closes the gap where the Debezium -> Kafka topics had no consumer. It
reads ``cdc.impressions.events`` (Debezium with the ExtractNewRecordState
"unwrap" transform, so records are flattened rows with ``__op`` /
``__source_ts_ms`` metadata) and computes a tumbling-window count of events
per page_type, written to an ``upsert-kafka`` topic.

Streaming concerns demonstrated (vs. the batch hello-world):

- **Exactly-once.** Checkpointing is enabled with EXACTLY_ONCE mode so state
  and Kafka offsets commit atomically; a restart resumes without double- or
  under-counting.
- **Event time + watermarks.** The window is driven by the CDC source
  timestamp with a bounded-out-of-orderness watermark, so late events land in
  the right window instead of being attributed to processing time.
- **CDC semantics.** Deletes (``__op = 'd'``) are filtered out of the counts;
  only inserts/updates are aggregated.
- **Schema Registry.** With ``CDC_FORMAT=avro-confluent`` (default) the
  source is Avro with Confluent Schema Registry ids; see ``cdc_sql.source_ddl``
  for how the DDL-derived reader schema survives source ALTER TABLEs.

Why an upsert-kafka sink rather than ``print``:

- A windowed aggregate is an updating result: with the default streaming
  behaviour Flink can emit a value for a window and later revise it (late
  data, retractions). The ``print`` sink shows those as +I/-U/+U rows in
  stdout of the TaskManager, which nothing can consume programmatically.
- ``upsert-kafka`` keys every row by the sink's PRIMARY KEY
  ``(page_type, window_start, window_end)``: an update becomes an upsert on
  that key and a retraction a tombstone. Kafka log compaction plus a
  downstream reader that keeps the latest value per key (Flink
  ``upsert-kafka`` source, ClickHouse ReplacingMergeTree, ksqlDB table, ...)
  materialises exactly one current row per (page_type, window).
- The output is a normal Kafka topic on the same broker as the CDC input,
  so the whole path is Postgres -> Debezium -> Kafka -> Flink -> Kafka with
  no side channel.

``CDC_SINK=print`` keeps the old behaviour for debugging.

Topic / table names match debezium_deployment/connectors/postgres-source.json
(topic.prefix = "cdc", schema "impressions", table "events").
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.table import StreamTableEnvironment

from flink_applications.cdc_sql import (
    AGG_QUERY,
    SOURCE_FORMATS,
    insert_query,
    sink_ddl,
    source_ddl,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "debezium-kafka:9092")
EVENTS_TOPIC = os.environ.get("CDC_EVENTS_TOPIC", "cdc.impressions.events")
COUNTS_TOPIC = os.environ.get(
    "CDC_COUNTS_TOPIC", "cdc.impressions.page_type_counts"
)
CONSUMER_GROUP = os.environ.get("CDC_CONSUMER_GROUP", "flink-cdc-impressions")
# 'avro-confluent' (Debezium with io.confluent.connect.avro.AvroConverter) or
# 'json' (Debezium with JsonConverter, schemas.enable=false).
CDC_FORMAT = os.environ.get("CDC_FORMAT", "avro-confluent")
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://debezium-schema-registry:8081"
)
# 'upsert-kafka' (default) or 'print' (debug).
CDC_SINK = os.environ.get("CDC_SINK", "upsert-kafka")
CHECKPOINT_INTERVAL_MS = int(
    os.environ.get("CHECKPOINT_INTERVAL_MS", "10000")
)

SINKS = ("upsert-kafka", "print")


def build_environments():
    """Create the stream env (with exactly-once checkpointing) + table env."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(
        CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE
    )
    t_env = StreamTableEnvironment.create(env)
    return env, t_env


def run():
    if CDC_FORMAT not in SOURCE_FORMATS:
        raise SystemExit(
            f"CDC_FORMAT={CDC_FORMAT!r} not in {SOURCE_FORMATS}"
        )
    if CDC_SINK not in SINKS:
        raise SystemExit(f"CDC_SINK={CDC_SINK!r} not in {SINKS}")

    env, t_env = build_environments()
    t_env.execute_sql(
        source_ddl(
            EVENTS_TOPIC,
            KAFKA_BOOTSTRAP,
            CONSUMER_GROUP,
            fmt=CDC_FORMAT,
            schema_registry_url=SCHEMA_REGISTRY_URL,
        )
    )

    if CDC_SINK == "print":
        # Debug path: the TableResult iterates the (unbounded) changelog and
        # prints +I/-U/+U rows to the client's stdout; .print() blocks for the
        # life of the job, which is the point when debugging.
        t_env.sql_query(AGG_QUERY).execute().print()
        return

    t_env.execute_sql(sink_ddl(COUNTS_TOPIC, KAFKA_BOOTSTRAP))
    # A streaming INSERT INTO is submitted asynchronously: execute_sql returns
    # a TableResult as soon as the job is accepted by the cluster and the job
    # keeps running there. We deliberately do NOT call .wait() -- on a
    # streaming job it would block this client process until the job is
    # cancelled, and `flink run` already gives us the job id / status.
    result = t_env.execute_sql(insert_query())
    job_client = result.get_job_client()
    if job_client is not None:
        print(f"Submitted streaming INSERT, job id {job_client.get_job_id()}")


if __name__ == "__main__":
    run()
