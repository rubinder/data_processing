"""PyFlink streaming job that consumes Debezium CDC for impression events.

This closes the gap where the Debezium -> Kafka topics had no consumer. It
reads ``cdc.impressions.events`` (Debezium with the ExtractNewRecordState
"unwrap" transform, so records are flattened JSON with ``__op`` /
``__source_ts_ms`` metadata) and computes a tumbling-window count of events
per page_type.

Streaming concerns demonstrated (vs. the batch hello-world):

- **Exactly-once.** Checkpointing is enabled with EXACTLY_ONCE mode so state
  and Kafka offsets commit atomically; a restart resumes without double- or
  under-counting.
- **Event time + watermarks.** The window is driven by the CDC source
  timestamp with a bounded-out-of-orderness watermark, so late events land in
  the right window instead of being attributed to processing time.
- **CDC semantics.** Deletes (``__op = 'd'``) are filtered out of the counts;
  only inserts/updates are aggregated.

Topic / table names match debezium_deployment/connectors/postgres-source.json
(topic.prefix = "cdc", schema "impressions", table "events").
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.table import StreamTableEnvironment

from flink_applications.cdc_sql import AGG_QUERY, source_ddl

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
EVENTS_TOPIC = os.environ.get("CDC_EVENTS_TOPIC", "cdc.impressions.events")
CONSUMER_GROUP = os.environ.get("CDC_CONSUMER_GROUP", "flink-cdc-impressions")
CHECKPOINT_INTERVAL_MS = int(
    os.environ.get("CHECKPOINT_INTERVAL_MS", "10000")
)


def build_environments():
    """Create the stream env (with exactly-once checkpointing) + table env."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(
        CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE
    )
    t_env = StreamTableEnvironment.create(env)
    return env, t_env


def run():
    env, t_env = build_environments()
    t_env.execute_sql(
        source_ddl(EVENTS_TOPIC, KAFKA_BOOTSTRAP, CONSUMER_GROUP)
    )
    t_env.sql_query(AGG_QUERY).execute().print()


if __name__ == "__main__":
    run()
