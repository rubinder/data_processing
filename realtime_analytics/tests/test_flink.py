"""Flink SQL and tuning configuration.

The DDL and the tuning object are plain Python, so the decisions that would
otherwise only be visible on a running cluster are asserted here: the
watermark exists and is configurable, the sink batches, and the settings that
address backpressure and hot keys are actually switched on.
"""
import pytest

from realtime_analytics import flink_sql
from realtime_analytics.flink_job import FlinkTuning, table_config_options


def test_source_declares_event_time_watermark():
    ddl = flink_sql.kafka_source_ddl("t", "kafka:9092", "g")
    assert "WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND" in ddl
    assert "'connector' = 'kafka'" in ddl


def test_watermark_delay_is_configurable():
    ddl = flink_sql.kafka_source_ddl("t", "kafka:9092", "g", 120)
    assert "INTERVAL '120' SECOND" in ddl


def test_source_discovers_new_partitions():
    """Without discovery, partitions added later are silently never read."""
    ddl = flink_sql.kafka_source_ddl("t", "kafka:9092", "g")
    assert "'scan.topic-partition-discovery.interval'" in ddl


def test_source_sets_offset_reset_policy():
    """`group-offsets` without a reset policy dies on a brand-new group.

    It fails only on the first deploy into a fresh environment -- every
    restart during development finds committed offsets and works -- so the
    option is asserted rather than trusted.
    """
    ddl = flink_sql.kafka_source_ddl("t", "kafka:9092", "g")
    assert "'properties.auto.offset.reset' = 'earliest'" in ddl


def test_rollup_sink_is_kafka_not_jdbc():
    """The sink must not be JDBC: Flink has no ClickHouse JDBC dialect.

    flink-connector-jdbc ships dialects for MySQL/Postgres/Oracle/etc. and
    none for ClickHouse at any released version, so a jdbc-connector sink
    fails at submit time with "Could not find any jdbc dialect factory".
    Rows go over Kafka and ClickHouse ingests them with its Kafka engine.
    """
    ddl = flink_sql.minute_agg_sink_ddl("conversation.minute_agg", "kafka:9092")
    assert "'connector' = 'kafka'" in ddl
    assert "'jdbc'" not in ddl
    assert "'topic' = 'conversation.minute_agg'" in ddl
    # SQL timestamp format, or ClickHouse cannot parse window_start.
    assert "'json.timestamp-format.standard' = 'SQL'" in ddl


def test_window_aggregation_uses_tvf_and_event_time():
    sql = flink_sql.minute_agg_sql(5)
    assert "INSERT INTO minute_agg_sink" in sql
    assert "TUMBLE(TABLE conversation_events" in sql
    assert "DESCRIPTOR(event_ts)" in sql
    assert "INTERVAL '5' MINUTE" in sql
    assert "GROUP BY account_id, event_type, window_start, window_end" in sql


def test_rollup_never_emits_null_latency():
    """A window with no agent_response averages to NULL.

    ClickHouse's avg_latency_ms column is a non-nullable Float64, so a JSON
    null stalls ingestion of the whole block.
    """
    assert "COALESCE(AVG(" in flink_sql.minute_agg_sql()


def test_default_tuning_addresses_backpressure_and_skew():
    tuning = FlinkTuning()
    # Unaligned checkpoints: the remedy for barriers stuck behind buffered data.
    assert tuning.unaligned_checkpoints is True
    # Two-phase aggregation: the remedy for one enormous tenant.
    assert tuning.two_phase_aggregation is True
    # A pause between checkpoints, or a struggling job never drains its backlog.
    assert 0 < tuning.checkpoint_min_pause_ms < tuning.checkpoint_interval_ms
    # Sink parallelism below job parallelism keeps part creation in check.
    assert tuning.sink_parallelism <= tuning.parallelism


def test_source_idle_timeout_is_set():
    """An idle partition otherwise freezes the global watermark."""
    options = table_config_options(FlinkTuning())
    assert options["table.exec.source.idle-timeout"] == "60000ms"


def test_mini_batch_options_present_when_enabled():
    options = table_config_options(FlinkTuning())
    assert options["table.exec.mini-batch.enabled"] == "true"
    assert options["table.exec.mini-batch.allow-latency"] == "1000ms"
    assert options["table.exec.mini-batch.size"] == "20000"
    assert options["table.optimizer.agg-phase-strategy"] == "TWO_PHASE"


def test_mini_batch_options_absent_when_disabled():
    tuning = FlinkTuning(mini_batch_enabled=False)
    options = table_config_options(tuning)
    assert options["table.exec.mini-batch.enabled"] == "false"
    assert "table.exec.mini-batch.allow-latency" not in options


def test_tuning_reads_environment(monkeypatch):
    monkeypatch.setenv("FLINK_PARALLELISM", "16")
    monkeypatch.setenv("FLINK_STATE_BACKEND", "hashmap")
    monkeypatch.setenv("FLINK_UNALIGNED_CHECKPOINTS", "false")
    tuning = FlinkTuning()
    assert tuning.parallelism == 16
    assert tuning.state_backend == "hashmap"
    assert tuning.unaligned_checkpoints is False


@pytest.mark.parametrize("backend", ["rocksdb", "hashmap"])
def test_state_backend_choices_are_supported(backend, monkeypatch):
    monkeypatch.setenv("FLINK_STATE_BACKEND", backend)
    assert FlinkTuning().state_backend == backend
