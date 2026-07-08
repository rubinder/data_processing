"""Tests for the CDC SQL builders (no Flink runtime required)."""

from flink_applications.cdc_sql import AGG_QUERY, source_ddl


def test_source_ddl_wires_topic_and_bootstrap():
    ddl = source_ddl("cdc.impressions.events", "kafka:9092", "grp")
    assert "'topic' = 'cdc.impressions.events'" in ddl
    assert "'properties.bootstrap.servers' = 'kafka:9092'" in ddl
    assert "'properties.group.id' = 'grp'" in ddl
    # Kafka connector with JSON format.
    assert "'connector' = 'kafka'" in ddl
    assert "'format' = 'json'" in ddl


def test_source_ddl_declares_event_time_watermark():
    ddl = source_ddl("t", "b", "g")
    assert "TO_TIMESTAMP_LTZ(__source_ts_ms, 3)" in ddl
    assert "WATERMARK FOR event_time" in ddl


def test_agg_query_excludes_deletes_and_windows():
    assert "__op <> 'd'" in AGG_QUERY
    assert "TUMBLE(TABLE cdc_events" in AGG_QUERY
    assert "GROUP BY page_type" in AGG_QUERY
