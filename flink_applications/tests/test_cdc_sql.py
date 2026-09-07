"""Tests for the CDC SQL builders (no Flink runtime required)."""

import re

import pytest

from flink_applications.cdc_sql import (
    AGG_QUERY,
    INSERT_QUERY,
    SINK_TABLE,
    SOURCE_FORMATS,
    SOURCE_TABLE,
    insert_query,
    sink_ddl,
    source_ddl,
)

REQUIRED_SOURCE_COLUMNS = ("page_type", "impression_id", "__op", "__source_ts_ms")


def _with_options(ddl: str) -> dict:
    """Parse the ``'k' = 'v'`` pairs of the WITH clause into a dict."""
    with_clause = ddl.split("WITH", 1)[1]
    return dict(re.findall(r"'([^']+)'\s*=\s*'([^']*)'", with_clause))


def test_source_ddl_wires_topic_and_bootstrap():
    ddl = source_ddl("cdc.impressions.events", "kafka:9092", "grp")
    opts = _with_options(ddl)
    assert opts["topic"] == "cdc.impressions.events"
    assert opts["properties.bootstrap.servers"] == "kafka:9092"
    assert opts["properties.group.id"] == "grp"
    assert opts["connector"] == "kafka"
    assert opts["scan.startup.mode"] == "earliest-offset"


def test_source_ddl_defaults_to_json_format():
    opts = _with_options(source_ddl("t", "b", "g"))
    assert opts["format"] == "json"
    assert opts["json.ignore-parse-errors"] == "true"
    assert "avro-confluent.url" not in opts


def test_source_ddl_avro_confluent_uses_registry_and_drops_json_options():
    ddl = source_ddl(
        "t", "b", "g",
        fmt="avro-confluent",
        schema_registry_url="http://debezium-schema-registry:8081",
    )
    opts = _with_options(ddl)
    assert opts["format"] == "avro-confluent"
    assert opts["avro-confluent.url"] == "http://debezium-schema-registry:8081"
    assert not any(k.startswith("json.") for k in opts)


def test_source_ddl_rejects_unknown_format():
    with pytest.raises(ValueError, match="unsupported format"):
        source_ddl("t", "b", "g", fmt="protobuf")


@pytest.mark.parametrize("fmt", SOURCE_FORMATS)
def test_source_ddl_declares_columns_the_query_needs(fmt):
    ddl = source_ddl("t", "b", "g", fmt=fmt)
    assert f"CREATE TABLE {SOURCE_TABLE}" in ddl
    for col in REQUIRED_SOURCE_COLUMNS:
        assert re.search(rf"^\s*{col}\s+\w+", ddl, re.MULTILINE), col
    # Types match what Debezium emits for the Postgres columns in init_db.sql.
    assert re.search(r"^\s*page_type INT", ddl, re.MULTILINE)
    assert re.search(r"^\s*event_date INT", ddl, re.MULTILINE)
    assert re.search(r"^\s*__source_ts_ms BIGINT", ddl, re.MULTILINE)


def test_source_ddl_declares_event_time_watermark():
    ddl = source_ddl("t", "b", "g")
    assert "TO_TIMESTAMP_LTZ(__source_ts_ms, 3)" in ddl
    assert "WATERMARK FOR event_time" in ddl


def test_agg_query_excludes_deletes_and_windows():
    assert "__op <> 'd'" in AGG_QUERY
    assert f"TUMBLE(TABLE {SOURCE_TABLE}" in AGG_QUERY
    assert "GROUP BY page_type, window_start, window_end" in AGG_QUERY


def test_sink_ddl_is_upsert_kafka_with_json_key_and_value():
    ddl = sink_ddl("cdc.impressions.page_type_counts", "debezium-kafka:9092")
    opts = _with_options(ddl)
    assert f"CREATE TABLE {SINK_TABLE}" in ddl
    assert opts["connector"] == "upsert-kafka"
    assert opts["topic"] == "cdc.impressions.page_type_counts"
    assert opts["properties.bootstrap.servers"] == "debezium-kafka:9092"
    assert opts["key.format"] == "json"
    assert opts["value.format"] == "json"
    # upsert-kafka requires a primary key; ours is the aggregation key.
    assert (
        "PRIMARY KEY (page_type, window_start, window_end) NOT ENFORCED" in ddl
    )


def test_sink_columns_match_agg_query_output():
    ddl = sink_ddl("t", "b")
    column_section = ddl.split("PRIMARY KEY")[0]
    sink_columns = re.findall(
        r"^\s*(\w+)\s+(INT|BIGINT|TIMESTAMP\(3\))", column_section, re.MULTILINE
    )
    assert sink_columns == [
        ("page_type", "INT"),
        ("window_start", "TIMESTAMP(3)"),
        ("window_end", "TIMESTAMP(3)"),
        ("event_count", "BIGINT"),
        ("distinct_impressions", "BIGINT"),
    ]
    # Same names, same order, as the SELECT list in AGG_QUERY.
    select_list = AGG_QUERY.split("SELECT", 1)[1].split("FROM", 1)[0]
    projected = re.findall(r"(\w+)\s*,?\s*$", select_list, re.MULTILINE)
    assert projected == [name for name, _ in sink_columns]


def test_insert_query_targets_sink_with_agg_query():
    assert INSERT_QUERY.startswith(f"INSERT INTO {SINK_TABLE}")
    assert AGG_QUERY in INSERT_QUERY
    assert insert_query("other_sink").startswith("INSERT INTO other_sink")


def test_cdc_impressions_module_has_no_top_level_flink_dependency_in_sql():
    # cdc_sql must stay importable without pyflink installed.
    import flink_applications.cdc_sql as mod
    import inspect

    source = inspect.getsource(mod)
    assert not re.search(r"^\s*(import|from)\s+pyflink", source, re.MULTILINE)
