"""Against real Iceberg tables: the field IDs, the snapshot summaries and the
whole drift story, end to end. Marked ``spark``; skipped without a JVM."""
from datetime import datetime, timedelta

import pytest
from iceberg_deployment import schema_evolution

from ops_agent import config, contracts, graph, lakehouse, report, runner, sensors, tables

pytestmark = pytest.mark.spark

START = datetime(2026, 6, 1)


def _seed_clean(engine, batches=(7, 11, 13)):
    for seed in batches:
        lakehouse.seed_raw(engine, count=100, start=START, seed_value=seed)
    as_of = lakehouse.max_event_date(engine)
    lakehouse.build_aggregated(engine, as_of=as_of)
    return as_of


def test_schema_history_carries_field_ids_and_a_rename_keeps_them(engine):
    _seed_clean(engine)
    raw = engine.qualified(config.RAW_TABLE)
    before = engine.schema_history(config.RAW_TABLE)
    assert before[-1]["columns"] == {"user_id": "string", "impression_id": "string",
                                     "page_type": "int", "event_ts": "timestamptz",
                                     "event_type": "string"}
    schema_evolution.rename_column(engine.spark, raw, "event_type", "event_name")
    schema_evolution.widen_column(engine.spark, raw, "page_type", "BIGINT")
    after = engine.schema_history(config.RAW_TABLE)
    assert len(after) == len(before) + 2
    assert after[-1]["field_ids"]["event_name"] == before[-1]["field_ids"]["event_type"]
    assert after[-1]["columns"]["page_type"] == "long"
    assert sensors.build_rename_map(after) == {
        "event_type": ("event_name", before[-1]["field_ids"]["event_type"])}


def test_snapshot_details_distinguish_appends_from_rebuilds(engine):
    _seed_clean(engine)
    raw_details = engine.snapshot_details(config.RAW_TABLE)
    assert [d["operation"] for d in raw_details] == ["append"] * 3
    assert not any(d["is_full_rebuild"] for d in raw_details)
    assert raw_details[-1]["total_records"] == sum(d["added_records"] for d in raw_details)
    agg_details = engine.snapshot_details(config.AGGREGATED_TABLE)
    assert agg_details[-1]["operation"] == "overwrite" and agg_details[-1]["is_full_rebuild"]


def test_scan_projection_sql_aliases_and_round_trip_writes(engine):
    _seed_clean(engine)
    projected = engine.scan_arrow(config.RAW_TABLE, columns=["event_ts", "event_type"])
    assert projected.column_names == ["event_ts", "event_type"]
    out = engine.sql("SELECT count(*) AS n FROM t", {"t": config.RAW_TABLE}).to_pylist()
    assert out[0]["n"] == projected.num_rows
    # ops tables: create, append typed Arrow rows, read them back through SQL
    engine.create_table(tables.OPS_AGENT_RUNS)
    import pyarrow as pa
    engine.append(tables.OPS_AGENT_RUNS.name, pa.Table.from_pylist(
        [{"run_at": START.date(), "findings_count": 1, "actioned_count": 0, "dry_run": True}],
        schema=tables.OPS_AGENT_RUNS.arrow_schema()))
    rows = engine.sql("SELECT * FROM t", {"t": tables.OPS_AGENT_RUNS.name}).to_pylist()
    assert rows == [{"run_at": START.date(), "findings_count": 1, "actioned_count": 0,
                     "dry_run": True}]


def test_aggregated_build_is_contract_gated(engine):
    _seed_clean(engine)
    lakehouse.append_rows(engine, [("u", "imp_bad", 4, START, "a")])   # page_type 4
    with pytest.raises(contracts.ContractViolation, match="accepted_values"):
        lakehouse.build_aggregated(engine, as_of=lakehouse.max_event_date(engine))
    # The previous good table is still in place.
    assert engine.snapshot_details(config.AGGREGATED_TABLE)[-1]["is_full_rebuild"]


def test_clean_then_drifted_lakehouse_end_to_end(engine, tmp_path):
    as_of = _seed_clean(engine)
    for _ in range(3):
        runner.persist(engine, runner.run_monitors(engine, as_of))
    clean = graph.run(engine=engine, dry_run=True, as_of=as_of)
    assert clean["findings"] == [], [f.detail for f in clean["findings"]]

    raw = engine.qualified(config.RAW_TABLE)
    schema_evolution.add_column(engine.spark, raw, "campaign_id", "STRING")
    schema_evolution.widen_column(engine.spark, raw, "page_type", "BIGINT")
    schema_evolution.rename_column(engine.spark, raw, "event_type", "event_name")
    day2 = as_of + timedelta(days=1)
    lakehouse.append_rows(engine, [
        (f"user_{i}", f"imp_late_{i}", 2, datetime.combine(day2, datetime.min.time()), "g")
        for i in range(5)])
    results = runner.run_monitors(engine, day2)
    runner.persist(engine, results)
    volume = next(r for r in results if r.monitor == "raw_impressions_row_count")
    assert volume.status == "breach" and volume.metric == 5

    state = graph.run(engine=engine, dry_run=True, as_of=day2)
    severities = {(i["finding"].kind, i["finding"].evidence.get("change")
                   or i["finding"].evidence.get("monitor") or ""): i["severity"]
                  for i in state["classified"]}
    assert severities[("schema_drift", "renamed")] == "renaming"
    assert severities[("schema_drift", "type_changed")] == "widening"
    assert severities[("schema_drift", "added")] == "additive"
    assert severities[("enum_drift", "")] == "enum_drift"
    assert severities[("volume_anomaly", "")] == "breaking"
    assert severities[("monitor_breach", "raw_impressions_row_count")] == "breaking"

    path, snapshot = report.write_report(engine, day2, tmp_path)
    assert snapshot.previous_run == as_of
    text = path.read_text()
    assert "ATTENTION REQUIRED" in text and "field id" in text and "`g`" in text
