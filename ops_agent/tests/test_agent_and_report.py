from datetime import date

import pyarrow as pa

from ops_agent import actions, config, graph, report, runner, tables
from ops_agent.sensors import Finding
from tests.conftest import AS_OF, next_day, raw_rows


def _finding(kind="schema_drift", column="campaign_id", change="added"):
    return Finding(kind, config.RAW_TABLE, f"column '{column}' {change}",
                   {"change": change, "column": column, "observed_type": "string"})


def test_incident_file_is_written_once_per_finding_and_is_stable():
    p1 = actions.write_incident(_finding(), "additive", "New column.", AS_OF)
    p2 = actions.write_incident(_finding(), "additive", "New column.", AS_OF)
    assert p1 == p2 and len(list(actions.INCIDENTS_DIR.glob("*.md"))) == 1
    text = p1.read_text()
    assert "additive" in text and "New column." in text and config.RAW_TABLE in text
    assert actions.incident_slug(_finding()) != actions.incident_slug(_finding(column="x"))


def test_github_issue_is_dry_run_by_default():
    out = actions.file_github_issue(_finding(), "breaking", "why", execute=False)
    assert out.startswith("DRY-RUN") and "gh issue create" in out


def test_clean_lakehouse_produces_no_findings_and_records_the_run(fake):
    """The baseline that makes every real finding meaningful."""
    state = graph.run(engine=fake, dry_run=True, as_of=AS_OF)
    assert state["findings"] == [] and state["actions_taken"] == []
    runs = fake.scan_arrow(tables.OPS_AGENT_RUNS.name).to_pylist()
    assert runs == [{"run_at": AS_OF, "findings_count": 0, "actioned_count": 0,
                     "dry_run": True}]
    assert not fake.table_exists(tables.OPS_FINDING_LOG.name)


def test_drift_produces_classified_findings_actions_and_a_finding_log(fake):
    # Rename event_type -> event_name (same field id), widen page_type, add a
    # column, then land a collapsed batch carrying an unregistered stage.
    raw = fake.tables[config.RAW_TABLE]
    renamed = raw.rename_columns(["user_id", "impression_id", "page_type", "event_ts",
                                  "event_name"])
    renamed = renamed.set_column(2, "page_type", renamed.column("page_type").cast(pa.int64()))
    renamed = renamed.append_column("campaign_id", pa.array([None] * renamed.num_rows,
                                                            pa.string()))
    fake.tables[config.RAW_TABLE] = renamed
    fake.set_schema_history(config.RAW_TABLE, [
        {"schema_id": 0, "columns": {"user_id": "string", "impression_id": "string",
                                     "page_type": "int", "event_ts": "timestamptz",
                                     "event_type": "string"},
         "field_ids": {"user_id": 1, "impression_id": 2, "page_type": 3, "event_ts": 4,
                       "event_type": 5}},
        {"schema_id": 1, "columns": {"user_id": "string", "impression_id": "string",
                                     "page_type": "long", "event_ts": "timestamptz",
                                     "event_name": "string", "campaign_id": "string"},
         "field_ids": {"user_id": 1, "impression_id": 2, "page_type": 3, "event_ts": 4,
                       "event_name": 5, "campaign_id": 6}},
    ])
    late = raw_rows(5, seed=99).to_pylist()
    for row in late:
        row["event_type"] = "g"
    batch = pa.Table.from_pylist(late, schema=tables.RAW_IMPRESSIONS.arrow_schema())
    batch = batch.rename_columns(renamed.column_names[:5]).set_column(
        2, "page_type", batch.column("page_type").cast(pa.int64())).append_column(
        "campaign_id", pa.array([None] * batch.num_rows, pa.string()))
    fake.append(config.RAW_TABLE, batch)

    state = graph.run(engine=fake, dry_run=True, as_of=AS_OF)
    by_kind = {}
    for item in state["classified"]:
        by_kind.setdefault(item["finding"].kind, []).append(item)
    schema = {i["finding"].evidence["change"]: i["severity"] for i in by_kind["schema_drift"]}
    assert schema == {"renamed": "renaming", "type_changed": "widening", "added": "additive"}
    assert [i["severity"] for i in by_kind["enum_drift"]] == ["enum_drift"]
    assert by_kind["enum_drift"][0]["finding"].evidence["new_values"] == ["g"]
    assert [i["severity"] for i in by_kind["volume_anomaly"]] == ["breaking"]
    assert state["actions_taken"] and all(
        "DRY-RUN" in a or a.startswith("incident:") for a in state["actions_taken"])
    assert len(list(actions.INCIDENTS_DIR.glob("*.md"))) == 2   # renaming + volume

    log = fake.scan_arrow(tables.OPS_FINDING_LOG.name).to_pylist()
    assert {r["kind"] for r in log} == {"schema_drift", "enum_drift", "volume_anomaly"}
    assert sum(r["actioned"] for r in log) == 2


def test_report_says_no_run_rather_than_clean_when_nothing_ran(fake):
    snapshot = report.load_snapshot(fake, AS_OF)
    text = report.build_report(snapshot)
    assert "NO RUN RECORDED" in text and "CLEAN" not in text.split("## Schema")[0]


def test_report_reads_persisted_state_and_diffs_day_over_day(fake):
    runner.persist(fake, runner.run_monitors(fake, AS_OF))
    graph.run(engine=fake, dry_run=True, as_of=AS_OF)
    day1 = report.build_report(report.load_snapshot(fake, AS_OF))
    assert "Verdict — CLEAN" in day1 and "No earlier agent run" in day1
    assert "`aggregated_row_count`" in day1

    # Day two: one new column, and the aggregated table has not been rebuilt
    # for the new day, so the arrival SLA fires too. The diff must name both.
    fake.tables[config.RAW_TABLE] = fake.tables[config.RAW_TABLE].append_column(
        "campaign_id", pa.array([None] * fake.tables[config.RAW_TABLE].num_rows, pa.string()))
    day2_date = next_day(AS_OF)
    runner.persist(fake, runner.run_monitors(fake, day2_date))
    graph.run(engine=fake, dry_run=True, as_of=day2_date)
    snapshot = report.load_snapshot(fake, day2_date)
    text = report.build_report(snapshot)
    assert snapshot.previous_run == AS_OF
    assert "**2 new**, **0 cleared**, **0 still open**" in text
    assert "campaign_id" in text and "arrival_gap" in text and "new today" in text
    # Deterministic: rendering twice from the same snapshot is byte-identical.
    assert text == report.build_report(snapshot)


def test_report_delta_distinguishes_unchanged_from_never_measured():
    assert report._delta(None, 5.0) == "—"
    assert report._delta(5.0, 5.0) == "0"
    assert report._delta(5.0 + 1e-12, 5.0) == "0"
    assert report._delta(7.0, 5.0) == "+2"
    assert report._num(250000.0) == "250,000" and report._num(None) == "—"


def test_write_report_lands_in_the_given_directory(fake, tmp_path):
    graph.run(engine=fake, dry_run=True, as_of=AS_OF)
    path, snapshot = report.write_report(fake, AS_OF, tmp_path)
    assert path == tmp_path / f"daily-{AS_OF}.md" and snapshot.ran
    assert path.read_text().startswith(f"# Daily platform report — {AS_OF}")
