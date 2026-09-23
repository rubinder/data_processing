"""Rename pairing by field ID, widening as its own class, enum drift.

The rename tests are the point. Iceberg's whole claim is that a rename is
metadata-only because the field ID does not move; an agent that reports it
as an unrelated drop plus an unrelated add has not understood the table it
is watching.
"""
from datetime import date

import pyarrow as pa

from ops_agent import classifier, config, contracts, sensors, tables
from ops_agent.contracts import Contract, SchemaField
from tests.conftest import AS_OF, raw_rows
from tests.fake_engine import FakeEngine

RAW_COLUMNS = {"user_id": "string", "impression_id": "string", "page_type": "int",
               "event_ts": "timestamptz", "event_type": "string"}


def _contract(fields=None, enum_watch=(), expectations=()):
    fields = fields or list(RAW_COLUMNS.items())
    return Contract(config.RAW_TABLE, 1, "x",
                    tuple(SchemaField(n, t) for n, t in fields),
                    tuple(expectations), tuple(enum_watch))


def _state(**kw):
    base = {"table": config.RAW_TABLE, "columns": dict(RAW_COLUMNS), "row_count": 600,
            "rows_in_latest_snapshot": 200, "snapshot_count": 3, "newest_date": AS_OF}
    base.update(kw)
    return sensors.ObservedState(**base)


def _classified(findings, kind):
    return [(f, classifier.classify(f)[0]) for f in findings if f.kind == kind]


def test_no_findings_when_state_matches_contract():
    assert sensors.detect(_state(), _contract(), AS_OF, [200, 210, 190]) == []


def test_added_column_is_additive():
    state = _state(columns={**RAW_COLUMNS, "campaign_id": "string"})
    (finding, severity), = _classified(sensors.detect(state, _contract(), AS_OF, [200]),
                                       "schema_drift")
    assert finding.evidence["change"] == "added" and severity == "additive"


def test_dropped_column_is_breaking():
    state = _state(columns={k: v for k, v in RAW_COLUMNS.items() if k != "event_type"})
    (_, severity), = _classified(sensors.detect(state, _contract(), AS_OF, [200]),
                                 "schema_drift")
    assert severity == "breaking"


def test_widening_is_its_own_severity_not_breaking():
    state = _state(columns={**RAW_COLUMNS, "page_type": "long"})
    (finding, severity), = _classified(sensors.detect(state, _contract(), AS_OF, [200]),
                                       "schema_drift")
    assert finding.evidence["change"] == "type_changed" and severity == "widening"


def test_narrowing_is_breaking():
    contract = _contract([("page_type", "long")])
    state = _state(columns={"page_type": "int"})
    (_, severity), = _classified(sensors.detect(state, contract, AS_OF, [200]), "schema_drift")
    assert severity == "breaking"


def test_rename_is_one_finding_not_a_drop_plus_an_add():
    state = _state(columns={**{k: v for k, v in RAW_COLUMNS.items() if k != "event_type"},
                            "event_name": "string"},
                   renames={"event_type": ("event_name", 5)})
    drift = sensors.detect(state, _contract(), AS_OF, [200])
    assert len(drift) == 1
    finding = drift[0]
    assert finding.evidence == {
        "change": "renamed", "column": "event_type", "renamed_to": "event_name",
        "field_id": 5, "declared_type": "string", "observed_type": "string",
        "type_compatible": True}
    severity, reasoning = classifier.classify(finding)
    assert severity == "renaming" and "field id 5" in reasoning


def test_rename_that_also_changed_type_is_breaking():
    state = _state(columns={**{k: v for k, v in RAW_COLUMNS.items() if k != "page_type"},
                            "page_kind": "string"},
                   renames={"page_type": ("page_kind", 3)})
    (finding, severity), = _classified(sensors.detect(state, _contract(), AS_OF, [200]),
                                       "schema_drift")
    assert finding.evidence["type_compatible"] is False and severity == "breaking"


def test_rename_map_is_transitive_and_excludes_genuine_drops():
    history = [
        {"schema_id": 0, "columns": {"a": "string", "gone": "int"},
         "field_ids": {"a": 1, "gone": 2}},
        {"schema_id": 1, "columns": {"b": "string", "gone": "int"},
         "field_ids": {"b": 1, "gone": 2}},
        {"schema_id": 2, "columns": {"c": "string"}, "field_ids": {"c": 1}},
    ]
    assert sensors.build_rename_map(history) == {"a": ("c", 1), "b": ("c", 1)}


def test_volume_collapse_is_detected_and_modest_change_is_not():
    collapse = sensors.detect(_state(rows_in_latest_snapshot=5), _contract(), AS_OF,
                              [200, 210, 190])
    (_, severity), = _classified(collapse, "volume_anomaly")
    assert severity == "breaking"
    assert not _classified(sensors.detect(_state(rows_in_latest_snapshot=170), _contract(),
                                          AS_OF, [200, 210, 190]), "volume_anomaly")


def test_staleness_is_measured_against_as_of_not_wall_clock():
    contract = _contract(expectations=[{"type": "freshness", "column": "event_ts",
                                        "max_lag_days": 1}])
    assert not _classified(sensors.detect(_state(), contract, AS_OF, [200]), "staleness")
    stale = sensors.detect(_state(), contract, date(2026, 6, 9), [200])
    (_, severity), = _classified(stale, "staleness")
    assert severity == "breaking"


def test_unparseable_dates_are_their_own_breaking_finding():
    contract = _contract(expectations=[{"type": "freshness", "column": "event_ts",
                                        "max_lag_days": 1}])
    findings = sensors.detect(_state(unparseable_date_count=2), contract, AS_OF, [200])
    (finding, severity), = _classified(findings, "data_corruption")
    assert finding.evidence["unparseable_count"] == 2 and severity == "breaking"


def test_enum_drift_reports_new_values_and_never_blocks():
    contract = _contract(enum_watch=[{"column": "event_type",
                                      "known_values": list("abcdef")}])
    state = _state(enum_values={"event_type": list("abcdefg")})
    (finding, severity), = _classified(sensors.detect(state, contract, AS_OF, [200]),
                                       "enum_drift")
    assert finding.evidence["new_values"] == ["g"] and severity == "enum_drift"


def test_enum_watch_that_read_nothing_is_breaking_not_clean():
    contract = _contract(enum_watch=[{"column": "event_type", "known_values": ["a"]}])
    absent = sensors.detect(_state(), contract, AS_OF, [200])
    (f, severity), = _classified(absent, "enum_drift")
    assert f.evidence["error"] == "column_absent" and severity == "breaking"
    empty = sensors.detect(_state(enum_values={"event_type": []}), contract, AS_OF, [200])
    (f, severity), = _classified(empty, "enum_drift")
    assert f.evidence["error"] == "no_values" and severity == "breaking"


def test_enum_watch_on_a_high_cardinality_column_is_flagged_as_misconfigured():
    contract = _contract(enum_watch=[{"column": "user_id", "known_values": []}])
    state = _state(enum_values={"user_id": [f"u{i}" for i in range(201)]})
    (f, severity), = _classified(sensors.detect(state, contract, AS_OF, [200]), "enum_drift")
    assert f.evidence["error"] == "cardinality_exceeded" and severity == "benign"


def test_observe_reads_metadata_and_projects_only_the_columns_it_needs(fake):
    contract = contracts.contract_for("raw_impressions.yaml")
    state = sensors.observe(fake, tables.RAW_IMPRESSIONS, "event_ts", ("event_type",))
    assert state.columns == RAW_COLUMNS
    assert state.snapshot_count == 3
    assert state.rows_in_latest_snapshot == fake.snapshot_row_counts(config.RAW_TABLE)[-1]
    assert state.newest_date == AS_OF
    assert state.enum_values["event_type"] == list("abcde")[:len(state.enum_values["event_type"])]
    assert sensors.detect(state, contract, AS_OF, [200, 200]) == []


def test_observe_follows_a_rename_so_a_watch_does_not_go_blind():
    """After event_type -> event_name the watch declared on event_type must
    keep reading the same field ID rather than reporting column_absent."""
    engine = FakeEngine()
    renamed = raw_rows(30).rename_columns(
        ["user_id", "impression_id", "page_type", "event_ts", "event_name"])
    engine.put(config.RAW_TABLE, renamed)
    engine.set_schema_history(config.RAW_TABLE, [
        {"schema_id": 0, "columns": RAW_COLUMNS,
         "field_ids": {n: i + 1 for i, n in enumerate(RAW_COLUMNS)}},
        {"schema_id": 1,
         "columns": {**{k: v for k, v in RAW_COLUMNS.items() if k != "event_type"},
                     "event_name": "string"},
         "field_ids": {**{n: i + 1 for i, n in enumerate(RAW_COLUMNS) if n != "event_type"},
                       "event_name": 5}},
    ])
    state = sensors.observe(engine, tables.RAW_IMPRESSIONS, "event_ts", ("event_type",))
    assert "event_type" in state.enum_values and not state.enum_errors
    findings = sensors.detect(state, contracts.contract_for("raw_impressions.yaml"), AS_OF, [])
    assert [f.evidence.get("change") for f in findings] == ["renamed"]


def test_observe_survives_a_date_value_that_sorts_above_every_real_date():
    engine = FakeEngine()
    rows = raw_rows(30).to_pylist()
    rows[0]["event_ts"] = None
    table = pa.Table.from_pylist(rows, schema=tables.RAW_IMPRESSIONS.arrow_schema())
    # Force a string date column so an unparseable value can exist at all.
    strings = ["zzzz-not-a-date"] + [str(r["event_ts"]) for r in rows[1:]]
    table = table.set_column(3, "event_ts", pa.array(strings))
    engine.put(config.RAW_TABLE, table)
    state = sensors.observe(engine, tables.RAW_IMPRESSIONS, "event_ts")
    assert state.unparseable_date_count == 1
    assert state.newest_date == AS_OF


def test_llm_disabled_without_api_key():
    import importlib
    importlib.reload(classifier)
    assert classifier.USE_LLM is False
    severity, reasoning = classifier.classify(sensors.Finding("mystery", config.RAW_TABLE, "?"))
    assert severity == "benign" and "disabled" in reasoning
