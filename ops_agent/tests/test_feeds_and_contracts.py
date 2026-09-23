from datetime import date, datetime

import pyarrow as pa
import pytest

from ops_agent import config, contracts, feeds, tables
from tests.conftest import AS_OF, aggregate, raw_rows


def test_the_shipped_feed_loads_and_declares_the_expected_shape():
    (feed,) = feeds.load_feeds()
    assert feed.name == "impressions"
    assert [l.table for l in feed.watched_layers] == [config.RAW_TABLE]
    assert [l.table for l in feed.typed_layers] == [config.AGGREGATED_TABLE]
    assert feed.arrival and feed.arrival[0].calendar == "daily"


def _write(tmp_path, body: str):
    (tmp_path / "f.yaml").write_text(body)
    return tmp_path


@pytest.mark.parametrize("body, needle", [
    ("name: f\nlayers: {raw: {table: db.nope}}\n", "db.nope"),
    ("name: f\nlayers: {raw: {table: db.impressions, contract: missing.yaml}}\n", "missing.yaml"),
    ("name: f\nlayers: {raw: {table: db.impressions, typed: true}}\n", "typed: true"),
    ("name: f\nlayers: {raw: {table: db.impressions}}\nmonitors:\n"
     "  - {name: m, layer: silver, kind: row_count, query: x}\n", "layer: silver"),
    ("name: f\nlayers: {raw: {table: db.impressions}}\narrival:\n"
     "  - {layer: raw, column: event_ts, calendar: trading, max_lag_periods: 1}\n", "trading"),
    ("name: f\nlayers: {raw: {table: db.impressions}}\narrival:\n"
     "  - {layer: raw, column: event_ts, calendar: daily, max_lag_periods: 1,"
     " min_rows_per_period: 1}\n", "min_rows_per_period"),
    ("name: f\nfeed_type: blob\nlayers: {raw: {table: db.impressions}}\n", "blob"),
])
def test_config_errors_are_loud_and_name_the_problem(tmp_path, body, needle):
    with pytest.raises(feeds.FeedConfigError, match=needle):
        feeds.load_feeds(_write(tmp_path, body))


def test_duplicate_monitor_names_across_feeds_are_rejected(tmp_path):
    for name in ("a", "b"):
        (tmp_path / f"{name}.yaml").write_text(
            f"name: {name}\nlayers: {{raw: {{table: db.impressions}}}}\nmonitors:\n"
            "  - {name: shared, layer: raw, kind: row_count, query: 'SELECT 1 AS metric'}\n")
    with pytest.raises(feeds.FeedConfigError, match="duplicate monitor name 'shared'"):
        feeds.load_feeds(tmp_path)


def test_an_empty_feed_directory_is_an_error_not_zero_checks(tmp_path):
    with pytest.raises(feeds.FeedConfigError, match="no feed configs"):
        feeds.load_feeds(tmp_path)


def test_clean_aggregated_table_passes_its_contract():
    contract = contracts.contract_for("aggregated_impressions.yaml")
    report = contracts.validate(aggregate(raw_rows(60)), contract, AS_OF)
    assert report.passed, [f for f in report.failures]


def test_contract_gate_fails_closed_on_each_expectation_kind():
    contract = contracts.contract_for("aggregated_impressions.yaml")
    table = aggregate(raw_rows(60))
    bad = table.to_pylist()
    bad[0]["funnel_depth"] = 9          # range
    bad[1]["impression_id"] = bad[2]["impression_id"]   # unique
    bad[3]["page_type"] = 4             # accepted_values
    bad[4]["user_id"] = None            # nullable: false
    broken = pa.Table.from_pylist(bad, schema=table.schema)
    report = contracts.validate(broken, contract, AS_OF)
    assert {f.kind for f in report.failures} == {"range", "unique", "accepted_values",
                                                 "not_null"}
    with pytest.raises(contracts.ContractViolation):
        contracts.assert_valid(broken, contract, AS_OF)


def test_range_on_an_empty_column_fails_rather_than_passing_vacuously():
    contract = contracts.Contract("t", 1, "x", (), ({"type": "range", "column": "v",
                                                   "min": 0, "max": 1},))
    table = pa.table({"v": pa.array([None, None], pa.int32())})
    assert not contracts.validate(table, contract, AS_OF).passed


def test_freshness_uses_the_logical_date_and_counts_unparseable_values():
    contract = contracts.Contract("t", 1, "x", (), ({"type": "freshness", "column": "d",
                                                   "max_lag_days": 1},))
    fresh = pa.table({"d": [datetime(2026, 6, 4), datetime(2026, 6, 3)]})
    assert contracts.validate(fresh, contract, AS_OF).passed
    assert not contracts.validate(fresh, contract, date(2026, 6, 9)).passed
    garbage = pa.table({"d": ["2026-06-04", "zzzz"]})
    result = contracts.validate(garbage, contract, AS_OF).results[0]
    assert not result.passed and "1 unparseable" in result.detail


def test_unknown_expectation_type_raises():
    contract = contracts.Contract("t", 1, "x", (), ({"type": "vibes", "column": "v"},))
    with pytest.raises(ValueError, match="unknown expectation type"):
        contracts.validate(pa.table({"v": [1]}), contract, AS_OF)


def test_table_registry_generates_ddl_and_arrow_schemas():
    ddl = tables.OPS_AGENT_RUNS.create_ddl("local.ops.agent_runs")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS local.ops.agent_runs (run_at DATE")
    assert "PARTITIONED BY (months(run_at))" in ddl
    assert "'format-version' = '2'" in tables.RAW_IMPRESSIONS.create_ddl("x")
    assert tables.RAW_IMPRESSIONS.arrow_schema().field("event_ts").type == pa.timestamp("us")
