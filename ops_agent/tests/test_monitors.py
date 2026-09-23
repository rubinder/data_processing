from datetime import date

import pytest

from ops_agent import config, feeds, monitors, runner, tables
from tests.conftest import AS_OF, raw_rows
from tests.fake_engine import FakeEngine


def test_monitor_definitions_load_from_feed_configs():
    defs = feeds.all_monitors()
    assert len(defs) >= 6
    assert {"row_count", "null_rate", "distribution_shift", "duplicate_rate",
            "cardinality"} <= {d.kind for d in defs}
    assert all(d.query.strip() for d in defs)


def test_evaluate_flags_a_collapse_against_baseline():
    status, detail = monitors.evaluate(10.0, [1000.0, 1010.0, 990.0, 1005.0],
                                       {"kind": "row_count", "breach_ratio": 0.5})
    assert status == "breach" and detail


def test_evaluate_passes_a_normal_value():
    status, _ = monitors.evaluate(995.0, [1000.0, 1010.0, 990.0, 1005.0],
                                  {"kind": "row_count", "breach_ratio": 0.5})
    assert status == "ok"


def test_evaluate_is_ok_with_no_baseline_rather_than_alarming():
    status, detail = monitors.evaluate(10.0, [], {"kind": "row_count"})
    assert status == "ok" and "no baseline" in detail.lower()


def test_absolute_limit_beats_missing_baseline():
    status, _ = monitors.evaluate(0.2, [], {"kind": "null_rate", "max_absolute": 0})
    assert status == "breach"


def test_distribution_shift_uses_robust_z_score():
    baseline = [100.0] * 10 + [101.0, 99.0]
    assert monitors.evaluate(500.0, baseline, {"kind": "distribution_shift"})[0] == "breach"
    assert monitors.evaluate(100.5, baseline, {"kind": "distribution_shift"})[0] == "ok"


def test_constant_baseline_does_not_divide_by_zero_and_tolerates_float_noise():
    assert monitors.evaluate(100.0, [100.0] * 8, {"kind": "distribution_shift"})[0] == "ok"
    assert monitors.evaluate(100.0 + 1e-12, [100.0] * 8,
                             {"kind": "distribution_shift"})[0] == "ok"
    assert monitors.evaluate(101.0, [100.0] * 8, {"kind": "distribution_shift"})[0] == "breach"


def test_unknown_kind_raises_instead_of_reporting_ok():
    with pytest.raises(ValueError, match="unknown monitor kind"):
        monitors.evaluate(1.0, [1.0, 2.0], {"kind": "row_cont"})


def test_a_typo_in_kind_is_rejected_at_config_load(tmp_path):
    (tmp_path / "typo.yaml").write_text(
        "name: typo_feed\nlayers: {raw: {table: db.impressions}}\nmonitors:\n"
        "  - name: typo_monitor\n    layer: raw\n    kind: row_cont\n"
        "    query: SELECT count(*) AS metric FROM t\n")
    with pytest.raises(feeds.FeedConfigError) as exc:
        feeds.load_feeds(tmp_path)
    assert "typo_monitor" in str(exc.value) and "row_count" in str(exc.value)


def test_an_unknown_kind_reaching_the_runner_is_a_breach_not_a_crash(fake, monkeypatch):
    bad = monitors.MonitorDef("typo_monitor", config.AGGREGATED_TABLE, "row_cont",
                              "SELECT count(*) AS metric FROM t")
    good = monitors.MonitorDef("fine_monitor", config.AGGREGATED_TABLE, "row_count",
                               "SELECT count(*) AS metric FROM t")

    class _OneFeed:
        name, monitors, typed_layers = "stub", (bad, good), ()

    monkeypatch.setattr(feeds, "load_feeds", lambda d=None, c=None: [_OneFeed()])
    results = runner.run_monitors(fake, as_of=AS_OF)
    typo = next(r for r in results if r.monitor == "typo_monitor")
    assert typo.status == "breach" and "unknown monitor kind" in typo.detail
    assert next(r for r in results if r.monitor == "fine_monitor").status == "ok"


def test_every_declared_monitor_runs_on_the_clean_lakehouse(fake):
    results = runner.run_monitors(fake, as_of=AS_OF)
    declared = {m.name for m in feeds.all_monitors()}
    assert declared <= {r.monitor for r in results}
    assert all(r.status == "ok" for r in results), [
        (r.monitor, r.detail) for r in results if r.status != "ok"]


def test_column_type_checks_are_generated_per_contract_field_and_stamped_as_of(fake):
    results = runner.run_monitors(fake, as_of=AS_OF)
    typed = [r for r in results if r.kind == "column_type"]
    assert len(typed) == len(tables.AGGREGATED_IMPRESSIONS.columns)
    assert {r.run_at for r in results} == {AS_OF}


def test_column_type_check_detects_a_declared_type_mismatch(fake):
    from ops_agent.contracts import Contract, SchemaField
    wrong = Contract(config.AGGREGATED_TABLE, 1, "x",
                     (SchemaField("funnel_depth", "string", False),), ())
    results = monitors.check_column_types(fake, tables.AGGREGATED_IMPRESSIONS, wrong, AS_OF)
    assert [r.status for r in results] == ["breach"]


def test_results_persist_and_are_readable_as_baselines(fake):
    results = runner.run_monitors(fake, as_of=AS_OF)
    assert runner.persist(fake, results) == len(results)
    target = next(r for r in results if r.metric is not None)
    assert runner.load_baselines(fake, target.monitor) == [target.metric]


def test_raw_volume_monitor_catches_a_collapsed_batch():
    """Cumulative count(*) barely moves when a near-total outage hits an
    append-only feed. The monitor measures rows added per snapshot instead."""
    engine = FakeEngine()
    engine.create_table(tables.RAW_IMPRESSIONS)
    for seed in range(5):
        engine.append(config.RAW_TABLE, raw_rows(60, seed=seed))
        runner.persist(engine, runner.run_monitors(engine, as_of=AS_OF))
    engine.append(config.RAW_TABLE, raw_rows(60, seed=99).slice(0, 1))
    results = runner.run_monitors(engine, as_of=AS_OF)
    volume = next(r for r in results if r.monitor == "raw_impressions_row_count")
    assert volume.metric == 1 and volume.status == "breach"


def test_rebuild_is_not_read_as_a_volume_spike(fake):
    """An overwrite's whole row count is not "rows added today"."""
    assert fake.snapshot_details(config.AGGREGATED_TABLE)[-1]["is_full_rebuild"]
    assert monitors.latest_incremental_added_rows(fake, config.AGGREGATED_TABLE) == 0.0
