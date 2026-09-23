from datetime import date

import pyarrow as pa

from ops_agent import alerts, arrival, config, runner, tables
from ops_agent.monitors import MonitorResult
from tests.conftest import AS_OF, next_day
from tests.fake_engine import FakeEngine


def test_missing_periods_finds_gaps_in_the_middle_and_none_when_complete():
    expected = [date(2026, 6, d) for d in (1, 2, 3, 4, 5)]
    observed = {date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 5)}
    assert arrival.missing_periods(observed, expected) == [date(2026, 6, 3), date(2026, 6, 4)]
    assert arrival.missing_periods(set(expected), expected) == []


def _sla(**kw):
    base = dict(table=config.AGGREGATED_TABLE, date_column="event_date", calendar="daily",
                max_lag_days=1, min_rows_per_period=None)
    base.update(kw)
    return arrival.ArrivalSLA(**base)


def test_arrival_passes_on_a_complete_feed(fake):
    results = arrival.check_arrival(fake, _sla(), AS_OF)
    assert results and all(r.status == "ok" for r in results), [r.detail for r in results]
    assert not any(r.kind == "arrival_partial" for r in results)


def test_arrival_breaches_when_as_of_runs_ahead_of_the_data(fake):
    results = arrival.check_arrival(fake, _sla(), next_day(AS_OF, 3))
    kinds = {r.kind: r for r in results}
    assert kinds["arrival_lag"].status == "breach"
    gap = kinds["arrival_gap"]
    assert gap.status == "breach" and gap.metric == 3 and "2026-06-05" in gap.detail


def test_arrival_gap_window_runs_through_as_of_not_just_to_newest(fake):
    """One day after the last arrival: lag is within limit, but the gap
    check must still see that today's period is missing."""
    results = {r.kind: r for r in arrival.check_arrival(fake, _sla(), next_day(AS_OF))}
    assert results["arrival_lag"].status == "ok"
    assert results["arrival_gap"].status == "breach"


def test_arrival_partial_warns_below_a_structural_floor(fake):
    results = {r.kind: r for r in arrival.check_arrival(fake, _sla(min_rows_per_period=10_000),
                                                       AS_OF)}
    assert results["arrival_partial"].status == "warn"


def test_arrival_on_a_missing_or_empty_table_is_a_breach():
    engine = FakeEngine()
    (missing,) = arrival.check_arrival(engine, _sla(), AS_OF)
    assert missing.kind == "arrival_missing" and missing.status == "breach"
    engine.create_table(tables.AGGREGATED_IMPRESSIONS)
    (empty,) = arrival.check_arrival(engine, _sla(), AS_OF)
    assert empty.detail == "no data at all" and empty.status == "breach"


def _result(status="breach", monitor="m1"):
    return MonitorResult(monitor, config.AGGREGATED_TABLE, "id", "row_count", 10.0, 1000.0,
                         status, "collapsed", AS_OF)


def test_only_breaches_and_warns_become_alerts_with_stable_keys():
    made = alerts.from_monitor_results([_result("ok"), _result("warn", "m2"),
                                        _result("breach", "m3")])
    assert {a.severity for a in made} == {"warn", "breach"} and len(made) == 2
    assert alerts.from_monitor_results([_result()])[0].key == \
        alerts.from_monitor_results([_result()])[0].key


def test_route_is_dry_run_throttles_repeats_and_logs():
    engine = FakeEngine()
    made = alerts.from_monitor_results([_result()])
    first = alerts.route(engine, made, dry_run=True)
    assert first and all("DRY-RUN" in line for line in first)
    second = alerts.route(engine, made, dry_run=True)
    assert second and all("throttled" in line.lower() for line in second)
    log = engine.scan_arrow(tables.OPS_ALERT_LOG.name)
    assert log.num_rows == 1 and log.column("delivered").to_pylist() == [False]
    other = alerts.route(engine, alerts.from_monitor_results([_result(monitor="m9")]))
    assert other and not any("throttled" in s.lower() for s in other)


def test_monitor_run_routes_alerts_and_a_clean_run_routes_nothing():
    engine = FakeEngine()
    routed = runner.route_alerts(engine, [_result("ok"), _result("breach", "b"),
                                          _result("warn", "w")])
    assert len(routed) == 2
    assert runner.route_alerts(engine, [_result("ok")]) == []
    assert isinstance(engine.scan_arrow(tables.OPS_ALERT_LOG.name), pa.Table)
