"""Did the data arrive at all?

Row-level quality checks pass trivially when nothing new lands: an empty
delta has no nulls, no duplicates and no out-of-range values. Arrival
monitoring is the check that fires when every other check is silent.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, timedelta

from ops_agent import feeds
from ops_agent.monitors import MonitorResult


@dataclass(frozen=True)
class ArrivalSLA:
    table: str
    date_column: str
    calendar: str          # "daily" is the only calendar the impression feed needs
    max_lag_days: int
    min_rows_per_period: int | None


def arrival_slas(directory=None) -> tuple[ArrivalSLA, ...]:
    return tuple(
        ArrivalSLA(a.table, a.column, a.calendar, a.max_lag_periods, a.min_rows_per_period)
        for feed in feeds.load_feeds(directory) for a in feed.arrival)


def expected_periods(calendar: str, start: date, end: date) -> list[date]:
    if calendar != "daily":
        raise ValueError(f"unknown calendar: {calendar!r}")
    days, cursor = [], start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def missing_periods(observed: set[date], expected: list[date]) -> list[date]:
    return [d for d in expected if d not in observed]


def check_arrival(engine, sla: ArrivalSLA, as_of: date,
                  lookback_days: int = 30) -> list[MonitorResult]:
    if not engine.table_exists(sla.table):
        return [MonitorResult(f"{sla.table}_arrival", sla.table, None, "arrival_missing",
                              None, None, "breach", "table does not exist", as_of)]

    rows = engine.sql(
        f"SELECT CAST({sla.date_column} AS DATE) AS d, count(*) AS n FROM t GROUP BY 1",
        tables={"t": sla.table}).to_pylist()
    observed = {r["d"]: r["n"] for r in rows if r["d"] is not None}
    if not observed:
        return [MonitorResult(f"{sla.table}_arrival", sla.table, sla.date_column,
                              "arrival_missing", 0.0, None, "breach", "no data at all", as_of)]

    results: list[MonitorResult] = []
    newest = max(observed)
    lag = (as_of - newest).days
    results.append(MonitorResult(
        f"{sla.table}_arrival_lag", sla.table, sla.date_column, "arrival_lag", float(lag),
        None, "breach" if lag > sla.max_lag_days else "ok",
        f"newest {newest} is {lag}d behind as_of {as_of} (limit {sla.max_lag_days}d)", as_of))

    # The gap window runs through ``as_of`` itself, not just to ``newest``:
    # the days between the last arrival and the logical "now" are exactly
    # what a stalled feed looks like.
    window_start = max(newest - timedelta(days=lookback_days), min(observed))
    expected = expected_periods(sla.calendar, window_start, max(as_of, newest))
    gaps = missing_periods(set(observed), expected)
    results.append(MonitorResult(
        f"{sla.table}_arrival_gap", sla.table, sla.date_column, "arrival_gap",
        float(len(gaps)), None, "breach" if gaps else "ok",
        (f"{len(gaps)} missing period(s): {', '.join(str(d) for d in gaps[:5])}")
        if gaps else "no gaps in the expected calendar", as_of))

    if sla.min_rows_per_period is not None:
        thin = [d for d in expected if d in observed and observed[d] < sla.min_rows_per_period]
        results.append(MonitorResult(
            f"{sla.table}_arrival_partial", sla.table, sla.date_column, "arrival_partial",
            float(len(thin)), None, "warn" if thin else "ok",
            f"{len(thin)} period(s) below {sla.min_rows_per_period} rows"
            if thin else "all periods complete", as_of))
    return results


def main() -> int:
    from ops_agent import config, lakehouse
    from ops_agent.engine import SparkEngine

    engine = SparkEngine()
    as_of = config.resolve_as_of_date(lakehouse.max_event_date(engine))
    breaches = 0
    for sla in arrival_slas():
        for result in check_arrival(engine, sla, as_of):
            marker = {"ok": " ", "warn": "!", "breach": "X"}[result.status]
            print(f"[{marker}] {result.monitor}: {result.detail}")
            breaches += result.status == "breach"
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
