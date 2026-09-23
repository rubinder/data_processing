"""Execute every monitor, judge against persisted history, persist the result.

The persisted history is the point. Baselines derived from the current run
are not baselines; ``ops.monitor_results`` is what a scheduled job accumulates
so that "anomalous" means something on day 30.
"""
from __future__ import annotations

import re
import statistics
import sys
from datetime import date

import pyarrow as pa

from ops_agent import contracts, feeds, monitors, tables
from ops_agent import alerts as alerts_mod

# Monitor names come from repo-owned YAML, never user input; the pattern is
# still enforced before a name is interpolated into SQL.
_MONITOR_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.]*$")


def load_baselines(engine, monitor: str, limit: int = 14) -> list[float]:
    if not engine.table_exists(tables.OPS_MONITOR_RESULTS.name):
        return []
    if not _MONITOR_NAME_RE.match(monitor):
        raise ValueError(f"unsafe monitor name: {monitor!r}")
    limit = int(limit)
    if limit <= 0:
        raise ValueError(f"limit must be positive: {limit!r}")
    rows = engine.sql(
        f"SELECT metric FROM r WHERE monitor = '{monitor}' AND metric IS NOT NULL "
        f"ORDER BY run_at DESC LIMIT {limit}",
        tables={"r": tables.OPS_MONITOR_RESULTS.name}).to_pylist()
    return [row["metric"] for row in rows]


def run_monitors(engine, as_of: date, feed_dir=None) -> list[monitors.MonitorResult]:
    """Every monitor declared by every feed, plus per-contract type checks."""
    results: list[monitors.MonitorResult] = []
    all_feeds = feeds.load_feeds(feed_dir)

    for definition in (m for f in all_feeds for m in f.monitors):
        required = (definition.table, *(definition.tables or {}).values())
        if not all(engine.table_exists(t) for t in required):
            continue
        try:
            if definition.source == "snapshot_added":
                metric = monitors.latest_incremental_added_rows(engine, definition.table)
            else:
                query_tables = {"t": definition.table, **(definition.tables or {})}
                out = engine.sql(definition.query, tables=query_tables)
                metric = float(out.column("metric")[0].as_py() or 0.0)
            baseline = load_baselines(engine, definition.name)
            params = {**(definition.params or {}), "kind": definition.kind}
            status, detail = monitors.evaluate(metric, baseline, params)
        except Exception as exc:  # noqa: BLE001 -- one broken monitor must not
            # take down the rest; it is reported as a breach so it is visible.
            results.append(monitors.MonitorResult(
                definition.name, definition.table, definition.column, definition.kind,
                None, None, "breach", f"monitor failed: {type(exc).__name__}: {exc}", as_of))
            continue
        results.append(monitors.MonitorResult(
            definition.name, definition.table, definition.column, definition.kind,
            metric, statistics.median(baseline) if baseline else None, status, detail, as_of))

    for feed in all_feeds:
        for layer in feed.typed_layers:
            table_def = feeds.table_def_for(layer.table)
            if not engine.table_exists(table_def.name):
                continue
            contract = contracts.contract_for(layer.contract)
            results += monitors.check_column_types(engine, table_def, contract, as_of)
    return results


def persist(engine, results: list[monitors.MonitorResult]) -> int:
    engine.create_table(tables.OPS_MONITOR_RESULTS)
    payload = [{
        "run_at": r.run_at, "monitor": r.monitor, "table_name": r.table,
        "column_name": r.column, "kind": r.kind, "metric": r.metric,
        "baseline_median": r.baseline, "status": r.status, "detail": r.detail,
    } for r in results]
    if payload:
        engine.append(tables.OPS_MONITOR_RESULTS.name, pa.Table.from_pylist(
            payload, schema=tables.OPS_MONITOR_RESULTS.arrow_schema()))
    return len(payload)


def route_alerts(engine, results: list[monitors.MonitorResult],
                 dry_run: bool = True) -> list[str]:
    engine.create_table(tables.OPS_ALERT_LOG)
    return alerts_mod.route(engine, alerts_mod.from_monitor_results(results), dry_run=dry_run)


def main() -> int:
    from ops_agent import config, lakehouse
    from ops_agent.engine import SparkEngine

    execute = "--execute" in sys.argv
    engine = SparkEngine()
    as_of = config.resolve_as_of_date(lakehouse.max_event_date(engine))
    results = run_monitors(engine, as_of)
    persist(engine, results)

    breaches = [r for r in results if r.status == "breach"]
    warns = [r for r in results if r.status == "warn"]
    print(f"monitors: {len(results)} checks at as_of={as_of} — "
          f"{len(breaches)} breach, {len(warns)} warn")
    for result in breaches + warns:
        print(f"  [{result.status}] {result.monitor}: {result.detail}")
    routed = route_alerts(engine, results, dry_run=not execute)
    for line in routed:
        print(f"  -> {line}")
    if routed and not execute:
        print("monitors: alert routing is dry-run (pass --execute to mark alerts delivered)")
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
