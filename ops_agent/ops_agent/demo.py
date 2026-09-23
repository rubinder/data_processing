"""The whole story in order, printed as it happens.

1. Seed the raw table in three batches (three snapshots of history) and
   build the aggregated table through its contract gate.
2. Run the monitors after each batch so baselines exist, then the agent:
   a clean lakehouse must produce zero findings, or every later finding is
   meaningless.
3. Evolve the schema -- add a column, widen a type, rename a column -- and
   append a collapsed batch carrying an event_type the contract does not
   register. None of it rewrites a data file.
4. Run the monitors and the agent one logical day later, then the report,
   which diffs against the clean day.

Runs against a fresh local warehouse by default so the output is
reproducible; the README records one run of it.
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime, timedelta

from iceberg_deployment import schema_evolution

from ops_agent import config, graph, lakehouse, report, runner


def _banner(title: str) -> None:
    print(f"\n=== {title}")


def _print_monitors(results) -> None:
    for r in sorted(results, key=lambda x: (x.status != "breach", x.monitor)):
        marker = {"ok": " ", "warn": "!", "breach": "X"}[r.status]
        print(f"  [{marker}] {r.monitor}: {r.detail}")


def _print_agent(state) -> None:
    print(f"  as_of={state['as_of']} findings={len(state['findings'])}")
    for item in state["classified"]:
        print(f"  [{item['severity']}] {item['finding'].detail}")
    for action in state.get("actions_taken", []):
        print(f"  -> {action}")


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    warehouse = os.environ.get("ICEBERG_WAREHOUSE",
                               os.path.join(config.MODULE_ROOT, "iceberg-warehouse"))
    if "--keep" not in argv and os.path.exists(warehouse):
        shutil.rmtree(warehouse)
    os.environ["ICEBERG_WAREHOUSE"] = warehouse

    from ops_agent.engine import SparkEngine
    engine = SparkEngine()
    raw = config.RAW_TABLE
    start = datetime(2026, 6, 1)

    _banner("1. seed three batches, build the aggregated table")
    for batch, seed_value in enumerate((7, 11, 13), start=1):
        rows = lakehouse.seed_raw(engine, count=200, start=start, seed_value=seed_value)
        print(f"  batch {batch}: appended {rows} rows (snapshot {len(engine.snapshots(raw))})")
    as_of = lakehouse.max_event_date(engine)
    built = lakehouse.build_aggregated(engine, as_of=as_of)
    print(f"  aggregated: {built} impressions, contract-gated before publish")
    print(f"  logical date (newest event): {as_of}")

    _banner("2. monitors x3 for baselines, then a clean agent run")
    for _ in range(3):
        results = runner.run_monitors(engine, as_of)
        runner.persist(engine, results)
    _print_monitors(results)
    routed = runner.route_alerts(engine, results)
    print(f"  alerts routed: {len(routed)}")
    state = graph.run(engine=engine, dry_run=True, as_of=as_of)
    _print_agent(state)
    path, _ = report.write_report(engine, as_of)
    print(f"  report -> {path.relative_to(config.MODULE_ROOT)}")

    _banner("3. evolve the schema and inject drift (no data file rewritten)")
    before = engine.schema_history(raw)[-1]["field_ids"]
    schema_evolution.add_column(engine.spark, engine.qualified(raw), "campaign_id", "STRING")
    print("  v2: ADD COLUMN campaign_id STRING")
    schema_evolution.widen_column(engine.spark, engine.qualified(raw), "page_type", "BIGINT")
    print("  v3: ALTER COLUMN page_type TYPE BIGINT")
    schema_evolution.rename_column(engine.spark, engine.qualified(raw), "event_type", "event_name")
    after = engine.schema_history(raw)[-1]["field_ids"]
    print(f"  v4: RENAME COLUMN event_type TO event_name "
          f"(field id {before['event_type']} -> {after['event_name']})")
    next_day = as_of + timedelta(days=1)
    collapsed = [(f"user_{i:04d}", f"imp_late_{i}", 2,
                  datetime.combine(next_day, datetime.min.time()) + timedelta(minutes=i), "g")
                 for i in range(5)]
    lakehouse.append_rows(engine, collapsed)
    print(f"  appended a collapsed batch: {len(collapsed)} rows, event_name='g'")
    files = engine.spark.sql(
        f"SELECT count(*) AS n FROM {engine.qualified(raw)}.files").collect()[0]["n"]
    print(f"  data files now: {files}; snapshots: {len(engine.snapshots(raw))}")

    _banner(f"4. monitors + agent one day later ({next_day}), then the report")
    results = runner.run_monitors(engine, next_day)
    runner.persist(engine, results)
    _print_monitors(results)
    routed = runner.route_alerts(engine, results)
    for line in routed:
        print(f"  -> {line}")
    state = graph.run(engine=engine, dry_run=True, as_of=next_day)
    _print_agent(state)
    path, snapshot = report.write_report(engine, next_day)
    print(f"  report -> {path.relative_to(config.MODULE_ROOT)} "
          f"(diffed against {snapshot.previous_run})")
    print(f"\nwarehouse kept at {warehouse}; incidents in "
          f"{config.INCIDENTS_DIR.relative_to(config.MODULE_ROOT)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
