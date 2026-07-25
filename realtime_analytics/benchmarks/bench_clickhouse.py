"""Measure every ClickHouse optimization stage, one variable at a time.

The point of this script is that the numbers in README.md are reproducible.
It builds one dataset, materializes it into seven physically different tables,
and runs the same four queries against each, reporting wall-clock latency
percentiles alongside the engine's own ``rows_read`` / ``bytes_read``.

Method notes (they matter more than the numbers):

* **One variable per stage.** Stage N differs from stage N-1 in exactly one
  schema decision. Data is generated once and copied, so no stage can win by
  accident of different rows.
* **Results are verified, not assumed.** Every stage's answer is compared
  against the naive baseline's answer before a speedup is reported. A
  mismatch is a failure, not a footnote.
* **Warm cache, interleaved rounds.** Every table is warmed, then all stages
  are measured round-robin rather than one table at a time. Dashboards are
  served from a warm cache, and interleaving stops cache warmth or thermal
  drift from accumulating against whichever stage happens to run last.
* **The query condition cache is disabled.** It is on by default in
  ClickHouse 25+ and makes an unindexed table behave like an indexed one on
  the second execution of the same query.
* **Percentiles, not averages.** p95 is the target being engineered for, so
  p95 is what gets reported, with the standard deviation alongside it.

Usage::

    python benchmarks/bench_clickhouse.py --rows 20000000 --repeat 9
    python benchmarks/bench_clickhouse.py --backend http --rows 50000000
"""
import argparse
import json
import os
import re
import statistics
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from realtime_analytics import data_gen  # noqa: E402
from realtime_analytics.db import get_backend, split_statements  # noqa: E402
from realtime_analytics.queries import BENCH_QUERIES, QUERY_TITLES  # noqa: E402

SQL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clickhouse"
)
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

# The benchmark window: 7 days inside the generated 90-day span.
WINDOW_START = "2026-06-01 00:00:00"
WINDOW_END = "2026-06-08 00:00:00"

MV_RE = re.compile(
    r"CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+"
    r"TO\s+(\w+)\s+AS\s+(SELECT\b.*)",
    re.IGNORECASE | re.DOTALL,
)


class Stage:
    """One point on the tuning curve."""

    def __init__(self, key, label, sql_file, table, variant, queries=None):
        self.key = key
        self.label = label
        self.sql_file = sql_file
        self.table = table
        self.variant = variant
        self.queries = queries or list(BENCH_QUERIES)


STAGES = [
    Stage("v0_naive", "Naive: all String, JSON blob, ORDER BY tuple()",
          "00_naive.sql", "events_v0_naive", "naive"),
    Stage("v1_typed", "+ Physical types, LowCardinality, codecs",
          "01_types_codecs.sql", "events_v1_typed", "typed"),
    Stage("v2_sorted", "+ Sorting key (account_id, event_ts)",
          "02_sorting_key.sql", "events_v2_sorted", "typed"),
    Stage("v3_partitioned", "+ PARTITION BY toYYYYMM(event_ts)",
          "03_partitioning.sql", "events_v3_partitioned", "typed"),
    Stage("v4_indexed", "+ Skipping indexes (bloom_filter, minmax)",
          "04_skipping_indexes.sql", "events_v4_indexed", "typed"),
    Stage("v5_matview", "+ Materialized views (pre-aggregation)",
          None, "conversation_daily", "mv",
          queries=["q1_tenant_dashboard", "q3_platform_wide"]),
    Stage("v6_projection", "Alternative: projections instead of MV/bloom",
          "06_projections.sql", "events_v6_projected", "typed"),
]

#: Stages that hold a full copy of the typed events and can be filled by
#: copying from the first typed table rather than regenerating.
COPY_TARGETS = [
    "events_v2_sorted",
    "events_v3_partitioned",
    "events_v4_indexed",
    "events_v6_projected",
]


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def read_sql(name: str) -> str:
    with open(os.path.join(SQL_DIR, name)) as handle:
        return handle.read()


def apply_sql_file(backend, name: str) -> None:
    for statement in split_statements(read_sql(name)):
        backend.command(statement)


def drop_all(backend) -> None:
    for view in ("mv_conversation_daily", "mv_agent_hourly"):
        backend.command(f"DROP VIEW IF EXISTS {view}")
    tables = [s.table for s in STAGES] + ["agent_hourly"]
    for table in tables:
        backend.command(f"DROP TABLE IF EXISTS {table}")


def build_dataset(backend, rows: int, batch: int) -> None:
    """Create every table and populate it with identical logical data."""
    log("Creating tables")
    for stage in STAGES:
        if stage.sql_file:
            apply_sql_file(backend, stage.sql_file)

    log(f"Generating {rows:,} naive rows")
    for offset in range(0, rows, batch):
        count = min(batch, rows - offset)
        backend.command(data_gen.insert_naive("events_v0_naive", count, offset))

    log(f"Generating {rows:,} typed rows")
    for offset in range(0, rows, batch):
        count = min(batch, rows - offset)
        backend.command(data_gen.insert_typed("events_v1_typed", count, offset))

    # Copy rather than regenerate: guarantees every typed stage holds exactly
    # the same rows, so a stage can only win on physical layout.
    columns = ", ".join(data_gen.TYPED_COLUMNS)
    for target in COPY_TARGETS:
        log(f"Copying into {target}")
        backend.command(
            f"INSERT INTO {target} ({columns}) "
            f"SELECT {columns} FROM events_v1_typed"
        )

    # Merge everything down so part counts cannot skew the comparison.
    log("OPTIMIZE ... FINAL on every table (equalizing part counts)")
    for stage in STAGES:
        if stage.sql_file:
            backend.command(f"OPTIMIZE TABLE {stage.table} FINAL")


def build_materialized_views(backend) -> None:
    """Create the views, then backfill their targets from the view definition.

    A ClickHouse materialized view only sees rows inserted after it exists, so
    a real deployment always pairs ``CREATE MATERIALIZED VIEW`` with a
    backfill. Deriving the backfill from the view's own SELECT keeps the two
    from drifting apart -- a very common source of "the dashboard disagrees
    with the raw table" incidents.
    """
    for view in ("mv_conversation_daily", "mv_agent_hourly", "mv_platform_daily"):
        backend.command(f"DROP VIEW IF EXISTS {view}")
    for table in ("conversation_daily", "agent_hourly", "platform_daily"):
        backend.command(f"DROP TABLE IF EXISTS {table}")

    script = read_sql("05_materialized_views.sql")
    for statement in split_statements(script):
        backend.command(statement)
        match = MV_RE.search(statement)
        if match:
            _, target, select_body = match.groups()
            log(f"Backfilling {target}")
            backend.command(f"INSERT INTO {target} {select_body}")
    for table in ("conversation_daily", "agent_hourly", "platform_daily"):
        backend.command(f"OPTIMIZE TABLE {table} FINAL")


def pick_benchmark_subject(backend) -> tuple[str, str]:
    """Choose the busiest tenant, and one of its conversations in-window.

    The largest tenant is the honest target: it is the one whose dashboard is
    slow, and tuning for the median tenant hides the problem.
    """
    account = backend.query(
        "SELECT account_id, count() AS c FROM events_v1_typed "
        "GROUP BY account_id ORDER BY c DESC LIMIT 1"
    ).rows[0]["account_id"]
    conversation = backend.query(
        f"SELECT toString(conversation_id) AS cid FROM events_v1_typed "
        f"WHERE account_id = '{account}' "
        f"AND event_ts >= toDateTime('{WINDOW_START}') "
        f"AND event_ts < toDateTime('{WINDOW_END}') "
        f"LIMIT 1"
    ).rows[0]["cid"]
    return account, conversation


def params_for(query_key: str, variant: str, account: str, conversation: str) -> dict:
    if query_key == "q2_conversation_lookup":
        return {"conversation_id": conversation}
    params = {"start": WINDOW_START, "end": WINDOW_END}
    if query_key != "q3_platform_wide":
        params["account_id"] = account
    return params


def normalize(rows: list[dict]) -> list[dict]:
    """Round floats so t-digest jitter across variants does not fail equality."""
    out = []
    for row in rows:
        clean = {}
        for key, value in row.items():
            if isinstance(value, float):
                clean[key] = round(value, 3)
            elif isinstance(value, str):
                # Engine variants render numerics as strings in JSON.
                try:
                    clean[key] = round(float(value), 3)
                except ValueError:
                    clean[key] = value
            elif isinstance(value, datetime):
                clean[key] = value.isoformat(sep=" ")
            else:
                clean[key] = value
        out.append(clean)
    return out


#: Counts must match exactly. Quantiles are allowed to differ by this much,
#: because a t-digest merged from many partial states is not bit-identical to
#: one computed in a single pass. The actual observed difference is recorded
#: and reported rather than hidden.
QUANTILE_TOLERANCE = 0.05


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def compare_rows(baseline: list[dict], actual: list[dict]) -> tuple:
    """Compare a stage's answer against the naive baseline.

    Returns ``(matches, max_quantile_rel_diff, first_difference)``. Counts and
    rates must be exact; percentile columns are compared with a relative
    tolerance and the worst observed deviation is returned so it can be
    reported honestly.
    """
    if len(baseline) != len(actual):
        return False, 0.0, {
            "reason": "row_count",
            "expected": len(baseline),
            "actual": len(actual),
        }
    worst = 0.0
    for expected_row, actual_row in zip(baseline, actual):
        if set(expected_row) != set(actual_row):
            return False, worst, {
                "reason": "columns",
                "expected": sorted(expected_row),
                "actual": sorted(actual_row),
            }
        for key, expected in expected_row.items():
            got = actual_row[key]
            approximate = "latency" in key or "p50" in key or "p9" in key
            if approximate and _is_number(expected) and _is_number(got):
                denominator = max(abs(expected), 1.0)
                diff = abs(got - expected) / denominator
                worst = max(worst, diff)
                if diff > QUANTILE_TOLERANCE:
                    return False, worst, {
                        "reason": "quantile_drift",
                        "column": key,
                        "expected": expected,
                        "actual": got,
                        "rel_diff": round(diff, 4),
                    }
            elif expected != got:
                return False, worst, {
                    "reason": "value",
                    "column": key,
                    "expected": expected,
                    "actual": got,
                }
    return True, worst, None


def summarize(walls: list[float], engine: list[float], result) -> dict:
    """Turn a list of timings into the reported statistics."""
    ordered = sorted(walls)
    return {
        "p50_ms": round(statistics.median(ordered), 2),
        "p95_ms": round(ordered[max(0, int(len(ordered) * 0.95) - 1)], 2),
        "min_ms": round(ordered[0], 2),
        "max_ms": round(ordered[-1], 2),
        "stdev_ms": round(statistics.stdev(ordered), 2) if len(ordered) > 1 else 0.0,
        "engine_ms": round(statistics.median(engine), 2),
        "rows_read": result.rows_read,
        "bytes_read": result.bytes_read,
        "result_rows": len(result.rows),
    }


def table_storage(backend, table: str) -> dict:
    rows = backend.query(
        "SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, "
        "sum(data_uncompressed_bytes) AS uncompressed, count() AS parts "
        f"FROM system.parts WHERE active AND table = '{table}'"
    ).rows
    if not rows:
        return {}
    row = rows[0]
    to_int = lambda v: int(float(v or 0))  # noqa: E731
    on_disk = to_int(row.get("bytes_on_disk"))
    uncompressed = to_int(row.get("uncompressed"))
    return {
        "rows": to_int(row.get("rows")),
        "bytes_on_disk": on_disk,
        "uncompressed_bytes": uncompressed,
        "parts": to_int(row.get("parts")),
        "compression_ratio": round(uncompressed / on_disk, 2) if on_disk else 0,
    }


def run(args) -> dict:
    backend = get_backend(args.backend, **({"path": args.db_path}
                                           if args.backend == "chdb" and args.db_path
                                           else {}))
    # ClickHouse 25.x+ enables the query condition cache by default: after one
    # execution it remembers which granules matched a predicate, so a repeat of
    # the same query skips the rest. That is a genuine production speedup, but
    # in a benchmark it makes an UNINDEXED table look indexed on the second
    # run -- it silently erased most of the measured value of the bloom filter
    # here until it was found. Disabled so each stage is judged on its own
    # structure. The serving API leaves it on.
    backend.apply_settings(use_query_condition_cache=0)

    started = time.time()
    if not args.reuse:
        drop_all(backend)
        build_dataset(backend, args.rows, args.batch)
        build_materialized_views(backend)
    elif args.rebuild_views:
        # Iterate on the aggregate layer without regenerating the raw events.
        build_materialized_views(backend)
    log("Dataset ready")

    account, conversation = pick_benchmark_subject(backend)
    log(f"Benchmark subject: account={account} conversation={conversation}")

    results = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backend": backend.name,
        "rows": args.rows,
        "repeat": args.repeat,
        "warmup": args.warmup,
        "account_id": account,
        "conversation_id": conversation,
        "query_condition_cache": "disabled",
        "window": {"start": WINDOW_START, "end": WINDOW_END},
        "stages": {},
        "storage": {},
        "baselines": {},
    }

    # Build the full measurement matrix up front so it can be executed in
    # interleaved rounds. Measuring one table to completion before moving to
    # the next lets cache warmth and thermal drift accumulate against
    # whichever stage happens to run last -- an earlier revision of this
    # script did exactly that and reported a 2.6x "regression" for
    # partitioning that disappeared entirely once the order was interleaved.
    targets = []
    for stage in STAGES:
        results["stages"][stage.key] = {
            "label": stage.label, "table": stage.table, "queries": {},
        }
        for query_key in stage.queries:
            variants = BENCH_QUERIES[query_key]
            variant = stage.variant if stage.variant in variants else "typed"
            if stage.key == "v5_matview" and variant != "mv":
                continue
            # str.format would eat the {name:Type} query parameters.
            sql = variants[variant].replace("{table}", stage.table)
            targets.append({
                "stage": stage.key,
                "query": query_key,
                "sql": sql,
                "params": params_for(query_key, variant, account, conversation),
                "walls": [],
                "engine": [],
                "result": None,
            })

    log(f"Warming caches ({args.warmup} passes over {len(targets)} targets)")
    for _ in range(args.warmup):
        for target in targets:
            backend.query(target["sql"], target["params"])

    log(f"Measuring ({args.repeat} interleaved rounds)")
    for round_index in range(args.repeat):
        for target in targets:
            result = backend.query(target["sql"], target["params"])
            target["walls"].append(result.wall_s * 1000)
            target["engine"].append(result.elapsed_s * 1000)
            if round_index == 0:
                target["result"] = result

    for target in targets:
        measured = summarize(target["walls"], target["engine"], target["result"])
        rows_out = normalize(target["result"].rows)
        query_key = target["query"]
        if target["stage"] == "v0_naive":
            results["baselines"][query_key] = rows_out
            measured["matches_baseline"] = True
            measured["quantile_rel_diff"] = 0.0
        else:
            matches, worst, difference = compare_rows(
                results["baselines"].get(query_key, []), rows_out
            )
            measured["matches_baseline"] = matches
            measured["quantile_rel_diff"] = round(worst, 4)
            if not matches:
                measured["baseline_diff"] = difference
        results["stages"][target["stage"]]["queries"][query_key] = measured

    for stage in STAGES:
        results["storage"][stage.table] = table_storage(backend, stage.table)
    for table in ("agent_hourly", "platform_daily"):
        results["storage"][table] = table_storage(backend, table)

    results["duration_s"] = round(time.time() - started, 1)
    backend.close()
    return results


def format_markdown(results: dict) -> str:
    lines = [
        "# ClickHouse tuning benchmark",
        "",
        f"- generated: `{results['generated_at']}`",
        f"- backend: `{results['backend']}`",
        f"- rows: `{results['rows']:,}`",
        f"- tenant under test: `{results['account_id']}`",
        f"- window: `{results['window']['start']}` .. `{results['window']['end']}`",
        f"- timed rounds: `{results['repeat']}` interleaved across all stages,"
        f" after `{results['warmup']}` warmup passes",
        "",
    ]
    for query_key, title in QUERY_TITLES.items():
        lines += [f"## {title}", "",
                  "| stage | p50 ms | p95 ms | stdev | rows read | bytes read "
                  "| vs naive | correct |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |"]
        baseline = None
        for stage_key, stage in results["stages"].items():
            measured = stage["queries"].get(query_key)
            if not measured:
                continue
            if baseline is None:
                baseline = measured["p95_ms"]
            speedup = baseline / measured["p95_ms"] if measured["p95_ms"] else 0
            lines.append(
                f"| `{stage_key}` {stage['label']} | {measured['p50_ms']} | "
                f"{measured['p95_ms']} | {measured['stdev_ms']} | "
                f"{measured['rows_read']:,} | "
                f"{measured['bytes_read'] / 1e6:.1f} MB | {speedup:.1f}x | "
                f"{'yes' if measured['matches_baseline'] else 'NO'} |"
            )
        lines.append("")

    lines += ["## Storage", "",
              "| table | rows | on disk | uncompressed | ratio | parts |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for table, storage in results["storage"].items():
        if not storage:
            continue
        lines.append(
            f"| `{table}` | {storage['rows']:,} | "
            f"{storage['bytes_on_disk'] / 1e6:.1f} MB | "
            f"{storage['uncompressed_bytes'] / 1e6:.1f} MB | "
            f"{storage['compression_ratio']}x | {storage['parts']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000_000,
                        help="events to generate (default 20M)")
    parser.add_argument("--batch", type=int, default=5_000_000,
                        help="rows per INSERT batch")
    parser.add_argument("--repeat", type=int, default=9,
                        help="interleaved timed rounds over every query")
    parser.add_argument("--warmup", type=int, default=3,
                        help="untimed passes before measuring, so every table "
                             "reaches steady cache state")
    parser.add_argument("--backend", default="chdb", choices=["chdb", "http"])
    parser.add_argument("--db-path", default=None,
                        help="persistent chdb directory (default: temporary)")
    parser.add_argument("--reuse", action="store_true",
                        help="skip build, benchmark an existing --db-path")
    parser.add_argument("--rebuild-views", action="store_true",
                        help="with --reuse: rebuild and backfill only the "
                             "materialized-view layer")
    parser.add_argument("--out", default=None, help="output path prefix")
    args = parser.parse_args()

    results = run(args)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    prefix = args.out or os.path.join(RESULTS_DIR, "latest")
    with open(f"{prefix}.json", "w") as handle:
        json.dump(results, handle, indent=2, default=str)
    markdown = format_markdown(results)
    with open(f"{prefix}.md", "w") as handle:
        handle.write(markdown)

    # Regenerate the chart from the same run, so the committed SVG can
    # never disagree with the committed numbers.
    from make_chart import write_chart

    chart = write_chart(results, f"{prefix.replace('latest', 'tuning')}.svg")

    print()
    print(markdown)
    log(f"Wrote {prefix}.json, {prefix}.md and {chart} "
        f"in {results['duration_s']}s")


if __name__ == "__main__":
    main()
