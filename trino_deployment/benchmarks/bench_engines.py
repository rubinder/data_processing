"""The four analyses: Trino over Iceberg, Trino over PostgreSQL gold (already
computed by dbt), and native PostgreSQL reading gold. Best-of-N wall time per
query from the client. The data is toy-sized, so this measures engine
overhead, not throughput; the README says so.

    uv run --extra test python benchmarks/bench_engines.py [--runs 7]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trino_deployment import analyses, client  # noqa: E402

HERE = Path(__file__).resolve().parent
PG_DSN = "host=localhost port=5433 dbname=data_processing user=mcp_reader password=mcp_reader"


def best_of(fn, runs: int) -> tuple[float, float]:
    times = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        times.append((time.perf_counter() - started) * 1000)
    return min(times), statistics.median(times)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=7)
    args = parser.parse_args()
    conn = client.connect()
    pg = psycopg.connect(PG_DSN)
    rows = []
    for name, sql in analyses.ANALYSES.items():
        trino_iceberg = best_of(lambda: client.query(sql, conn), args.runs)
        trino_gold = best_of(lambda: client.query(f"SELECT * FROM postgres_gold.gold.{name}", conn),
                             args.runs)
        native = best_of(lambda: pg.execute(f"SELECT * FROM gold.{name}").fetchall(), args.runs)
        rows.append((name, trino_iceberg, trino_gold, native))
        print(f"  {name:<20} trino/iceberg {trino_iceberg[0]:7.1f} ms   trino/gold {trino_gold[0]:7.1f} ms"
              f"   native pg {native[0]:7.1f} ms")
    n = client.query(f"SELECT count(*) FROM iceberg.db.impressions", conn).rows[0][0]
    lines = [f"# Engines — the four analyses, best of {args.runs} runs, {n} Iceberg rows", "",
             "| analysis | Trino over Iceberg, computed | Trino over PostgreSQL gold, precomputed "
             "| native PostgreSQL gold, precomputed |", "|---|---|---|---|"]
    for name, a, b, c in rows:
        lines.append(f"| {name} | {a[0]:.1f} ms (median {a[1]:.1f}) | {b[0]:.1f} ms (median {b[1]:.1f}) "
                     f"| {c[0]:.1f} ms (median {c[1]:.1f}) |")
    out = HERE / "results" / "engines.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nwritten to {out.relative_to(HERE.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
