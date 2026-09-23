"""Concurrent load on the service layer: pooled vs a connection per call.

Threads call the same two operations an agent makes (search the catalog,
run a template) as ``mcp_reader``. Reports throughput and p50/p95 latency at
1, 8 and 32 concurrent callers, with the pool and without it. Needs the
dbt PostgreSQL running.

    uv run --extra test python benchmarks/bench_concurrency.py [--calls 400]
"""
from __future__ import annotations

import argparse
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_deployment.embeddings import HashEmbedder  # noqa: E402
from mcp_deployment.service import postgres_service  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
QUESTIONS = ["conversion rate by page type", "most engaged users", "traffic by hour",
             "funnel drop-off", "summary per page type"]


def pct(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p * (len(ordered) - 1))))]


def one_call(service, rng) -> float:
    started = time.perf_counter()
    if rng.random() < 0.5:
        service.search_catalog(rng.choice(QUESTIONS), ["template"], 5)
    else:
        service.run_template("funnel_by_page_type", {"page_type": rng.choice([1, 2, 3])})
    return (time.perf_counter() - started) * 1000


def run(service, workers: int, calls: int) -> dict:
    rng = random.Random(7)
    seeds = [random.Random(rng.random()) for _ in range(calls)]
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        latencies = list(pool.map(lambda r: one_call(service, r), seeds))
    elapsed = time.perf_counter() - started
    return {"workers": workers, "calls": calls, "throughput": calls / elapsed,
            "p50": pct(latencies, .5), "p95": pct(latencies, .95), "max": max(latencies)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--calls", type=int, default=400)
    args = parser.parse_args()
    rows = []
    for label, size in (("connection per call", 0), ("pool of 8", 8), ("pool of 32", 32)):
        service = postgres_service(embedder=HashEmbedder(), pool_size=size)
        try:
            for workers in (1, 8, 32):
                r = run(service, workers, args.calls)
                r["mode"] = label
                rows.append(r)
                print(f"  {label:<20} {workers:>2} workers: {r['throughput']:7.0f} calls/s, "
                      f"p50 {r['p50']:6.1f} ms, p95 {r['p95']:6.1f} ms, max {r['max']:6.1f} ms")
        finally:
            if service.extra.get("pool") is not None:
                service.extra["pool"].close()
    lines = [f"# Concurrency — {args.calls} mixed search/run calls per cell, as mcp_reader, "
             "hashing embedder", "",
             "| mode | concurrent callers | calls/s | p50 | p95 | max |", "|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['mode']} | {r['workers']} | {r['throughput']:.0f} | {r['p50']:.1f} ms "
                     f"| {r['p95']:.1f} ms | {r['max']:.1f} ms |")
    out = RESULTS / "concurrency.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nwritten to {out.relative_to(HERE.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
