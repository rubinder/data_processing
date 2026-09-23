"""What happens at 500 templates instead of 5.

For each template count: generate that many synthetic-but-valid templates
into a temp directory, time the load (the lint runs on every file), sync
them into the pgvector catalog (embed + write, then a no-op re-sync), then
measure catalog search latency through pgvector as the reader, and template
execution latency as the reader. Synthetic entries are removed afterwards.
Needs the dbt PostgreSQL running.

    uv run --extra test python benchmarks/bench_scale.py [--counts 20,100,500,2000] [--queries 50]
"""
from __future__ import annotations

import argparse
import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_deployment import catalog, config, db, templates  # noqa: E402
from mcp_deployment.embeddings import get_embedder  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"

TABLES = {
    "funnel_analysis": ["page_type", "event_type", "impressions_at_stage", "total_impressions"],
    "page_type_summary": ["page_type", "total_impressions", "unique_users", "avg_funnel_depth"],
    "user_engagement": ["user_id", "total_impressions", "page_types_visited", "total_events"],
    "hourly_traffic": ["event_date", "hour", "page_type", "total_impressions"],
}
VERBS = ["volume", "share", "trend", "ranking", "breakdown", "count", "rate", "distribution"]
QUALIFIERS = ["by page type", "per hour", "per user", "for a date range", "for one page type",
              "for the top users", "week over week", "at stage d"]


def synth_template(i: int, rng: random.Random) -> str:
    table = rng.choice(list(TABLES))
    cols = TABLES[table]
    col = rng.choice(cols[1:])
    verb, qual = rng.choice(VERBS), rng.choice(QUALIFIERS)
    return f"""name: synth_{i:05d}
description: >
  Synthetic template {i}: {verb} of {col} {qual} from {table}. Answers
  "what is the {verb} of {col.replace('_', ' ')} {qual}".
tags: [synthetic, {table}]
params:
  - name: limit
    type: int
    description: rows to return
    default: 10
    min: 1
    max: 1000
returns: [{cols[0]}, {col}]
max_rows: 1000
timeout_ms: 5000
sql: |
  SELECT {cols[0]}, {col} FROM gold.{table} ORDER BY {cols[0]} LIMIT %(limit)s
"""


def pct(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p * (len(ordered) - 1))))]


def run_count(n: int, queries: int, rng: random.Random, embedder, writer, reader) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i in range(n):
            (root / f"synth_{i:05d}.yaml").write_text(synth_template(i, rng))
        started = time.perf_counter()
        loaded = templates.load_templates(root)
        load_ms = (time.perf_counter() - started) * 1000

    entries = catalog.entries_from_templates(loaded)
    base = catalog.entries_from_dbt()
    started = time.perf_counter()
    first = catalog.sync(writer, base + entries, embedder)
    sync_ms = (time.perf_counter() - started) * 1000
    started = time.perf_counter()
    catalog.sync(writer, base + entries, embedder)
    resync_ms = (time.perf_counter() - started) * 1000
    assert first.inserted + first.updated + first.unchanged == len(base) + len(entries)

    search_ms = []
    for _ in range(queries):
        question = f"{rng.choice(VERBS)} of {rng.choice(list(TABLES))} {rng.choice(QUALIFIERS)}"
        started = time.perf_counter()
        catalog.search(reader, question, embedder, ("template",), 10)
        search_ms.append((time.perf_counter() - started) * 1000)

    # The planner sequential-scans a table this small and only reaches for
    # the HNSW index when it estimates that is cheaper. Forcing the index
    # shows what the same query costs once the table is big enough for the
    # planner to choose it on its own.
    forced_ms = []
    reader.execute("SET enable_seqscan = off")
    try:
        for _ in range(queries):
            question = f"{rng.choice(VERBS)} of {rng.choice(list(TABLES))} {rng.choice(QUALIFIERS)}"
            started = time.perf_counter()
            catalog.search(reader, question, embedder, ("template",), 10)
            forced_ms.append((time.perf_counter() - started) * 1000)
    finally:
        reader.execute("RESET enable_seqscan")
    plan = reader.execute(
        "EXPLAIN SELECT entry_id FROM catalog.entries WHERE embedder = %s AND kind = ANY(%s) "
        "ORDER BY embedding <=> %s::vector LIMIT 10",
        (embedder.model, ["template"], catalog.vector_literal(embedder.embed(["x"])[0]))
    ).fetchall()
    uses_index = any("hnsw" in row[0].lower() for row in plan)

    exec_ms = []
    names = list(loaded)
    for _ in range(queries):
        started = time.perf_counter()
        templates.execute(reader, loaded[rng.choice(names)], {"limit": 10})
        exec_ms.append((time.perf_counter() - started) * 1000)

    total = reader.execute("SELECT count(*) FROM catalog.entries").fetchone()[0]
    return {"templates": n, "catalog_rows": total, "load_ms": load_ms, "sync_ms": sync_ms,
            "resync_ms": resync_ms, "search_p50": pct(search_ms, .5), "search_p95": pct(search_ms, .95),
            "forced_p50": pct(forced_ms, .5), "planner_uses_index": uses_index,
            "exec_p50": pct(exec_ms, .5), "exec_p95": pct(exec_ms, .95)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--counts", default="20,100,500,2000,10000")
    parser.add_argument("--queries", type=int, default=50)
    parser.add_argument("--embedder", default="hash", help="hash | sentence-transformers")
    args = parser.parse_args()
    counts = [int(c) for c in args.counts.split(",")]
    rng = random.Random(7)
    embedder = get_embedder(args.embedder)

    with db.connect(config.role_db("transform")) as writer, \
            db.connect(config.role_db("mcp_reader")) as reader:
        rows = []
        try:
            for n in counts:
                rows.append(run_count(n, args.queries, rng, embedder, writer, reader))
                print(f"  {n:>5} templates: load {rows[-1]['load_ms']:.0f} ms, sync "
                      f"{rows[-1]['sync_ms']:.0f} ms, re-sync {rows[-1]['resync_ms']:.0f} ms, "
                      f"search p50 {rows[-1]['search_p50']:.1f} / p95 {rows[-1]['search_p95']:.1f} ms "
                      f"(planner index={rows[-1]['planner_uses_index']}, forced index p50 "
                      f"{rows[-1]['forced_p50']:.1f} ms), "
                      f"exec p50 {rows[-1]['exec_p50']:.1f} / p95 {rows[-1]['exec_p95']:.1f} ms")
        finally:
            # Put the catalog back to the real entries.
            real = catalog.entries_from_dbt() + catalog.entries_from_templates(
                templates.load_templates())
            catalog.sync(writer, real, get_embedder("hash"))

    lines = [f"# Scale — synthetic templates through load, pgvector sync, search and execution "
             f"({args.queries} queries each, `{embedder.model}`, HNSW cosine)", "",
             "| templates | catalog rows | load + lint | first sync (embed + write) | no-op re-sync "
             "| search p50 | search p95 | planner uses HNSW | search p50, index forced "
             "| execute p50 | execute p95 |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['templates']} | {r['catalog_rows']} | {r['load_ms']:.0f} ms "
                     f"| {r['sync_ms']:.0f} ms | {r['resync_ms']:.0f} ms | {r['search_p50']:.1f} ms "
                     f"| {r['search_p95']:.1f} ms | {'yes' if r['planner_uses_index'] else 'no'} "
                     f"| {r['forced_p50']:.1f} ms | {r['exec_p50']:.1f} ms | {r['exec_p95']:.1f} ms |")
    suffix = "" if args.embedder == "hash" else f"-{args.embedder}"
    out = RESULTS / f"scale{suffix}.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nwritten to {out.relative_to(HERE.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
