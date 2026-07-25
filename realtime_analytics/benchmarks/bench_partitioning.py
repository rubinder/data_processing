"""How much does the partition key cost? Measured, at four granularities.

Partitioning is usually discussed only as a pruning win. It also has a price:
a query must consider every surviving part, and parts are created per
partition per insert. This script isolates that trade-off by building the same
20M rows with four partition keys -- none, monthly, weekly, daily -- and
running both a tenant-scoped query (where pruning cannot help, because the
sorting key already handles it) and a time-only query (where pruning is the
whole point).

Same methodology as bench_clickhouse.py, and for the same reason: measuring
one table to completion before starting the next produced swings of 3x on
repeat runs. Every table is warmed, then all of them are measured in
interleaved rounds.

Usage::

    python benchmarks/bench_partitioning.py --rows 20000000
"""
import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime_analytics import data_gen  # noqa: E402
from realtime_analytics.db import get_backend  # noqa: E402
from realtime_analytics.queries import Q1_TYPED, Q3_TYPED  # noqa: E402

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
WINDOW_START = "2026-06-01 00:00:00"
WINDOW_END = "2026-06-08 00:00:00"

VARIANTS = [
    ("part_none", "", "no partitioning"),
    ("part_month", "PARTITION BY toYYYYMM(event_ts)", "monthly (~3 partitions)"),
    ("part_week", "PARTITION BY toMonday(event_ts)", "weekly (~14 partitions)"),
    ("part_day", "PARTITION BY toDate(event_ts)", "daily (~91 partitions)"),
]

DDL = """
CREATE TABLE {name}
(
    event_id          UUID,
    conversation_id   UUID,
    account_id        LowCardinality(String),
    user_id           String CODEC(ZSTD(1)),
    agent_id          LowCardinality(String),
    event_type        LowCardinality(String),
    channel           LowCardinality(String),
    locale            LowCardinality(String),
    model             LowCardinality(String),
    intent            LowCardinality(String),
    event_ts          DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    ingest_ts         DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    latency_ms        UInt32 CODEC(T64, ZSTD(1)),
    prompt_tokens     UInt32 CODEC(T64, ZSTD(1)),
    completion_tokens UInt32 CODEC(T64, ZSTD(1)),
    sentiment         Float32 CODEC(ZSTD(1)),
    resolved          UInt8 CODEC(ZSTD(1)),
    escalation_reason LowCardinality(String)
)
ENGINE = MergeTree
{partition}
ORDER BY (account_id, event_ts)
"""


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def build(backend, rows: int, batch: int) -> None:
    columns = ", ".join(data_gen.TYPED_COLUMNS)
    log(f"Generating {rows:,} rows into part_none")
    backend.command("DROP TABLE IF EXISTS part_none")
    backend.command(DDL.format(name="part_none", partition=""))
    for offset in range(0, rows, batch):
        backend.command(
            data_gen.insert_typed("part_none", min(batch, rows - offset), offset)
        )
    for name, partition, _ in VARIANTS[1:]:
        log(f"Copying into {name}")
        backend.command(f"DROP TABLE IF EXISTS {name}")
        backend.command(DDL.format(name=name, partition=partition))
        backend.command(
            f"INSERT INTO {name} ({columns}) SELECT {columns} FROM part_none"
        )
    for name, _, _ in VARIANTS:
        backend.command(f"OPTIMIZE TABLE {name} FINAL")


def run(args) -> dict:
    backend = get_backend(
        args.backend, **({"path": args.db_path} if args.db_path else {})
    )
    backend.apply_settings(use_query_condition_cache=0)
    if not args.reuse:
        build(backend, args.rows, args.batch)

    account = backend.query(
        "SELECT account_id, count() AS c FROM part_none "
        "GROUP BY account_id ORDER BY c DESC LIMIT 1"
    ).rows[0]["account_id"]

    targets = []
    for name, _, label in VARIANTS:
        targets.append({
            "table": name, "label": label, "query": "tenant_dashboard",
            "sql": Q1_TYPED.replace("{table}", name),
            "params": {"account_id": account, "start": WINDOW_START,
                       "end": WINDOW_END},
            "walls": [], "result": None,
        })
        targets.append({
            "table": name, "label": label, "query": "platform_wide",
            "sql": Q3_TYPED.replace("{table}", name),
            "params": {"start": WINDOW_START, "end": WINDOW_END},
            "walls": [], "result": None,
        })

    log(f"Warming ({args.warmup} passes)")
    for _ in range(args.warmup):
        for target in targets:
            backend.query(target["sql"], target["params"])

    log(f"Measuring ({args.repeat} interleaved rounds)")
    for index in range(args.repeat):
        for target in targets:
            result = backend.query(target["sql"], target["params"])
            target["walls"].append(result.wall_s * 1000)
            if index == 0:
                target["result"] = result

    rows_out = []
    for target in targets:
        walls = sorted(target["walls"])
        parts = backend.query(
            "SELECT count() AS parts FROM system.parts "
            f"WHERE active AND table = '{target['table']}'"
        ).rows[0]["parts"]
        rows_out.append({
            "table": target["table"],
            "label": target["label"],
            "query": target["query"],
            "parts": int(float(parts)),
            "p50_ms": round(statistics.median(walls), 2),
            "p95_ms": round(walls[max(0, int(len(walls) * 0.95) - 1)], 2),
            "stdev_ms": round(statistics.stdev(walls), 2) if len(walls) > 1 else 0,
            "rows_read": target["result"].rows_read,
        })

    backend.close()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "rows": args.rows,
        "account_id": account,
        "repeat": args.repeat,
        "warmup": args.warmup,
        "measurements": rows_out,
    }


def format_markdown(results: dict) -> str:
    lines = [
        "# Partition granularity: pruning benefit vs per-part cost",
        "",
        f"- generated: `{results['generated_at']}`",
        f"- rows: `{results['rows']:,}`, tenant: `{results['account_id']}`",
        f"- `{results['repeat']}` interleaved rounds after "
        f"`{results['warmup']}` warmup passes",
        "",
    ]
    for query in ("tenant_dashboard", "platform_wide"):
        lines += [
            f"## {query}",
            "",
            "| partition key | parts | p50 ms | p95 ms | rows read |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for row in results["measurements"]:
            if row["query"] != query:
                continue
            lines.append(
                f"| {row['label']} | {row['parts']} | {row['p50_ms']} | "
                f"{row['p95_ms']} | {row['rows_read']:,} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000_000)
    parser.add_argument("--batch", type=int, default=5_000_000)
    parser.add_argument("--repeat", type=int, default=15)
    parser.add_argument("--warmup", type=int, default=4)
    parser.add_argument("--backend", default="chdb", choices=["chdb", "http"])
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--reuse", action="store_true")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    started = time.time()
    results = run(args)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    prefix = args.out or os.path.join(RESULTS_DIR, "partitioning")
    with open(f"{prefix}.json", "w") as handle:
        json.dump(results, handle, indent=2, default=str)
    markdown = format_markdown(results)
    with open(f"{prefix}.md", "w") as handle:
        handle.write(markdown)
    print()
    print(markdown)
    log(f"Done in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
