"""Capture ClickHouse's own index analysis as a committed artifact.

Latency numbers say *that* a schema change worked. ``EXPLAIN indexes = 1``
says *why*: it reports, per index, how many parts and granules survived
pruning. "18/1466 granules" is not an argument, it is the query planner's own
accounting, and it is the single most convincing thing to put in front of
someone who does not want to re-run a benchmark.

Runs on a deliberately small dataset -- the pruning *ratios* are what matter
and they show up at any size -- so this completes in seconds and needs no
Docker.

Usage::

    python benchmarks/explain_evidence.py
"""
import argparse
import os
import re
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime_analytics import data_gen  # noqa: E402
from realtime_analytics.db import get_backend, split_statements  # noqa: E402
from realtime_analytics.queries import Q1_TYPED, Q2_TYPED, Q3_TYPED  # noqa: E402

SQL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clickhouse"
)
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

WINDOW = {"start": "2026-06-01 00:00:00", "end": "2026-06-08 00:00:00"}

STAGE_FILES = [
    ("v1_typed", "01_types_codecs.sql", "events_v1_typed"),
    ("v2_sorted", "02_sorting_key.sql", "events_v2_sorted"),
    ("v3_partitioned", "03_partitioning.sql", "events_v3_partitioned"),
    ("v4_indexed", "04_skipping_indexes.sql", "events_v4_indexed"),
]

CASES = [
    {
        "key": "q1_tenant_dashboard",
        "title": "Tenant dashboard — WHERE account_id = ... AND event_ts IN (7 days)",
        "sql": Q1_TYPED,
        "params": lambda account, conv: {"account_id": account, **WINDOW},
        "note": "The sorting key is (account_id, event_ts), so this filter is a "
                "key prefix and collapses to a mark range.",
    },
    {
        "key": "q3_platform_wide",
        "title": "Platform-wide — WHERE event_ts IN (7 days), no tenant filter",
        "sql": Q3_TYPED,
        "params": lambda account, conv: dict(WINDOW),
        "note": "No tenant filter, so the leading key column buys nothing and "
                "only PARTITION BY can prune.",
    },
    {
        "key": "q2_conversation_lookup",
        "title": "Conversation drill-down — WHERE conversation_id = <uuid>",
        "sql": Q2_TYPED,
        "params": lambda account, conv: {"conversation_id": conv},
        "note": "A high-cardinality needle: neither the sorting key nor the "
                "partition key applies, which is what the bloom filter is for.",
    },
]

GRANULES_RE = re.compile(r"Granules:\s*(\d+)/(\d+)")
PARTS_RE = re.compile(r"Parts:\s*(\d+)/(\d+)")


def explain_lines(backend, sql: str, params: dict) -> list[str]:
    rows = backend.query("EXPLAIN indexes = 1 " + sql, params).rows
    return [str(list(row.values())[0]) for row in rows]


def total_granules(backend, table: str) -> int:
    """Marks in the table == granules a full scan must read."""
    rows = backend.query(
        "SELECT sum(marks) AS marks FROM system.parts "
        f"WHERE active AND table = '{table}'"
    ).rows
    return int(float(rows[0]["marks"])) if rows else 0


def summarize(lines: list[str]) -> dict:
    """Final parts/granules selected, and which index steps were consulted."""
    parts = PARTS_RE.findall("\n".join(lines))
    granules = GRANULES_RE.findall("\n".join(lines))
    indexes = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped in ("PrimaryKey", "Partition", "MinMax"):
            indexes.append(stripped)
        elif stripped.startswith("Name:"):
            indexes.append(stripped.split("Name:")[1].strip())
    return {
        "indexes": indexes,
        "parts_selected": int(parts[-1][0]) if parts else None,
        "parts_total": int(parts[-1][1]) if parts else None,
        "granules_selected": int(granules[-1][0]) if granules else None,
        "granules_total": int(granules[0][1]) if granules else None,
    }


def index_block(lines: list[str]) -> list[str]:
    """Just the ReadFromMergeTree / Indexes portion of the plan."""
    out, capturing = [], False
    for line in lines:
        if "ReadFromMergeTree" in line:
            capturing = True
        if capturing:
            out.append(line.rstrip())
    return out


def run(rows: int) -> str:
    backend = get_backend("chdb", path=tempfile.mkdtemp(prefix="explain-"))
    backend.apply_settings(use_query_condition_cache=0)

    for _, sql_file, _ in STAGE_FILES:
        for statement in split_statements(open(os.path.join(SQL_DIR, sql_file)).read()):
            backend.command(statement)

    print(f"Generating {rows:,} rows", flush=True)
    backend.command(data_gen.insert_typed("events_v1_typed", rows))
    columns = ", ".join(data_gen.TYPED_COLUMNS)
    for _, _, table in STAGE_FILES[1:]:
        backend.command(
            f"INSERT INTO {table} ({columns}) SELECT {columns} FROM events_v1_typed"
        )
    for _, _, table in STAGE_FILES:
        backend.command(f"OPTIMIZE TABLE {table} FINAL")

    account = backend.query(
        "SELECT account_id, count() AS c FROM events_v1_typed "
        "GROUP BY account_id ORDER BY c DESC LIMIT 1"
    ).rows[0]["account_id"]
    conversation = backend.query(
        "SELECT toString(conversation_id) AS cid FROM events_v1_typed LIMIT 1"
    ).rows[0]["cid"]

    doc = [
        "# Why the queries got faster: ClickHouse's own index analysis",
        "",
        "Output of `EXPLAIN indexes = 1`, which reports how many parts and",
        "granules survive each pruning step. A granule is 8192 rows; granules",
        "not selected are never read from disk.",
        "",
        f"- generated: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- rows: `{rows:,}` (ratios are what matter, and hold at any size)",
        f"- tenant: `{account}`",
        "- query condition cache disabled, so an unindexed table cannot borrow"
        " a previous run's granule matches",
        "",
        "Regenerate with `python benchmarks/explain_evidence.py`.",
        "",
    ]

    for case in CASES:
        doc += [f"## {case['title']}", "", case["note"], "",
                "| stage | indexes consulted | parts | granules read |",
                "| --- | --- | ---: | ---: |"]
        captured = {}
        for stage_key, _, table in STAGE_FILES:
            lines = explain_lines(
                backend, case["sql"].replace("{table}", table),
                case["params"](account, conversation),
            )
            captured[stage_key] = lines
            info = summarize(lines)
            if info["granules_selected"] is None:
                # ORDER BY tuple() produces no index block at all: there is
                # nothing to prune with, so every granule is read.
                total = total_granules(backend, table)
                idx = "none — `ORDER BY tuple()`"
                parts = "all"
                gran = f"**{total:,}**/{total:,} (full scan)"
            else:
                idx = ", ".join(dict.fromkeys(info["indexes"])) or "—"
                parts = f"{info['parts_selected']}/{info['parts_total']}"
                gran = (f"**{info['granules_selected']:,}**"
                        f"/{info['granules_total']:,}")
            doc.append(f"| `{stage_key}` | {idx} | {parts} | {gran} |")
        doc.append("")

        # Raw planner output for the stage where this query is fastest.
        best = STAGE_FILES[-1][0] if case["key"] == "q2_conversation_lookup" else (
            "v2_sorted" if case["key"] == "q1_tenant_dashboard" else "v3_partitioned"
        )
        doc += [f"<details><summary>Raw plan — <code>{best}</code></summary>", "",
                "```"] + index_block(captured[best]) + ["```", "</details>", ""]

    backend.close()
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, "explain.md")
    with open(path, "w") as handle:
        handle.write("\n".join(doc) + "\n")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=2_000_000)
    args = parser.parse_args()
    print(f"Wrote {run(args.rows)}")


if __name__ == "__main__":
    main()
