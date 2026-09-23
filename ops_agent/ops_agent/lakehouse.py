"""The two data tables the agent watches, and how they are built.

``db.impressions`` is the append-only event table ``iceberg_deployment``
creates and seeds; this module only appends to it. ``db.impressions_aggregated``
is rebuilt from it on every run, one row per impression, and is gated by its
contract *before* the overwrite lands -- validate, then publish -- so a build
that violates the contract leaves the previous good table in place.
"""
from __future__ import annotations

import sys
from datetime import date, datetime

import pyarrow as pa
from iceberg_deployment import impressions

from ops_agent import config, contracts, tables
from ops_agent.engine import df_to_arrow

AGGREGATE_SQL = """
SELECT impression_id,
       user_id,
       CAST(page_type AS INT)            AS page_type,
       CAST(min(event_ts) AS DATE)       AS event_date,
       CAST(count(*) AS INT)             AS funnel_depth,
       max(event_type)                   AS max_event_type,
       min(event_ts)                     AS first_event_ts,
       max(event_ts)                     AS last_event_ts
FROM {raw}
GROUP BY impression_id, user_id, page_type
"""


def ensure_tables(engine) -> None:
    engine.create_table(tables.RAW_IMPRESSIONS)
    engine.create_table(tables.AGGREGATED_IMPRESSIONS)


def seed_raw(engine, count: int = 200, start: datetime | None = None,
             seed_value: int = 7) -> int:
    """Append one deterministic batch of impressions. One batch, one snapshot.

    ``impressions.sample_rows`` numbers impressions from zero on every call,
    so two batches would share ids and the aggregated table's ``unique``
    expectation would fail on the second build. Ids are prefixed with the
    batch seed, which is what a real producer's batch id does.
    """
    engine.create_table(tables.RAW_IMPRESSIONS)
    return append_rows(engine, batch_rows(count, start, seed_value))


def batch_rows(count: int, start: datetime | None, seed_value: int) -> list[tuple]:
    return [(user, f"b{seed_value}_{imp}", page, ts, event)
            for user, imp, page, ts, event in impressions.sample_rows(count, start, seed_value)]


def append_rows(engine, rows: list[tuple]) -> int:
    """Append explicit rows shaped like ``impressions.sample_rows`` output,
    against the table's *current* schema (so it survives a rename)."""
    qualified = engine.qualified(config.RAW_TABLE)
    impressions.rows_to_df(engine.spark, qualified, rows).writeTo(qualified).append()
    return len(rows)


def max_event_date(engine) -> date | None:
    if not engine.table_exists(config.RAW_TABLE):
        return None
    rows = engine.sql("SELECT max(event_ts) AS m FROM t",
                      tables={"t": config.RAW_TABLE}).to_pylist()
    newest = rows[0]["m"] if rows else None
    return newest.date() if newest is not None else None


def build_aggregated(engine, as_of: date | None = None,
                     contract_file: str = "aggregated_impressions.yaml") -> int:
    """Rebuild the aggregated table. Contract-gated: validate, then publish."""
    engine.create_table(tables.AGGREGATED_IMPRESSIONS)
    as_of = as_of or config.resolve_as_of_date(max_event_date(engine))
    df = engine.spark.sql(AGGREGATE_SQL.format(raw=engine.qualified(config.RAW_TABLE)))
    built: pa.Table = df_to_arrow(df)
    contracts.assert_valid(built, contracts.contract_for(contract_file), as_of)
    engine.overwrite(config.AGGREGATED_TABLE, built)
    return built.num_rows


def main() -> int:
    from ops_agent.engine import SparkEngine

    command = sys.argv[1] if len(sys.argv) > 1 else "build"
    engine = SparkEngine()
    if command == "seed":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 200
        written = seed_raw(engine, count=count, seed_value=len(engine.snapshots(config.RAW_TABLE)) + 7)
        print(f"seed: appended {written} rows to {config.RAW_TABLE}")
    elif command == "build":
        rows = build_aggregated(engine)
        print(f"build: {config.AGGREGATED_TABLE} rebuilt with {rows} rows")
    else:
        print("usage: python -m ops_agent.lakehouse [seed [count] | build]")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
