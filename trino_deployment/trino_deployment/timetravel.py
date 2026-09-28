"""Snapshots and time travel through Trino's Iceberg syntax.

``"<table>$snapshots"`` is the metadata table; ``FOR VERSION AS OF`` and
``FOR TIMESTAMP AS OF`` read the table as it was. Same snapshots Spark sees
in ../iceberg_deployment/time_travel.py, because it is the same table.
"""
from __future__ import annotations

import sys

from trino_deployment import client
from trino_deployment.seed import TABLE


def _split(table: str) -> tuple[str, str]:
    catalog_schema, name = table.rsplit(".", 1)
    return catalog_schema, name


def snapshots(table: str = TABLE, conn=None) -> list[dict]:
    catalog_schema, name = _split(table)
    return client.query(
        f'SELECT snapshot_id, parent_id, committed_at, operation, summary '
        f'FROM {catalog_schema}."{name}$snapshots" ORDER BY committed_at', conn).dicts()


def count_at(snapshot_id: int, table: str = TABLE, conn=None) -> int:
    return client.query(f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot_id}",
                        conn).rows[0][0]


def partitions(table: str = TABLE, conn=None) -> list[dict]:
    catalog_schema, name = _split(table)
    return client.query(
        f'SELECT partition, record_count, file_count FROM {catalog_schema}."{name}$partitions" '
        f'ORDER BY record_count DESC', conn).dicts()


def main() -> int:
    snaps = snapshots()
    print(f"snapshots of {TABLE}: {len(snaps)}")
    for s in snaps:
        added = (s["summary"] or {}).get("added-records", "?")
        print(f"  {s['snapshot_id']}  {s['committed_at']}  {s['operation']:<9} "
              f"added-records={added}  rows then={count_at(s['snapshot_id'])}")
    parts = partitions()
    print(f"partitions: {len(parts)} (day(event_ts), page_type); "
          f"largest {parts[0]['record_count'] if parts else 0} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
