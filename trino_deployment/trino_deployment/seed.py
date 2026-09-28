"""Create and fill ``iceberg.db.impressions`` through Trino.

Two sources. ``from_postgres`` is the one that matters: a single
``INSERT ... SELECT`` pulls the raw layer out of PostgreSQL through the
``postgres_raw`` catalog (the ``ingestion`` role), derives the event
timestamp the same way dbt's staging model does, drops rows whose date does
not parse (dbt's ``safe_to_date`` does the same), and lands them in the
lakehouse. After that the Iceberg table and dbt's gold tables describe the
same events, which is what makes the reconciliation in ``federation.py`` a
check rather than a demo. ``synthetic`` seeds from the repository's shared
impression generator for a standalone run.
"""
from __future__ import annotations

import sys
from datetime import datetime

from trino_deployment import client

TABLE = "iceberg.db.impressions"

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS iceberg.db"

# Same columns, same hidden partitioning and format version as
# ../iceberg_deployment/iceberg_deployment/impressions.py, in Trino's DDL.
CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    user_id       varchar,
    impression_id varchar,
    page_type     integer,
    event_ts      timestamp(6),
    event_type    varchar
)
WITH (
    format_version = 2,
    partitioning = ARRAY['day(event_ts)', 'page_type']
)
"""

# try_cast, not cast: raw.impressions carries a row whose date is not a
# date, put there on purpose to prove the safe casts in dbt. It is skipped
# here for the same reason it is skipped there.
FROM_POSTGRES = f"""
INSERT INTO {TABLE}
SELECT user_id,
       impression_id,
       page_type,
       CAST(try_cast(date AS date) AS timestamp(6))
           + hour * INTERVAL '1' HOUR
           + min * INTERVAL '1' MINUTE
           + second * INTERVAL '1' SECOND AS event_ts,
       event_type
FROM postgres_raw.raw.impressions
WHERE try_cast(date AS date) IS NOT NULL
"""


def create(conn=None) -> None:
    client.query(CREATE_SCHEMA, conn)
    client.query(CREATE_TABLE, conn)


def truncate(conn=None) -> None:
    client.query(f"DELETE FROM {TABLE}", conn)


def from_postgres(conn=None) -> int:
    create(conn)
    return client.query(FROM_POSTGRES, conn).rows[0][0]


def synthetic(count: int = 200, seed_value: int = 7, start: datetime | None = None,
              conn=None) -> int:
    from iceberg_deployment.impressions import sample_rows

    create(conn)
    rows = sample_rows(count, start or datetime(2026, 6, 1), seed_value)
    values = ", ".join(
        f"('{u}', 'b{seed_value}_{i}', {p}, TIMESTAMP '{ts:%Y-%m-%d %H:%M:%S}', '{e}')"
        for u, i, p, ts, e in rows)
    client.query(f"INSERT INTO {TABLE} VALUES {values}", conn)
    return len(rows)


def row_count(conn=None) -> int:
    return client.query(f"SELECT count(*) FROM {TABLE}", conn).rows[0][0]


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "postgres"
    if mode == "postgres":
        n = from_postgres()
        print(f"seed: {n} rows from postgres_raw.raw.impressions -> {TABLE}")
    elif mode == "synthetic":
        n = synthetic()
        print(f"seed: {n} synthetic rows -> {TABLE}")
    elif mode == "truncate":
        truncate()
        print(f"seed: {TABLE} emptied")
    else:
        print("usage: python -m trino_deployment.seed [postgres | synthetic | truncate]")
        return 2
    print(f"seed: {TABLE} now holds {row_count()} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
