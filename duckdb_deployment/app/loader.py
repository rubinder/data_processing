"""Load impression data from the web server API into the embedded DuckDB.

Mirrors dbt_deployment/load_data.py, but writes to an in-process DuckDB
connection instead of PostgreSQL.
"""
import csv
import gzip
import io

import duckdb
import requests


def load_impressions(
    conn: duckdb.DuckDBPyConnection,
    api_base_url: str,
    page_type: int,
    date: str,
    hour: int,
) -> int:
    """Fetch impressions for (page_type, date, hour) and load them into DuckDB.

    GETs ``{api_base_url}/impression``, gunzips the CSV payload, deletes any
    existing rows for the same (page_type, date, hour) partition and inserts
    the fresh rows. Returns the number of rows loaded.
    """
    url = f"{api_base_url}/impression"
    params = {"page_type": page_type, "date": date, "hour": hour}

    print(f"Fetching data: page_type={page_type}, date={date}, hour={hour}")
    response = requests.get(url, params=params)
    response.raise_for_status()

    decompressed = gzip.decompress(response.content).decode("utf-8")
    reader = csv.DictReader(io.StringIO(decompressed))
    rows = list(reader)
    print(f"  Received {len(rows)} rows")

    conn.execute(
        "DELETE FROM impressions "
        "WHERE page_type = ? AND date = ? AND hour = ?",
        [page_type, date, hour],
    )

    batch = [
        (
            row["user_id"],
            row["impression_id"],
            int(row["page_type"]),
            row["date"],
            int(row["hour"]),
            int(row["min"]),
            int(row["second"]),
            row["event_type"],
        )
        for row in rows
    ]
    if batch:
        conn.executemany(
            "INSERT INTO impressions "
            "(user_id, impression_id, page_type, date, hour, min, second, "
            "event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
    print(f"  Loaded {len(batch)} rows into impressions")
    return len(batch)
