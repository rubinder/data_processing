"""
Load impression data from the web server API into the ClickHouse cluster.

Data is inserted into the Distributed `impressions` table, which shards rows
across clickhouse-01 and clickhouse-02 (backed by ReplicatedMergeTree).

Usage:
    python load_data.py --page_type 1 --date 2026-01-01 --hour 10
    python load_data.py --all  # loads page_type 1,2,3 for today at current hour
"""
import argparse
import csv
import gzip
import io
import os
from datetime import date as date_cls
from datetime import datetime

import clickhouse_connect
import requests

COLUMNS = [
    "user_id",
    "impression_id",
    "page_type",
    "date",
    "hour",
    "minute",
    "second",
    "event_type",
]


def get_client():
    """Create a ClickHouse client from environment variables."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def parse_csv_gz(raw: bytes) -> list[dict]:
    """Decompress a gzip CSV payload and return typed impression rows.

    The API's CSV header uses ``min``; it is mapped to ``minute`` to avoid
    clashing with ClickHouse's ``min`` aggregate function.
    """
    decompressed = gzip.decompress(raw).decode("utf-8")
    reader = csv.DictReader(io.StringIO(decompressed))
    rows = []
    for row in reader:
        rows.append(
            {
                "user_id": row["user_id"],
                "impression_id": row["impression_id"],
                "page_type": int(row["page_type"]),
                "date": row["date"],
                "hour": int(row["hour"]),
                "minute": int(row["min"]),
                "second": int(row["second"]),
                "event_type": row["event_type"],
            }
        )
    return rows


def fetch_and_parse(
    api_base_url: str, page_type: int, date: str, hour: int
) -> list[dict]:
    """Fetch the impression csv.gz from the API and parse it to typed rows."""
    url = f"{api_base_url}/impression"
    params = {"page_type": page_type, "date": date, "hour": hour}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return parse_csv_gz(response.content)


def load_impressions(
    client, api_base_url: str, page_type: int, date: str, hour: int
):
    """Fetch, idempotently replace, and insert one (page_type, date, hour) slice."""
    print(f"Fetching data: page_type={page_type}, date={date}, hour={hour}")
    rows = fetch_and_parse(api_base_url, page_type, date, hour)
    print(f"  Received {len(rows)} rows")

    # Idempotent reload: drop any existing slice on every shard first.
    client.command(
        "ALTER TABLE impressions_local ON CLUSTER impressions_cluster "
        "DELETE WHERE page_type = %(page_type)s AND date = %(date)s "
        "AND hour = %(hour)s",
        parameters={"page_type": page_type, "date": date, "hour": hour},
    )

    data = [
        [
            r["user_id"],
            r["impression_id"],
            r["page_type"],
            date_cls.fromisoformat(r["date"]),
            r["hour"],
            r["minute"],
            r["second"],
            r["event_type"],
        ]
        for r in rows
    ]
    if data:
        client.insert("impressions", data, column_names=COLUMNS)
    print(f"  Loaded {len(data)} rows into the distributed impressions table")


def main():
    parser = argparse.ArgumentParser(
        description="Load impression data into the ClickHouse cluster"
    )
    parser.add_argument("--page_type", type=int, choices=[1, 2, 3])
    parser.add_argument("--date", type=str)
    parser.add_argument("--hour", type=int)
    parser.add_argument(
        "--all", action="store_true", help="Load all page types for today"
    )
    args = parser.parse_args()

    api_base_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    client = get_client()

    if args.all:
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")
        hour = now.hour
        for pt in [1, 2, 3]:
            load_impressions(client, api_base_url, pt, date, hour)
    elif args.page_type and args.date and args.hour is not None:
        load_impressions(client, api_base_url, args.page_type, args.date, args.hour)
    else:
        parser.error("Provide --page_type, --date, --hour or use --all")


if __name__ == "__main__":
    main()
