"""
Load impression data from the web server API into the dbt PostgreSQL database.

Usage:
    python load_data.py --page_type 1 --date 2026-01-01 --hour 10
    python load_data.py --all  # loads page_type 1,2,3 for today at current hour
"""
import argparse
import csv
import gzip
import io
import os
import sys
from datetime import datetime

import psycopg2
import requests


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DBT_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("DBT_POSTGRES_PORT", "5433")),
        dbname=os.getenv("DBT_POSTGRES_DB", "data_processing"),
        user=os.getenv("DBT_POSTGRES_USER", "dbt"),
        password=os.getenv("DBT_POSTGRES_PASSWORD", "dbt"),
    )


def load_impressions(api_base_url: str, page_type: int, date: str, hour: int):
    url = f"{api_base_url}/impression"
    params = {"page_type": page_type, "date": date, "hour": hour}

    print(f"Fetching data: page_type={page_type}, date={date}, hour={hour}")
    response = requests.get(url, params=params)
    response.raise_for_status()

    decompressed = gzip.decompress(response.content).decode("utf-8")
    reader = csv.DictReader(io.StringIO(decompressed))
    rows = list(reader)
    print(f"  Received {len(rows)} rows")

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM raw.impressions WHERE page_type = %s AND date = %s AND hour = %s",
        (page_type, date, hour),
    )

    insert_sql = """
        INSERT INTO raw.impressions (user_id, impression_id, page_type, date, hour, min, second, event_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
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
    cur.executemany(insert_sql, batch)
    conn.commit()
    print(f"  Loaded {len(batch)} rows into raw.impressions")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load impression data into dbt PostgreSQL")
    parser.add_argument("--page_type", type=int, choices=[1, 2, 3])
    parser.add_argument("--date", type=str)
    parser.add_argument("--hour", type=int)
    parser.add_argument("--all", action="store_true", help="Load all page types for today")
    args = parser.parse_args()

    api_base_url = os.getenv("API_BASE_URL", "http://localhost:8000")

    if args.all:
        now = datetime.now()
        date = now.strftime("%Y-%m-%d")
        hour = now.hour
        for pt in [1, 2, 3]:
            load_impressions(api_base_url, pt, date, hour)
    elif args.page_type and args.date and args.hour is not None:
        load_impressions(api_base_url, args.page_type, args.date, args.hour)
    else:
        parser.error("Provide --page_type, --date, --hour or use --all")


if __name__ == "__main__":
    main()
