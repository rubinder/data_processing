"""PySpark job that pulls data from an API and saves it to storage."""

import argparse
import gzip
import os
import sys

import requests
from dotenv import load_dotenv

from spark_applications.utils.mode import Mode, add_mode_argument, parse_mode
from spark_applications.utils.session import get_spark_session
from spark_applications.utils.storage import get_storage_adapter

load_dotenv()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Pull impression data from API"
    )
    add_mode_argument(parser)
    parser.add_argument(
        "--page_type", type=str, required=True,
        help="Page type to pull (1, 2, or 3)",
    )
    parser.add_argument(
        "--date", type=str, required=True,
        help="Date to pull (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--hour", type=str, required=True,
        help="Hour to pull (0-23)",
    )
    return parser.parse_args(argv)


def fetch_impression_data(
    base_url: str, page_type: str, date: str, hour: str
) -> bytes:
    """Fetch gzip CSV data from the impression API."""
    url = f"{base_url}/impression"
    params = {
        "page_type": page_type,
        "date": date,
        "hour": hour,
    }
    response = requests.get(url, params=params, timeout=300)
    response.raise_for_status()
    return response.content


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    mode = parse_mode(args)

    api_base_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    job_id = f"impression_{args.page_type}_{args.date}_{args.hour}"
    raw_path = (
        f"impressions/page_type={args.page_type}"
        f"/date={args.date}/hour={args.hour}/data.csv.gz"
    )
    table_path = "impressions"

    spark = get_spark_session("ApiPull", mode)
    storage = get_storage_adapter(mode)

    try:
        # Step 1: Update status to in_progress
        storage.update_status(spark, job_id, "in_progress")

        # Step 2: Fetch data from API
        print(f"Fetching data: page_type={args.page_type}, "
              f"date={args.date}, hour={args.hour}")
        raw_content = fetch_impression_data(
            api_base_url, args.page_type, args.date, args.hour,
        )

        # Step 3: Save raw file
        storage.save_raw_file(raw_content, raw_path)
        print(f"Saved raw file: {raw_path}")

        # Step 4: Read raw file into DataFrame
        # Decompress gzip before reading as CSV
        decompressed_path = raw_path.replace(".csv.gz", ".csv")
        decompressed_content = gzip.decompress(raw_content)
        storage.save_raw_file(decompressed_content, decompressed_path)
        df = storage.read_csv(spark, decompressed_path)
        print(f"Read {df.count()} rows from raw file")

        # Step 5: Write partitioned table
        storage.write_partitioned(
            df,
            table_path,
            partition_cols=["page_type", "date", "hour"],
        )
        print("Wrote partitioned table")

        # Step 6: Update status to completed
        storage.update_status(spark, job_id, "completed")
        print(f"Job {job_id} completed successfully")

    except Exception as e:
        print(f"Job {job_id} failed: {e}", file=sys.stderr)
        try:
            storage.update_status(spark, job_id, "failed")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
