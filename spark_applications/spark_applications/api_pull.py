"""PySpark job that pulls data from an API and saves it to storage."""

import argparse
import os
import sys
import time
from typing import Callable

import requests
from dotenv import load_dotenv

from spark_applications.utils.mode import Mode, add_mode_argument, parse_mode
from spark_applications.utils.observability import (
    get_logger,
    log_event,
    log_metrics,
)
from spark_applications.utils.quality import (
    reconcile_counts,
    split_on_contract,
)
from spark_applications.utils.schema import (
    CORRUPT_RECORD_COL,
    IMPRESSION_SCHEMA,
    schema_with_corrupt_column,
)
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
    base_url: str,
    page_type: str,
    date: str,
    hour: str,
    *,
    max_attempts: int = 4,
    backoff_base: float = 0.5,
    timeout: int = 300,
    sleep: Callable[[float], None] = time.sleep,
) -> bytes:
    """Fetch gzip CSV data from the impression API with retry + backoff.

    A single ``requests.get`` is fragile: any transient network blip or 5xx
    fails the whole job. We retry transient failures with exponential backoff
    (``backoff_base * 2**attempt``). The pull is a safe GET and downstream
    writes are idempotent, so retrying cannot double-apply data. ``sleep`` is
    injectable so tests don't actually wait.
    """
    url = f"{base_url}/impression"
    params = {"page_type": page_type, "date": date, "hour": hour}

    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt == max_attempts - 1:
                break
            wait = backoff_base * (2 ** attempt)
            print(
                f"API fetch attempt {attempt + 1}/{max_attempts} failed: "
                f"{exc}; retrying in {wait:.1f}s",
                file=sys.stderr,
            )
            sleep(wait)

    raise RuntimeError(
        f"API fetch failed after {max_attempts} attempts: {last_error}"
    )


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
    log = get_logger("api_pull")

    try:
        # Step 1: Update status to in_progress
        storage.update_status(spark, job_id, "in_progress")

        # Step 2: Fetch data from API
        log_event(
            log, "fetch_start", job=job_id,
            page_type=args.page_type, date=args.date, hour=args.hour,
        )
        raw_content = fetch_impression_data(
            api_base_url, args.page_type, args.date, args.hour,
        )

        # Step 3: Land the raw gzip file in the landing zone
        storage.save_raw_file(raw_content, raw_path)
        log_event(
            log, "raw_landed", job=job_id, path=raw_path,
            bytes=len(raw_content),
        )

        # Step 4: Read the gzip CSV directly with Spark.
        # Spark decompresses and parses .gz in a distributed manner, so we do
        # not pull/decompress the file on the driver. An explicit schema means
        # a single read pass (no inferSchema double-scan) and enforces the
        # column contract; PERMISSIVE mode captures rows that violate it.
        read_schema = schema_with_corrupt_column(IMPRESSION_SCHEMA)
        raw_df = storage.read_csv(
            spark,
            raw_path,
            schema=read_schema,
            corrupt_column=CORRUPT_RECORD_COL,
        )

        # Step 5: Enforce the contract. Conforming rows proceed; malformed or
        # schema-drifting rows are quarantined instead of corrupting the table.
        required = [f.name for f in IMPRESSION_SCHEMA.fields]
        split = split_on_contract(raw_df, required_cols=required)
        bad_count = split.quarantined.count()
        if bad_count:
            storage.write_quarantine(
                split.quarantined, f"{table_path}/{job_id}"
            )

        # Reconcile: every row read must be accounted for as either written or
        # quarantined — nothing silently dropped.
        valid_count = split.valid.count()
        reconcile_counts(
            split.total,
            valid_count + bad_count,
            label=f"{job_id} raw vs written+quarantined",
        )
        log_metrics(
            log, job=job_id,
            rows_read=split.total,
            rows_written=valid_count,
            rows_quarantined=bad_count,
        )

        # Step 6: Write partitioned table. Dynamic partition overwrite makes
        # re-running this (page_type, date, hour) idempotent without deleting
        # other partitions.
        storage.write_partitioned(
            split.valid,
            table_path,
            partition_cols=["page_type", "date", "hour"],
        )

        # Step 7: Update status to completed
        storage.update_status(spark, job_id, "completed")
        log_event(log, "job_completed", job=job_id, status="completed")

    except Exception as e:
        log_event(log, "job_failed", job=job_id, error=str(e))
        try:
            storage.update_status(spark, job_id, "failed")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
