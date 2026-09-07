"""PySpark job that pulls data from an API and saves it to storage.

Landing is transactional (``utils/landing.py``): the raw gzip is staged, the
table is written from the staged bytes, and only then is the raw file
promoted to its partition path and a ``_manifest.json`` published with the
row counts and checksum. A failure anywhere in between deletes the staged
file, so no run can leave a raw file that the table does not account for.

Before the table write the batch is checked against its own history
(``quality.check_volume``: same page_type and hour on previous days, read
from those days' manifests) and against the contract
(``quality.check_quarantine_ratio``). A contract breach aborts the landing;
a volume anomaly is logged as a warning by default and aborts only when
``VOLUME_CHECK_MODE=fail``.
"""

import argparse
import os
import sys
import time
from datetime import date as date_type
from datetime import timedelta
from typing import Callable

import requests
from dotenv import load_dotenv

from spark_applications.utils.landing import (
    landed_raw_file,
    manifest_path_for,
    raw_path_for,
)
from spark_applications.utils.mode import add_mode_argument, parse_mode
from spark_applications.utils.observability import (
    get_logger,
    log_event,
    log_metrics,
)
from spark_applications.utils.quality import (
    DEFAULT_MAX_QUARANTINE_RATIO,
    check_quarantine_ratio,
    check_volume,
    reconcile_counts,
    split_on_contract,
)
from spark_applications.utils.schema import (
    CORRUPT_RECORD_COL,
    IMPRESSION_SCHEMA,
    schema_with_corrupt_column,
)
from spark_applications.utils.session import get_spark_session
from spark_applications.utils.storage import (
    StorageAdapter,
    get_storage_adapter,
)

load_dotenv()

# How many previous days of the same (page_type, hour) slice form the volume
# baseline, and whether an anomaly is a warning or a failure.
VOLUME_BASELINE_DAYS = int(os.getenv("VOLUME_BASELINE_DAYS", "7"))
VOLUME_CHECK_MODE = os.getenv("VOLUME_CHECK_MODE", "warn")
MAX_QUARANTINE_RATIO = float(
    os.getenv("MAX_QUARANTINE_RATIO", str(DEFAULT_MAX_QUARANTINE_RATIO))
)


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


def baseline_row_counts(
    storage: StorageAdapter,
    page_type: str,
    date: str,
    hour: str,
    days: int = VOLUME_BASELINE_DAYS,
) -> list[int]:
    """``rows_written`` from the manifests of the same slice on prior days.

    Days with no manifest (never pulled, or landed before manifests existed)
    are skipped rather than counted as zero, so a young table simply has a
    shorter baseline.
    """
    anchor = date_type.fromisoformat(date)
    counts = []
    for back in range(1, days + 1):
        previous = (anchor - timedelta(days=back)).isoformat()
        manifest = storage.read_raw_manifest(
            manifest_path_for(raw_path_for(page_type, previous, hour))
        )
        if manifest and "rows_written" in manifest:
            counts.append(int(manifest["rows_written"]))
    return counts


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    mode = parse_mode(args)

    api_base_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    job_id = f"impression_{args.page_type}_{args.date}_{args.hour}"
    raw_path = raw_path_for(args.page_type, args.date, args.hour)
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

        # Step 3-6 run inside the landing transaction: the raw bytes are
        # staged, the table is written from the staged copy, and the raw
        # file is promoted + its manifest published only if everything below
        # succeeds. Any exception aborts the landing (staged file deleted).
        with landed_raw_file(
            storage, raw_content, job_id, raw_path
        ) as landing:
            log_event(
                log, "raw_staged", job=job_id, path=landing.read_path,
                bytes=landing.raw_bytes, sha256=landing.raw_sha256,
            )

            # Step 4: Read the gzip CSV directly with Spark.
            # Spark decompresses and parses .gz in a distributed manner, so
            # we do not pull/decompress the file on the driver. An explicit
            # schema means a single read pass (no inferSchema double-scan)
            # and enforces the column contract; PERMISSIVE mode captures
            # rows that violate it.
            read_schema = schema_with_corrupt_column(IMPRESSION_SCHEMA)
            raw_df = storage.read_csv(
                spark,
                landing.read_path,
                schema=read_schema,
                corrupt_column=CORRUPT_RECORD_COL,
            )

            # Step 5: Enforce the contract. Conforming rows proceed;
            # malformed or schema-drifting rows are quarantined instead of
            # corrupting the table.
            required = [f.name for f in IMPRESSION_SCHEMA.fields]
            split = split_on_contract(raw_df, required_cols=required)
            bad_count = split.quarantined.count()
            if bad_count:
                storage.write_quarantine(
                    split.quarantined, f"{table_path}/{job_id}"
                )

            # Reconcile: every row read must be accounted for as either
            # written or quarantined — nothing silently dropped.
            valid_count = split.valid.count()
            reconcile_counts(
                split.total,
                valid_count + bad_count,
                label=f"{job_id} raw vs written+quarantined",
            )
            landing.record_counts(
                rows_read=split.total,
                rows_written=valid_count,
                rows_quarantined=bad_count,
            )

            # Contract check: a large quarantine share means the source
            # changed shape. Raising here aborts the landing.
            check_quarantine_ratio(
                split.total, bad_count, max_ratio=MAX_QUARANTINE_RATIO
            )

            # Volume check against the same slice on previous days.
            volume = check_volume(
                valid_count,
                baseline_row_counts(
                    storage, args.page_type, args.date, args.hour
                ),
            )
            log_metrics(
                log, job=job_id,
                rows_read=split.total,
                rows_written=valid_count,
                rows_quarantined=bad_count,
                **volume.as_fields(),
            )
            if volume.status == "anomaly":
                log_event(
                    log, "volume_anomaly", job=job_id, reason=volume.reason,
                    mode=VOLUME_CHECK_MODE,
                )
                if VOLUME_CHECK_MODE == "fail":
                    raise ValueError(
                        f"{job_id}: volume anomaly — {volume.reason}"
                    )

            # Step 6: Write partitioned table. Dynamic partition overwrite
            # makes re-running this (page_type, date, hour) idempotent
            # without deleting other partitions.
            storage.write_partitioned(
                split.valid,
                table_path,
                partition_cols=["page_type", "date", "hour"],
            )

        # Landing committed: raw file at its final path, manifest published.
        log_event(
            log, "raw_committed", job=job_id, path=raw_path,
            manifest=manifest_path_for(raw_path),
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
