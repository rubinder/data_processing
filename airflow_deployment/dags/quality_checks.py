"""Pure quality-check helpers for the impression pipeline.

No Airflow import: everything here takes plain paths/datetimes and returns
plain values or raises ``QualityCheckError``, so it unit-tests with
``tmp_path`` and no scheduler. ``impression_quality_checks.py`` is the thin
DAG wrapper.

Inputs (written by ``spark_applications/api_pull.py`` in local mode):

* status files   ``<data_dir>/status/<job_id>.json``
                 ``{"job_id", "status", "updated_at"}``
                 ``job_id = impression_<page_type>_<date>_<hour>``
* manifests      ``<data_dir>/raw/impressions/page_type=<pt>/date=<YYYY-MM-DD>/
                 hour=<HH>/_manifest.json``
                 keys: job_id, page_type, date, hour, rows_read, rows_written,
                 rows_quarantined, raw_bytes, raw_sha256, landed_at,
                 committed_at (ISO-8601 UTC)

Hours are passed around as the zero-padded string the pipeline DAG uses for
``--hour`` (``logical_date.strftime('%H')``) because the Spark job builds the
raw path and job_id from that string verbatim.
"""

from __future__ import annotations

import json
import os
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable


class QualityCheckError(Exception):
    """A quality check failed. Airflow marks the task failed."""


class FreshnessError(QualityCheckError):
    """No recent completed run."""


class VolumeError(QualityCheckError):
    """Row volume or quarantine ratio out of bounds."""


# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

def status_dir(data_dir: str) -> str:
    return os.path.join(data_dir, "status")


def manifest_path(
    data_dir: str, page_type: str, date: str, hour: str
) -> str:
    """Path of the manifest api_pull writes beside data.csv.gz."""
    return os.path.join(
        data_dir, "raw", "impressions",
        f"page_type={page_type}", f"date={date}", f"hour={hour}",
        "_manifest.json",
    )


def baseline_manifest_paths(
    data_dir: str, page_type: str, date: str, hour: str, days: int = 7
) -> list[str]:
    """Manifests for the same hour on each of the previous ``days`` days.

    Same-hour-previous-days is the baseline because impression volume has a
    strong diurnal cycle; comparing 03:00 to 15:00 would always breach.
    """
    day = datetime.strptime(date, "%Y-%m-%d").date()
    return [
        manifest_path(
            data_dir, page_type,
            (day - timedelta(days=n)).isoformat(), hour,
        )
        for n in range(1, days + 1)
    ]


# --------------------------------------------------------------------------
# Timestamps / partition resolution
# --------------------------------------------------------------------------

def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are treated as UTC."""
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def resolve_partition(
    logical_dates: Iterable[datetime], now: datetime
) -> tuple[str, str]:
    """Pick the (date, hour) to check.

    ``logical_dates`` are the logical dates of the producer runs that emitted
    the triggering dataset events (the pipeline derives ``--date``/``--hour``
    from its logical date, so this is exactly the partition it wrote). The
    newest wins if several events piled up. With no events (manual trigger)
    fall back to the current UTC hour.
    """
    dates = [d for d in logical_dates if d is not None]
    target = max(dates) if dates else now
    target = target.astimezone(timezone.utc)
    return target.strftime("%Y-%m-%d"), target.strftime("%H")


# --------------------------------------------------------------------------
# Freshness
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class StatusRecord:
    job_id: str
    status: str
    updated_at: datetime


def load_status_records(status_dir_path: str) -> list[StatusRecord]:
    """Read every ``*.json`` status file; unreadable files are skipped.

    A half-written file (the Spark job may be mid-write) must not fail the
    check on its own; the SLA comparison on the remaining files still does.
    """
    if not os.path.isdir(status_dir_path):
        return []
    records: list[StatusRecord] = []
    for name in sorted(os.listdir(status_dir_path)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(status_dir_path, name)
        try:
            with open(path) as f:
                raw = json.load(f)
            records.append(
                StatusRecord(
                    job_id=str(raw["job_id"]),
                    status=str(raw["status"]),
                    updated_at=parse_timestamp(raw["updated_at"]),
                )
            )
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return records


def check_freshness(
    status_dir_path: str,
    sla_minutes: int,
    now: datetime | None = None,
) -> timedelta:
    """Return the age of the newest ``completed`` run; raise if over the SLA.

    "Newest" is by ``updated_at``, not by the partition encoded in the
    job_id, so a late backfill of an old hour does not make the pipeline look
    fresh: its updated_at is recent, and that is correct, the pipeline *did*
    just complete something. Whether the *current* hour landed is the volume
    check's job.
    """
    return freshness_from_records(
        load_status_records(status_dir_path), sla_minutes, now=now,
        source=f"status files under {status_dir_path}",
    )


def freshness_from_records(
    records: Iterable[StatusRecord],
    sla_minutes: int,
    now: datetime | None = None,
    source: str = "status records",
) -> timedelta:
    """Storage-agnostic core of :func:`check_freshness`.

    Takes already-loaded records so the same rule runs over local status
    files (``LocalQualitySource``) and the DynamoDB status table
    (``AwsQualitySource``).
    """
    now = now or datetime.now(timezone.utc)
    completed = [r for r in records if r.status == "completed"]
    if not completed:
        raise FreshnessError(f"no completed runs in {source}")
    newest = max(completed, key=lambda r: r.updated_at)
    age = now - newest.updated_at
    sla = timedelta(minutes=sla_minutes)
    if age > sla:
        raise FreshnessError(
            f"newest completed run {newest.job_id} is {age} old "
            f"(updated_at={newest.updated_at.isoformat()}), "
            f"SLA is {sla_minutes} min"
        )
    return age


# --------------------------------------------------------------------------
# Volume
# --------------------------------------------------------------------------

@dataclass
class VolumeResult:
    job_id: str
    rows_read: int
    rows_written: int
    rows_quarantined: int
    quarantine_ratio: float
    baseline_count: int
    baseline_median: float | None
    ratio: float | None
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
            "quarantine_ratio": self.quarantine_ratio,
            "baseline_count": self.baseline_count,
            "baseline_median": self.baseline_median,
            "ratio": self.ratio,
            "warnings": list(self.warnings),
        }


def load_manifest(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _int_field(manifest: dict, key: str, path: str) -> int:
    try:
        return int(manifest[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise VolumeError(f"manifest {path} has no integer {key!r}") from exc


def check_volume(
    manifest_path_: str,
    baseline_manifest_paths_: Iterable[str],
    min_ratio: float = 0.5,
    max_ratio: float = 2.0,
    max_quarantine_ratio: float = 0.01,
) -> VolumeResult:
    """Compare this hour's rows_written to the median of the baselines.

    Local-filesystem entry point; the rule itself is
    :func:`volume_from_manifests`, shared with the AWS source.

    * Current manifest missing -> ``VolumeError`` (the hour did not land).
    * Baseline manifest missing -> a warning, not a failure (first days of a
      new page_type, or a gap that was never backfilled). With no baselines at
      all the ratio check is skipped, with a warning.
    * ``rows_written / median(baselines)`` outside ``[min_ratio, max_ratio]``
      -> ``VolumeError``.
    * ``rows_quarantined / rows_read`` above ``max_quarantine_ratio`` ->
      ``VolumeError``. This is checked first: a quarantine spike is the
      more specific diagnosis (see RUNBOOK.md, quarantine section).
    """
    current = load_manifest(manifest_path_) if os.path.isfile(
        manifest_path_
    ) else None
    baselines = [
        (path, load_manifest(path) if os.path.isfile(path) else None)
        for path in baseline_manifest_paths_
    ]
    return volume_from_manifests(
        current, manifest_path_, baselines,
        min_ratio=min_ratio, max_ratio=max_ratio,
        max_quarantine_ratio=max_quarantine_ratio,
    )


def volume_from_manifests(
    current: dict | None,
    current_label: str,
    baselines: Iterable[tuple[str, dict | None]],
    min_ratio: float = 0.5,
    max_ratio: float = 2.0,
    max_quarantine_ratio: float = 0.01,
) -> VolumeResult:
    """Storage-agnostic core of :func:`check_volume`.

    ``current`` is the manifest dict for the checked hour (``None`` if it was
    not found); ``baselines`` pairs each baseline's label with its manifest
    dict or ``None`` when missing.
    """
    if current is None:
        raise VolumeError(f"current manifest missing: {current_label}")
    rows_read = _int_field(current, "rows_read", current_label)
    rows_written = _int_field(current, "rows_written", current_label)
    rows_quarantined = _int_field(current, "rows_quarantined", current_label)
    job_id = str(current.get("job_id", current_label))

    warnings: list[str] = []

    quarantine_ratio = (
        rows_quarantined / rows_read if rows_read > 0 else 0.0
    )
    if quarantine_ratio > max_quarantine_ratio:
        raise VolumeError(
            f"{job_id}: quarantine ratio {quarantine_ratio:.4f} "
            f"({rows_quarantined}/{rows_read}) exceeds "
            f"{max_quarantine_ratio}"
        )
    if rows_read == 0:
        warnings.append(f"{job_id}: rows_read is 0")

    baseline_counts: list[int] = []
    for label, manifest in baselines:
        if manifest is None:
            warnings.append(f"missing baseline manifest: {label}")
            continue
        baseline_counts.append(_int_field(manifest, "rows_written", label))
    baselines = baseline_counts

    median: float | None = None
    ratio: float | None = None
    if not baselines:
        warnings.append(
            f"{job_id}: no baseline manifests available; "
            "volume ratio not checked"
        )
    else:
        median = float(statistics.median(baselines))
        if median == 0:
            warnings.append(
                f"{job_id}: baseline median is 0; volume ratio undefined"
            )
        else:
            ratio = rows_written / median
            if ratio < min_ratio or ratio > max_ratio:
                raise VolumeError(
                    f"{job_id}: rows_written={rows_written} is "
                    f"{ratio:.2f}x the baseline median {median:.0f} "
                    f"(n={len(baselines)}); allowed "
                    f"[{min_ratio}, {max_ratio}]"
                )

    return VolumeResult(
        job_id=job_id,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
        quarantine_ratio=quarantine_ratio,
        baseline_count=len(baselines),
        baseline_median=median,
        ratio=ratio,
        warnings=warnings,
    )


# --------------------------------------------------------------------------
# Sources: where status records and manifests live per SPARK_MODE
# --------------------------------------------------------------------------

RAW_MANIFEST_KEY = "raw/impressions/page_type={pt}/date={d}/hour={h}/_manifest.json"


def _previous_days(date: str, days: int) -> list[str]:
    day = datetime.strptime(date, "%Y-%m-%d").date()
    return [(day - timedelta(days=n)).isoformat() for n in range(1, days + 1)]


class LocalQualitySource:
    """Status files + manifests under ``PIPELINE_DATA_DIR`` (SPARK_MODE=local).

    Layout written by ``spark_applications`` ``LocalStorageAdapter``.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def describe(self) -> str:
        return f"local data dir {self.data_dir}"

    def status_records(self) -> list[StatusRecord]:
        return load_status_records(status_dir(self.data_dir))

    def manifest(self, page_type: str, date: str, hour: str):
        path = manifest_path(self.data_dir, page_type, date, hour)
        return path, (load_manifest(path) if os.path.isfile(path) else None)

    def baseline_manifests(
        self, page_type: str, date: str, hour: str, days: int
    ) -> list[tuple[str, dict | None]]:
        return [
            self.manifest(page_type, previous, hour)
            for previous in _previous_days(date, days)
        ]


class AwsQualitySource:
    """DynamoDB status table + S3 manifests (SPARK_MODE=aws).

    Mirrors ``spark_applications`` ``AwsStorageAdapter``: status items are
    ``{job_id, status, updated_at}`` in the DynamoDB table and manifests sit
    at ``raw/impressions/page_type=..%/date=../hour=../_manifest.json`` in the
    landing bucket. Clients are injectable so the class tests with
    ``botocore.stub.Stubber`` and no network.
    """

    def __init__(self, bucket: str, table_name: str, s3=None, dynamodb=None):
        import boto3

        self.bucket = bucket
        self.table_name = table_name
        self.s3 = s3 or boto3.client("s3")
        self.dynamodb = dynamodb or boto3.client("dynamodb")

    def describe(self) -> str:
        return f"DynamoDB {self.table_name} / s3://{self.bucket}"

    def status_records(self) -> list[StatusRecord]:
        # The status table is one item per job_id (tens of thousands at most
        # after years of hourly runs); a paginated scan is the simplest
        # correct read and runs once per check.
        records: list[StatusRecord] = []
        paginator = self.dynamodb.get_paginator("scan")
        for page in paginator.paginate(TableName=self.table_name):
            for item in page.get("Items", []):
                try:
                    records.append(
                        StatusRecord(
                            job_id=item["job_id"]["S"],
                            status=item["status"]["S"],
                            updated_at=parse_timestamp(item["updated_at"]["S"]),
                        )
                    )
                except (KeyError, ValueError, TypeError):
                    continue
        return records

    def manifest(self, page_type: str, date: str, hour: str):
        key = RAW_MANIFEST_KEY.format(pt=page_type, d=date, h=hour)
        label = f"s3://{self.bucket}/{key}"
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"]
        except self.s3.exceptions.NoSuchKey:
            return label, None
        return label, json.loads(body.read())

    def baseline_manifests(
        self, page_type: str, date: str, hour: str, days: int
    ) -> list[tuple[str, dict | None]]:
        return [
            self.manifest(page_type, previous, hour)
            for previous in _previous_days(date, days)
        ]


def quality_source(
    spark_mode: str,
    data_dir: str,
    bucket: str | None = None,
    table_name: str | None = None,
):
    """Pick the source for the pipeline's storage mode."""
    if spark_mode == "aws":
        if not bucket or not table_name:
            raise QualityCheckError(
                "SPARK_MODE=aws needs S3_LANDING_BUCKET (or S3_BUCKET) and "
                "DYNAMODB_TABLE"
            )
        return AwsQualitySource(bucket, table_name)
    return LocalQualitySource(data_dir)
