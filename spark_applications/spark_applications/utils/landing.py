"""Transactional landing of a raw file alongside its table write.

The API pull produces two artefacts that must agree: the raw gzip in the
landing zone and the parquet/Delta partition derived from it. Neither a POSIX
filesystem nor S3 offers a transaction spanning both, so a crash between the
two writes used to leave an *orphan* — a raw file no table partition
accounts for, or (worse for consumers of the raw zone) a raw file that looks
final but whose contents never reached the table.

:func:`landed_raw_file` gets the same guarantee with a staging area and a
commit marker:

1. **Stage.** The bytes are written to ``<table>/_staging/<job_id>/`` — a
   path no consumer reads. Any stale staging left by a crashed run of the
   same job is discarded first.
2. **Work.** The caller reads the staged file and writes the table while it
   is staged (the ``with`` block).
3. **Commit.** Only if the block completes: the staged file is moved to its
   final partition path (an atomic rename locally, a single-object PUT on
   S3) and a ``_manifest.json`` is published *last*, carrying row counts and
   the sha256 of the bytes.
4. **Abort.** If the block raises, the staged file is deleted. The previous
   successful landing (if any) at the final path is untouched.

The invariant consumers rely on: **a manifest exists only if the raw file at
its final path and the table partition were both written from the same
bytes.** Anything under ``_staging/`` is by definition uncommitted.

Residual windows (all repaired by an idempotent re-run — the deterministic
``job_id`` and dynamic partition overwrite make re-running a (page_type,
date, hour) safe):

- Crash after the table write but before promote: table has data, raw is
  still staged. The next run discards the stale staging and redoes both.
- Crash after promote but before the manifest: raw is final, no manifest.
  Consumers must not trust it yet; the next run overwrites both.
"""

import hashlib
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

from spark_applications.utils.storage import StorageAdapter

RAW_TABLE = "impressions"
RAW_FILE_NAME = "data.csv.gz"
MANIFEST_NAME = "_manifest.json"
STAGING_DIR = "_staging"


def raw_path_for(page_type: str, date: str, hour: str) -> str:
    """Final raw path for one (page_type, date, hour)."""
    return (
        f"{RAW_TABLE}/page_type={page_type}/date={date}/hour={hour}"
        f"/{RAW_FILE_NAME}"
    )


def staging_path_for(job_id: str) -> str:
    """Staging path for a job; nothing downstream reads under _staging/."""
    return f"{RAW_TABLE}/{STAGING_DIR}/{job_id}/{RAW_FILE_NAME}"


def manifest_path_for(raw_path: str) -> str:
    """Manifest path: beside the raw file it describes."""
    directory = raw_path.rsplit("/", 1)[0]
    return f"{directory}/{MANIFEST_NAME}"


def _partition_from_path(raw_path: str) -> dict[str, str]:
    parts = {}
    for segment in raw_path.split("/"):
        if "=" in segment:
            key, value = segment.split("=", 1)
            parts[key] = value
    return parts


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RawLanding:
    """Handle the caller receives inside :func:`landed_raw_file`."""

    job_id: str
    read_path: str
    final_path: str
    raw_bytes: int
    raw_sha256: str
    landed_at: str
    counts: dict[str, int] = field(default_factory=dict)

    def record_counts(
        self, rows_read: int, rows_written: int, rows_quarantined: int
    ) -> None:
        """Record what the table write accounted for, for the manifest."""
        self.counts = {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_quarantined": rows_quarantined,
        }

    def manifest(self, committed_at: str) -> dict:
        manifest = {
            "job_id": self.job_id,
            "raw_path": self.final_path,
            "raw_bytes": self.raw_bytes,
            "raw_sha256": self.raw_sha256,
            "landed_at": self.landed_at,
            "committed_at": committed_at,
        }
        manifest.update(_partition_from_path(self.final_path))
        manifest.update(self.counts)
        return manifest


@contextmanager
def landed_raw_file(
    storage: StorageAdapter,
    content: bytes,
    job_id: str,
    final_path: str,
) -> Iterator[RawLanding]:
    """Stage ``content``, run the block, then commit (or abort) the landing.

    See the module docstring for the guarantees. The caller reads from
    ``landing.read_path`` inside the block and should call
    ``landing.record_counts(...)`` before the block ends so the manifest
    carries the reconciliation numbers.
    """
    staging_path = staging_path_for(job_id)

    # A previous run of this job that crashed mid-flight may have left its
    # staged bytes behind. They were never committed; drop them.
    storage.delete_raw_file(staging_path)

    storage.save_raw_file(content, staging_path)
    landing = RawLanding(
        job_id=job_id,
        read_path=staging_path,
        final_path=final_path,
        raw_bytes=len(content),
        raw_sha256=hashlib.sha256(content).hexdigest(),
        landed_at=_now(),
    )

    try:
        yield landing
    except BaseException:
        # Abort: no orphan. The last good landing at final_path is untouched.
        storage.delete_raw_file(staging_path)
        raise

    # Commit: promote, then publish the marker last.
    storage.move_raw_file(staging_path, final_path)
    storage.write_raw_manifest(
        landing.manifest(committed_at=_now()),
        manifest_path_for(final_path),
    )
