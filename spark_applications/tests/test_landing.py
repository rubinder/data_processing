"""Tests for the transactional raw-file landing (Tasks.md #3).

The raw gzip and the parquet table are two writes to two places; neither a
filesystem nor S3 gives us a transaction across them. ``landed_raw_file``
gets the same guarantee a different way: the raw file is written to a
staging path, the table write happens while it is staged, and only then is
the file promoted to its final path and a manifest published. A failure at
any point deletes the staged file, so a half-finished run can never leave a
raw file that no table partition accounts for.
"""

import hashlib
import json
import os

import pytest

from spark_applications.utils.landing import (
    RawLanding,
    landed_raw_file,
    manifest_path_for,
    raw_path_for,
    staging_path_for,
)
from spark_applications.utils.storage import LocalStorageAdapter

JOB = "impression_1_2026-01-01_10"
CONTENT = b"user_id,impression_id\nu1,i1\n"


def _paths():
    final = raw_path_for("1", "2026-01-01", "10")
    return final, staging_path_for(JOB), manifest_path_for(final)


def test_paths_are_deterministic_and_partitioned():
    final, staging, manifest = _paths()
    assert final == (
        "impressions/page_type=1/date=2026-01-01/hour=10/data.csv.gz"
    )
    assert staging == f"impressions/_staging/{JOB}/data.csv.gz"
    assert manifest == (
        "impressions/page_type=1/date=2026-01-01/hour=10/_manifest.json"
    )


def test_commit_promotes_raw_and_publishes_manifest(tmp_path):
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, staging, manifest = _paths()

    with landed_raw_file(storage, CONTENT, JOB, final) as landing:
        assert isinstance(landing, RawLanding)
        # While the block runs the bytes are readable at the staging path
        # and nothing has been published yet.
        assert storage.raw_file_exists(staging)
        assert not storage.raw_file_exists(final)
        assert storage.read_raw_manifest(manifest) is None
        landing.record_counts(rows_read=1, rows_written=1, rows_quarantined=0)

    assert storage.raw_file_exists(final)
    assert not storage.raw_file_exists(staging)
    # The staging directory for the job is gone, not just the file.
    assert not os.path.exists(
        os.path.join(str(tmp_path), "raw", "impressions", "_staging", JOB)
    )

    published = storage.read_raw_manifest(manifest)
    assert published["job_id"] == JOB
    assert published["page_type"] == "1"
    assert published["date"] == "2026-01-01"
    assert published["hour"] == "10"
    assert published["rows_read"] == 1
    assert published["rows_written"] == 1
    assert published["rows_quarantined"] == 0
    assert published["raw_bytes"] == len(CONTENT)
    assert published["raw_sha256"] == hashlib.sha256(CONTENT).hexdigest()
    assert published["landed_at"] <= published["committed_at"]
    # The manifest is the *last* thing written, so its presence implies the
    # raw file and the table write both completed.
    with open(os.path.join(str(tmp_path), "raw", manifest)) as f:
        assert json.load(f) == published


def test_failure_inside_the_block_removes_the_staged_file(tmp_path):
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, staging, manifest = _paths()

    with pytest.raises(RuntimeError, match="table write blew up"):
        with landed_raw_file(storage, CONTENT, JOB, final):
            assert storage.raw_file_exists(staging)
            raise RuntimeError("table write blew up")

    # No orphan: nothing at staging, nothing at final, no manifest.
    assert not storage.raw_file_exists(staging)
    assert not storage.raw_file_exists(final)
    assert storage.read_raw_manifest(manifest) is None


def test_failure_does_not_clobber_a_previous_successful_landing(tmp_path):
    """A re-run that fails must leave the last good raw file and manifest
    in place; only the staged copy is discarded."""
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, staging, manifest = _paths()

    with landed_raw_file(storage, CONTENT, JOB, final) as landing:
        landing.record_counts(rows_read=1, rows_written=1, rows_quarantined=0)
    good = storage.read_raw_manifest(manifest)

    with pytest.raises(RuntimeError):
        with landed_raw_file(storage, b"newer", JOB, final):
            raise RuntimeError("boom")

    assert storage.raw_file_exists(final)
    assert storage.read_raw_manifest(manifest) == good


def test_stale_staging_from_a_crashed_run_is_discarded_on_entry(tmp_path):
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, staging, _ = _paths()
    storage.save_raw_file(b"left over from a crash", staging)

    with landed_raw_file(storage, CONTENT, JOB, final) as landing:
        # The staged bytes are this run's, not the crashed run's.
        staged = os.path.join(str(tmp_path), "raw", staging)
        with open(staged, "rb") as f:
            assert f.read() == CONTENT
        landing.record_counts(rows_read=1, rows_written=1, rows_quarantined=0)

    assert storage.raw_file_exists(final)


def test_rerun_replaces_the_raw_file_and_manifest(tmp_path):
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, _, manifest = _paths()

    with landed_raw_file(storage, CONTENT, JOB, final) as landing:
        landing.record_counts(rows_read=1, rows_written=1, rows_quarantined=0)
    with landed_raw_file(storage, b"v2", JOB, final) as landing:
        landing.record_counts(rows_read=5, rows_written=4, rows_quarantined=1)

    with open(os.path.join(str(tmp_path), "raw", final), "rb") as f:
        assert f.read() == b"v2"
    assert storage.read_raw_manifest(manifest)["rows_written"] == 4


def test_landing_read_path_is_the_staging_path(tmp_path):
    storage = LocalStorageAdapter(base_dir=str(tmp_path))
    final, staging, _ = _paths()
    with landed_raw_file(storage, CONTENT, JOB, final) as landing:
        assert landing.read_path == staging
