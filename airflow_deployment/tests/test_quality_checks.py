"""Tests for dags/quality_checks.py (pure functions, no Airflow import)."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")
)

from quality_checks import (  # noqa: E402
    FreshnessError,
    VolumeError,
    baseline_manifest_paths,
    check_freshness,
    check_volume,
    manifest_path,
    resolve_partition,
    status_dir,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _write_status(data_dir, job_id, status, updated_at):
    path = os.path.join(status_dir(str(data_dir)), f"{job_id}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(
            {"job_id": job_id, "status": status,
             "updated_at": updated_at.isoformat()},
            f,
        )


def _write_manifest(path, rows_read, rows_written, rows_quarantined):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(
            {"job_id": "j", "rows_read": rows_read,
             "rows_written": rows_written,
             "rows_quarantined": rows_quarantined},
            f,
        )


# --- paths ------------------------------------------------------------------

def test_manifest_path_matches_api_pull_layout(tmp_path):
    assert manifest_path(str(tmp_path), "1", "2026-01-01", "10") == (
        os.path.join(
            str(tmp_path), "raw", "impressions", "page_type=1",
            "date=2026-01-01", "hour=10", "_manifest.json",
        )
    )


def test_baseline_paths_are_same_hour_previous_days(tmp_path):
    paths = baseline_manifest_paths(str(tmp_path), "2", "2026-01-03", "05",
                                    days=3)
    assert [p.split(os.sep)[-3] for p in paths] == [
        "date=2026-01-02", "date=2026-01-01", "date=2025-12-31",
    ]
    assert all(p.split(os.sep)[-2] == "hour=05" for p in paths)


# --- partition resolution ---------------------------------------------------

def test_resolve_partition_prefers_newest_triggering_run():
    older = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)
    newer = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    assert resolve_partition([older, newer, None], now=NOW) == (
        "2026-01-01", "10"
    )


def test_resolve_partition_falls_back_to_now():
    assert resolve_partition([], now=NOW) == ("2026-01-01", "12")


# --- freshness --------------------------------------------------------------

def test_freshness_ok_within_sla(tmp_path):
    _write_status(tmp_path, "impression_1_2026-01-01_11", "completed",
                  NOW - timedelta(minutes=30))
    _write_status(tmp_path, "impression_1_2026-01-01_10", "completed",
                  NOW - timedelta(hours=3))
    age = check_freshness(status_dir(str(tmp_path)), sla_minutes=90, now=NOW)
    assert age == timedelta(minutes=30)


def test_freshness_fails_when_newest_completed_is_stale(tmp_path):
    _write_status(tmp_path, "impression_1_2026-01-01_08", "completed",
                  NOW - timedelta(hours=4))
    # An in-progress run does not count as fresh data.
    _write_status(tmp_path, "impression_1_2026-01-01_11", "in_progress",
                  NOW - timedelta(minutes=1))
    with pytest.raises(FreshnessError, match="SLA is 90 min"):
        check_freshness(status_dir(str(tmp_path)), sla_minutes=90, now=NOW)


def test_freshness_fails_with_no_completed_runs(tmp_path):
    with pytest.raises(FreshnessError, match="no completed"):
        check_freshness(status_dir(str(tmp_path)), sla_minutes=90, now=NOW)


def test_freshness_skips_unreadable_status_files(tmp_path):
    _write_status(tmp_path, "good", "completed", NOW - timedelta(minutes=5))
    with open(os.path.join(status_dir(str(tmp_path)), "half.json"), "w") as f:
        f.write('{"job_id": "half", "sta')
    assert check_freshness(status_dir(str(tmp_path)), 90, now=NOW) == (
        timedelta(minutes=5)
    )


# --- volume -----------------------------------------------------------------

def _current_and_baselines(tmp_path, current_rows, baseline_rows):
    current = manifest_path(str(tmp_path), "1", "2026-01-08", "10")
    _write_manifest(current, current_rows, current_rows, 0)
    baselines = baseline_manifest_paths(str(tmp_path), "1", "2026-01-08",
                                        "10", days=len(baseline_rows))
    for path, rows in zip(baselines, baseline_rows):
        _write_manifest(path, rows, rows, 0)
    return current, baselines


def test_volume_ok_within_band(tmp_path):
    current, baselines = _current_and_baselines(
        tmp_path, 1_000, [900, 1_100, 1_000]
    )
    result = check_volume(current, baselines)
    assert result.ratio == 1.0
    assert result.baseline_median == 1_000
    assert result.baseline_count == 3
    assert result.warnings == []


def test_volume_collapse_and_spike_fail(tmp_path):
    current, baselines = _current_and_baselines(tmp_path, 100, [1_000, 1_000])
    with pytest.raises(VolumeError, match="allowed"):
        check_volume(current, baselines)

    current, baselines = _current_and_baselines(tmp_path, 9_000, [1_000])
    with pytest.raises(VolumeError):
        check_volume(current, baselines)


def test_volume_missing_baseline_is_a_warning(tmp_path):
    current, baselines = _current_and_baselines(tmp_path, 1_000, [1_000])
    missing = baseline_manifest_paths(str(tmp_path), "1", "2026-01-08", "10",
                                      days=3)
    result = check_volume(current, missing)
    assert result.baseline_count == 1
    assert sum("missing baseline" in w for w in result.warnings) == 2


def test_volume_no_baselines_skips_ratio_with_warning(tmp_path):
    current = manifest_path(str(tmp_path), "1", "2026-01-08", "10")
    _write_manifest(current, 500, 500, 0)
    result = check_volume(current, [])
    assert result.ratio is None
    assert any("no baseline" in w for w in result.warnings)


def test_volume_missing_current_manifest_fails(tmp_path):
    with pytest.raises(VolumeError, match="current manifest missing"):
        check_volume(manifest_path(str(tmp_path), "1", "2026-01-08", "10"), [])


def test_volume_quarantine_ratio_breach_fails_first(tmp_path):
    current = manifest_path(str(tmp_path), "1", "2026-01-08", "10")
    _write_manifest(current, 1_000, 900, 100)
    with pytest.raises(VolumeError, match="quarantine ratio"):
        check_volume(current, [])
    # Configurable ceiling.
    result = check_volume(current, [], max_quarantine_ratio=0.2)
    assert result.quarantine_ratio == 0.1
