"""Tests for api_pull job."""

import csv
import gzip
import io
import os

import pytest

from spark_applications.api_pull import fetch_impression_data, parse_args
from spark_applications.utils.schema import IMPRESSION_SCHEMA
from spark_applications.utils.storage import LocalStorageAdapter


def test_parse_args():
    args = parse_args([
        "--mode", "local",
        "--page_type", "1",
        "--date", "2026-01-01",
        "--hour", "10",
    ])
    assert args.mode == "local"
    assert args.page_type == "1"
    assert args.date == "2026-01-01"
    assert args.hour == "10"


def test_fetch_impression_data(mocker):
    mock_response = mocker.MagicMock()
    mock_response.content = b"fake_gzip_content"
    mock_response.raise_for_status = mocker.MagicMock()

    mocker.patch(
        "spark_applications.api_pull.requests.get",
        return_value=mock_response,
    )

    result = fetch_impression_data(
        "http://localhost:8000", "1", "2026-01-01", "10"
    )

    assert result == b"fake_gzip_content"


def test_fetch_retries_then_succeeds(mocker):
    """Transient failures are retried; a later success returns normally."""
    import requests

    good = mocker.MagicMock()
    good.content = b"ok"
    good.raise_for_status = mocker.MagicMock()

    get = mocker.patch(
        "spark_applications.api_pull.requests.get",
        side_effect=[
            requests.exceptions.ConnectionError("boom"),
            requests.exceptions.ConnectionError("boom"),
            good,
        ],
    )

    result = fetch_impression_data(
        "http://localhost:8000", "1", "2026-01-01", "10",
        backoff_base=0.0, sleep=lambda _: None,
    )

    assert result == b"ok"
    assert get.call_count == 3


def test_fetch_raises_after_max_attempts(mocker):
    """Exhausting retries raises rather than failing silently."""
    import requests

    mocker.patch(
        "spark_applications.api_pull.requests.get",
        side_effect=requests.exceptions.ConnectionError("down"),
    )

    with pytest.raises(RuntimeError, match="failed after"):
        fetch_impression_data(
            "http://localhost:8000", "1", "2026-01-01", "10",
            max_attempts=3, backoff_base=0.0, sleep=lambda _: None,
        )


def _make_gzip_csv() -> bytes:
    """Build a gzip-compressed impression CSV with 10 rows."""
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow([
        "user_id", "impression_id", "page_type",
        "date", "hour", "min", "second", "event_type",
    ])
    for i in range(10):
        writer.writerow([
            f"user_{i}", f"imp_{i}", "1",
            "2026-01-01", "10", "30", str(i), "a",
        ])
    return gzip.compress(csv_buffer.getvalue().encode("utf-8"))


def test_read_gzip_csv_directly_with_schema(spark, tmp_path):
    """Spark reads the .gz directly (no driver decompress) and the explicit
    schema is enforced rather than inferred."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))

    raw_gz_path = "test/data.csv.gz"
    adapter.save_raw_file(_make_gzip_csv(), raw_gz_path)

    # Read the gzip file directly with the explicit schema.
    df = adapter.read_csv(spark, raw_gz_path, schema=IMPRESSION_SCHEMA)

    assert df.count() == 10
    # Names and types match the contract we supplied, not an inferred guess.
    # (The CSV reader always relaxes nullability to True, so compare on
    # name/type rather than the full struct.)
    expected = [(f.name, f.dataType) for f in IMPRESSION_SCHEMA.fields]
    actual = [(f.name, f.dataType) for f in df.schema.fields]
    assert actual == expected
    # Numeric columns are typed, not strings.
    assert dict(df.dtypes)["hour"] == "int"
    assert dict(df.dtypes)["second"] == "int"


def test_local_storage_status(tmp_path):
    """Test status update via LocalStorageAdapter."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    adapter.update_status(None, "test_job", "in_progress")

    import json
    status_file = os.path.join(str(tmp_path), "status", "test_job.json")
    with open(status_file) as f:
        status = json.load(f)

    assert status["job_id"] == "test_job"
    assert status["status"] == "in_progress"


# --- transactional landing + volume checks wired into the job (#3) ----------

import json  # noqa: E402

from spark_applications import api_pull  # noqa: E402
from spark_applications.utils.landing import (  # noqa: E402
    manifest_path_for,
    raw_path_for,
    staging_path_for,
)


def _run_main(spark, mocker, tmp_path, monkeypatch, content: bytes,
              hour: str = "10"):
    """Run api_pull.main against tmp_path with the API mocked out."""
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(api_pull, "get_spark_session", lambda *a, **k: spark)
    # main() stops the session in its finally; keep the shared fixture alive.
    mocker.patch.object(spark, "stop")
    response = mocker.MagicMock()
    response.content = content
    response.raise_for_status = mocker.MagicMock()
    mocker.patch("spark_applications.api_pull.requests.get",
                 return_value=response)
    api_pull.main([
        "--mode", "local", "--page_type", "1",
        "--date", "2026-01-01", "--hour", hour,
    ])


def _status(tmp_path, job_id):
    with open(os.path.join(str(tmp_path), "status", f"{job_id}.json")) as f:
        return json.load(f)["status"]


def test_main_lands_raw_table_and_manifest_together(
    spark, mocker, tmp_path, monkeypatch
):
    _run_main(spark, mocker, tmp_path, monkeypatch, _make_gzip_csv())

    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    job_id = "impression_1_2026-01-01_10"
    final = raw_path_for("1", "2026-01-01", "10")

    assert adapter.raw_file_exists(final)
    assert not adapter.raw_file_exists(staging_path_for(job_id))
    manifest = adapter.read_raw_manifest(manifest_path_for(final))
    assert manifest["rows_read"] == 10
    assert manifest["rows_written"] == 10
    assert manifest["rows_quarantined"] == 0
    assert adapter.read_partitioned(spark, "impressions").count() == 10
    assert _status(tmp_path, job_id) == "completed"


def test_main_failure_after_staging_leaves_no_orphan(
    spark, mocker, tmp_path, monkeypatch
):
    """If the table write fails the staged raw file is removed, nothing is
    promoted, no manifest is published, and the status says failed."""
    mocker.patch.object(
        LocalStorageAdapter, "write_partitioned",
        side_effect=RuntimeError("disk full"),
    )

    with pytest.raises(RuntimeError, match="disk full"):
        _run_main(spark, mocker, tmp_path, monkeypatch, _make_gzip_csv())

    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    job_id = "impression_1_2026-01-01_10"
    final = raw_path_for("1", "2026-01-01", "10")
    assert not adapter.raw_file_exists(final)
    assert not adapter.raw_file_exists(staging_path_for(job_id))
    assert adapter.read_raw_manifest(manifest_path_for(final)) is None
    assert _status(tmp_path, job_id) == "failed"


def test_main_aborts_when_quarantine_ratio_is_breached(
    spark, mocker, tmp_path, monkeypatch
):
    """Half the rows failing the contract means the source changed shape:
    abort rather than land half an hour."""
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow([
        "user_id", "impression_id", "page_type",
        "date", "hour", "min", "second", "event_type",
    ])
    for i in range(10):
        hour = "10" if i % 2 else "NOPE"
        writer.writerow([f"u{i}", f"i{i}", "1", "2026-01-01", hour, "0",
                         str(i), "a"])
    content = gzip.compress(csv_buffer.getvalue().encode("utf-8"))

    with pytest.raises(ValueError, match="quarantine ratio"):
        _run_main(spark, mocker, tmp_path, monkeypatch, content)

    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    final = raw_path_for("1", "2026-01-01", "10")
    assert not adapter.raw_file_exists(final)
    assert adapter.read_raw_manifest(manifest_path_for(final)) is None
    # The bad rows were still preserved for inspection.
    assert os.path.isdir(os.path.join(
        str(tmp_path), "quarantine", "impressions",
        "impression_1_2026-01-01_10",
    ))


def test_baseline_row_counts_reads_prior_days_same_slice(tmp_path):
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    for day, rows in [("2025-12-31", 900), ("2025-12-30", 1100),
                      ("2025-12-24", 5)]:   # 24th is outside a 7-day window
        adapter.write_raw_manifest(
            {"rows_written": rows},
            manifest_path_for(raw_path_for("1", day, "10")),
        )
    # A different hour must not count as baseline for hour 10.
    adapter.write_raw_manifest(
        {"rows_written": 42},
        manifest_path_for(raw_path_for("1", "2025-12-31", "11")),
    )

    counts = api_pull.baseline_row_counts(
        adapter, "1", "2026-01-01", "10", days=7
    )
    assert sorted(counts) == [900, 1100]
    assert api_pull.baseline_row_counts(adapter, "2", "2026-01-01", "10") == []


def test_main_volume_anomaly_is_a_warning_by_default(
    spark, mocker, tmp_path, monkeypatch
):
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    # History says this slice normally has 1000 rows; today has 10.
    adapter.write_raw_manifest(
        {"rows_written": 1000},
        manifest_path_for(raw_path_for("1", "2025-12-31", "10")),
    )
    events = mocker.spy(api_pull, "log_event")

    _run_main(spark, mocker, tmp_path, monkeypatch, _make_gzip_csv())

    final = raw_path_for("1", "2026-01-01", "10")
    assert adapter.raw_file_exists(final)          # still landed
    anomalies = [
        c for c in events.call_args_list if c.args[1] == "volume_anomaly"
    ]
    assert len(anomalies) == 1
    assert anomalies[0].kwargs["mode"] == "warn"
    assert "below the 50% floor" in anomalies[0].kwargs["reason"]


def test_main_volume_anomaly_fails_the_run_when_configured(
    spark, mocker, tmp_path, monkeypatch
):
    monkeypatch.setattr(api_pull, "VOLUME_CHECK_MODE", "fail")
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    adapter.write_raw_manifest(
        {"rows_written": 1000},
        manifest_path_for(raw_path_for("1", "2025-12-31", "10")),
    )

    with pytest.raises(ValueError, match="volume anomaly"):
        _run_main(spark, mocker, tmp_path, monkeypatch, _make_gzip_csv())

    final = raw_path_for("1", "2026-01-01", "10")
    assert not adapter.raw_file_exists(final)
    assert _status(tmp_path, "impression_1_2026-01-01_10") == "failed"
