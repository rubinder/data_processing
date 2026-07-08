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
