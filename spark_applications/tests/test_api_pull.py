"""Tests for api_pull job."""

import csv
import gzip
import io
import os

from spark_applications.api_pull import fetch_impression_data, parse_args
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


def test_local_storage_round_trip(spark, tmp_path):
    """Test saving a gzip CSV and reading it back via LocalStorageAdapter."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))

    # Create a sample CSV and gzip it
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

    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    gz_content = gzip.compress(csv_bytes)

    # Save raw gzip file
    raw_gz_path = "test/data.csv.gz"
    adapter.save_raw_file(gz_content, raw_gz_path)

    # Decompress and save CSV
    raw_csv_path = "test/data.csv"
    adapter.save_raw_file(csv_bytes, raw_csv_path)

    # Read the CSV back
    df = adapter.read_csv(spark, raw_csv_path)

    assert df.count() == 10
    assert "user_id" in df.columns
    assert "event_type" in df.columns


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
