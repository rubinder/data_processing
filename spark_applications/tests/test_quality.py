"""Tests for data-quality helpers (issue #1 / #3 hardening)."""

import csv
import gzip
import io

import pytest

from spark_applications.utils.quality import (
    deduplicate,
    reconcile_counts,
    split_on_contract,
)
from spark_applications.utils.schema import (
    CORRUPT_RECORD_COL,
    IMPRESSION_SCHEMA,
    schema_with_corrupt_column,
)
from spark_applications.utils.storage import LocalStorageAdapter

REQUIRED = [f.name for f in IMPRESSION_SCHEMA.fields]


def _gzip_csv(rows: list[list]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "user_id", "impression_id", "page_type",
        "date", "hour", "min", "second", "event_type",
    ])
    for row in rows:
        writer.writerow(row)
    return gzip.compress(buf.getvalue().encode("utf-8"))


def test_split_quarantines_malformed_rows(spark, tmp_path):
    """A row whose 'hour' is not an int is quarantined, not written."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    rows = [
        ["u1", "i1", "1", "2026-01-01", "10", "0", "1", "a"],   # valid
        ["u2", "i2", "1", "2026-01-01", "NOPE", "0", "1", "a"],  # bad hour
        ["u3", "i3", "1", "2026-01-01", "11", "0", "2", "b"],   # valid
    ]
    adapter.save_raw_file(_gzip_csv(rows), "test/data.csv.gz")

    df = adapter.read_csv(
        spark,
        "test/data.csv.gz",
        schema=schema_with_corrupt_column(IMPRESSION_SCHEMA),
        corrupt_column=CORRUPT_RECORD_COL,
    )
    split = split_on_contract(df, required_cols=REQUIRED)

    valid_ids = {r.user_id for r in split.valid.collect()}
    bad_ids = {r.user_id for r in split.quarantined.collect()}

    assert valid_ids == {"u1", "u3"}
    assert bad_ids == {"u2"}
    assert split.total == 3
    # The corrupt-record column is dropped from the clean output.
    assert CORRUPT_RECORD_COL not in split.valid.columns


def test_deduplicate_keeps_latest_per_key(spark):
    """At-least-once redelivery: collapse to the latest row per key."""
    cols = ["impression_id", "second", "event_type"]
    data = [
        ("i1", 1, "a"),
        ("i1", 3, "c"),   # latest for i1
        ("i1", 2, "b"),
        ("i2", 5, "a"),
    ]
    df = spark.createDataFrame(data, cols)

    result = {
        r.impression_id: r
        for r in deduplicate(df, keys=["impression_id"], order_by="second")
        .collect()
    }

    assert len(result) == 2
    assert result["i1"].second == 3
    assert result["i1"].event_type == "c"
    assert result["i2"].second == 5


def test_reconcile_counts_passes_on_match():
    reconcile_counts(100, 100, label="raw->written")  # no raise


def test_reconcile_counts_within_tolerance():
    reconcile_counts(100, 99, label="raw->written", tolerance=0.05)


def test_reconcile_counts_raises_on_drift():
    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile_counts(100, 80, label="raw->written")


def test_reconcile_counts_zero_expected():
    reconcile_counts(0, 0, label="empty")
    with pytest.raises(ValueError):
        reconcile_counts(0, 5, label="empty")


# --- volume / anomaly checks (#3 follow-up) ---------------------------------

from spark_applications.utils.quality import (  # noqa: E402
    VolumeCheck,
    check_quarantine_ratio,
    check_volume,
)


def test_check_volume_ok_within_band():
    result = check_volume(current=1_000, baselines=[900, 1_100, 1_000])
    assert isinstance(result, VolumeCheck)
    assert result.status == "ok"
    assert result.baseline == 1_000       # median of the baselines
    assert result.ratio == 1.0
    assert result.reason is None


def test_check_volume_flags_a_collapse_and_a_spike():
    low = check_volume(current=100, baselines=[1_000, 1_000, 1_000])
    assert low.status == "anomaly"
    assert low.ratio == 0.1
    assert "below" in low.reason

    high = check_volume(current=5_000, baselines=[1_000])
    assert high.status == "anomaly"
    assert high.ratio == 5.0
    assert "above" in high.reason


def test_check_volume_band_is_configurable():
    result = check_volume(
        current=400, baselines=[1_000], min_ratio=0.3, max_ratio=3.0
    )
    assert result.status == "ok"


def test_check_volume_without_baselines_cannot_judge():
    result = check_volume(current=1_000, baselines=[])
    assert result.status == "no_baseline"
    assert result.baseline is None
    assert result.ratio is None


def test_check_volume_zero_rows_is_always_an_anomaly():
    # An empty pull is never "within the band" — a source that returned
    # nothing needs a human even if yesterday was also small.
    result = check_volume(current=0, baselines=[3])
    assert result.status == "anomaly"
    assert check_volume(current=0, baselines=[]).status == "anomaly"


def test_check_volume_median_ignores_one_bad_baseline_day():
    # A single outlier day (an earlier outage) must not drag the baseline.
    result = check_volume(current=1_000, baselines=[0, 1_000, 1_050, 980])
    assert result.status == "ok"


def test_check_quarantine_ratio():
    assert check_quarantine_ratio(rows_read=1_000, rows_quarantined=5) is None
    with pytest.raises(ValueError, match="quarantine ratio"):
        check_quarantine_ratio(rows_read=1_000, rows_quarantined=50)
    # Threshold is configurable; zero rows read is not a division error.
    assert check_quarantine_ratio(0, 0) is None
    check_quarantine_ratio(rows_read=100, rows_quarantined=20, max_ratio=0.25)
