"""Tests for storage adapter write semantics (issue #2 hardening).

These cover the two correctness/performance fixes:
  - dynamic partition overwrite: re-running one partition must not delete
    the others (the old full-table overwrite did);
  - compaction: a partitioned write produces one file per partition rather
    than tasks x partitions small files.
"""

import glob
import os

from spark_applications.utils.storage import LocalStorageAdapter

COLUMNS = [
    "user_id", "impression_id", "page_type",
    "date", "hour", "min", "second", "event_type",
]


def test_partition_overwrite_is_non_destructive(spark, tmp_path):
    """Reprocessing page_type=1 must leave the page_type=2 partition intact."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    partition_cols = ["page_type", "date", "hour"]

    initial = [
        ("u1", "i1", "1", "2026-01-01", 10, 0, 1, "a"),
        ("u2", "i2", "2", "2026-01-01", 10, 0, 1, "a"),
    ]
    adapter.write_partitioned(
        spark.createDataFrame(initial, COLUMNS), "impressions", partition_cols
    )

    # Re-run ONLY page_type=1 with replacement data.
    reprocess = [("u9", "i9", "1", "2026-01-01", 10, 0, 5, "b")]
    adapter.write_partitioned(
        spark.createDataFrame(reprocess, COLUMNS),
        "impressions",
        partition_cols,
    )

    rows = adapter.read_partitioned(spark, "impressions").collect()
    pt1 = [r for r in rows if str(r.page_type) == "1"]
    pt2 = [r for r in rows if str(r.page_type) == "2"]

    # page_type=2 survived the page_type=1 reprocess (was destroyed before).
    assert len(pt2) == 1
    assert pt2[0].user_id == "u2"
    # page_type=1 was replaced, not appended to.
    assert len(pt1) == 1
    assert pt1[0].user_id == "u9"


def test_partitioned_write_compacts_to_one_file_per_partition(spark, tmp_path):
    """All rows of a partition key land in a single output file."""
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))

    data = [
        (f"u{i}", f"i{i}", "1", "2026-01-01", 10, 0, i, "a")
        for i in range(20)
    ]
    adapter.write_partitioned(
        spark.createDataFrame(data, COLUMNS),
        "impressions",
        ["page_type", "date", "hour"],
    )

    part_dir = os.path.join(
        str(tmp_path), "processed", "impressions",
        "page_type=1", "date=2026-01-01", "hour=10",
    )
    data_files = glob.glob(os.path.join(part_dir, "*.parquet"))
    assert len(data_files) == 1


# --- raw-file primitives behind the transactional landing (#3) -------------

def test_move_raw_file_relocates_and_creates_parent(tmp_path):
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    adapter.save_raw_file(b"bytes", "a/_staging/job/data.csv.gz")

    adapter.move_raw_file(
        "a/_staging/job/data.csv.gz", "a/page_type=1/date=d/hour=1/data.csv.gz"
    )

    assert not adapter.raw_file_exists("a/_staging/job/data.csv.gz")
    assert adapter.raw_file_exists("a/page_type=1/date=d/hour=1/data.csv.gz")
    with open(
        os.path.join(
            str(tmp_path), "raw", "a/page_type=1/date=d/hour=1/data.csv.gz"
        ), "rb"
    ) as f:
        assert f.read() == b"bytes"


def test_delete_raw_file_is_idempotent(tmp_path):
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    adapter.save_raw_file(b"x", "p/data.csv.gz")
    adapter.delete_raw_file("p/data.csv.gz")
    adapter.delete_raw_file("p/data.csv.gz")  # already gone: no error
    assert not adapter.raw_file_exists("p/data.csv.gz")


def test_manifest_round_trip_and_missing(tmp_path):
    adapter = LocalStorageAdapter(base_dir=str(tmp_path))
    assert adapter.read_raw_manifest("p/_manifest.json") is None
    adapter.write_raw_manifest(
        {"job_id": "j", "rows_written": 3}, "p/_manifest.json"
    )
    assert adapter.read_raw_manifest("p/_manifest.json") == {
        "job_id": "j", "rows_written": 3,
    }
