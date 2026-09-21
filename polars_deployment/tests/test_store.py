"""ParquetStore: layout, idempotent partition writes, hive scan and pruning."""
import polars as pl

from polars_deployment.schema import COLUMNS, SCHEMA
from polars_deployment.store import ParquetStore
from tests.conftest import DATE, HOUR


def test_partition_layout(store):
    assert store.partition_dir(2, "2026-01-01", 10) == (
        store.root / "page_type=2" / "date=2026-01-01" / "hour=10"
    )
    assert store.partitions() == [(1, DATE, HOUR), (2, DATE, HOUR), (3, DATE, HOUR)]
    assert len(store.files()) == 3


def test_scan_recovers_schema_and_rows(store, events_df):
    scanned = store.scan().collect()
    assert scanned.columns == COLUMNS
    assert dict(scanned.schema) == SCHEMA
    key = ["user_id", "impression_id", "event_type"]
    assert scanned.sort(key).equals(events_df.sort(key))


def test_scan_empty_store(tmp_path):
    empty = ParquetStore(tmp_path / "nothing")
    scanned = empty.scan().collect()
    assert scanned.height == 0
    assert dict(scanned.schema) == SCHEMA
    assert empty.partitions() == []


def test_write_partition_replaces(store, events_df):
    pt1 = events_df.filter(pl.col("page_type") == 1)
    # Rewrite page_type 1 with a subset: the old rows must be gone.
    store.write_partition(pt1.head(2), 1, DATE, HOUR)
    remaining = store.scan().filter(pl.col("page_type") == 1).collect()
    assert remaining.height == 2
    assert len(store.files()) == 3


def test_partition_filter_prunes_files(store):
    """A filter on a hive column reads only that partition's file."""
    unfiltered = store.scan().explain()
    assert "2 other sources" in unfiltered, unfiltered

    plan = store.scan().filter(pl.col("page_type") == 3).explain()
    # The optimized scan lists only the files that survive the hive
    # predicate: one path, no "... N other sources" suffix.
    assert "page_type=3/" in plan, plan
    assert "other sources" not in plan, plan
    rows = store.scan().filter(pl.col("page_type") == 3).collect()
    assert rows["page_type"].unique().to_list() == [3]


def test_projection_pushdown(store):
    """Selecting two columns only reads two columns from Parquet."""
    plan = store.scan().select("user_id", "event_type").explain()
    assert "PROJECT 2/8 COLUMNS" in plan, plan
