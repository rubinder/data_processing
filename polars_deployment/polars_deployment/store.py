"""Hive-partitioned Parquet store for the impression events.

Layout::

    <root>/page_type=1/date=2026-01-01/hour=10/data.parquet

Each (page_type, date, hour) partition is one directory holding one file.
Writing a partition replaces the directory, so a load is idempotent. Reading
is a single ``scan_parquet`` over the tree with hive partitioning, which
recovers ``page_type``/``date``/``hour`` from the paths and prunes whole
files when a query filters on them.
"""
import datetime as dt
import shutil
from pathlib import Path

import polars as pl

from polars_deployment.schema import (
    COLUMNS, FILE_COLUMNS, HIVE_SCHEMA, SCHEMA,
)

DATA_FILE = "data.parquet"


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value)


class ParquetStore:
    """A directory of hive-partitioned Parquet files."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def partition_dir(
        self, page_type: int, date: str | dt.date, hour: int
    ) -> Path:
        """Directory for one (page_type, date, hour) partition."""
        return (
            self.root
            / f"page_type={int(page_type)}"
            / f"date={_as_date(date).isoformat()}"
            / f"hour={int(hour)}"
        )

    def write_partition(
        self,
        df: pl.DataFrame,
        page_type: int,
        date: str | dt.date,
        hour: int,
    ) -> Path:
        """Replace one partition with ``df``.

        The partition columns are dropped before writing: their values live
        in the directory path and are re-attached by ``scan``. Any rows in
        ``df`` are assumed to belong to the partition; the loader fetches one
        partition per call so this holds by construction.
        """
        target = self.partition_dir(page_type, date, hour)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        path = target / DATA_FILE
        (
            df.select(FILE_COLUMNS)
            .cast({c: SCHEMA[c] for c in FILE_COLUMNS})
            .write_parquet(path, compression="zstd", statistics=True)
        )
        return path

    def files(self) -> list[Path]:
        """Every partition file under the root, sorted by path."""
        return sorted(self.root.glob(f"*/*/*/{DATA_FILE}"))

    def partitions(self) -> list[tuple[int, dt.date, int]]:
        """(page_type, date, hour) for every partition present."""
        out = []
        for path in self.files():
            hour_dir, date_dir, pt_dir = (
                path.parent, path.parent.parent, path.parent.parent.parent
            )
            out.append((
                int(pt_dir.name.split("=", 1)[1]),
                dt.date.fromisoformat(date_dir.name.split("=", 1)[1]),
                int(hour_dir.name.split("=", 1)[1]),
            ))
        return sorted(out)

    def scan(self) -> pl.LazyFrame:
        """Lazy scan of the whole store, columns in canonical order.

        Nothing is read here. Filters on ``page_type``/``date``/``hour``
        applied to the returned frame are pushed into the scan and skip
        the files of partitions they exclude.
        """
        files = self.files()
        if not files:
            return pl.LazyFrame(schema=SCHEMA)
        return (
            pl.scan_parquet(
                self.root / "**" / DATA_FILE,
                hive_partitioning=True,
                hive_schema=HIVE_SCHEMA,
            )
            .select(COLUMNS)
        )
