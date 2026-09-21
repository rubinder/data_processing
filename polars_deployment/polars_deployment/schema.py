"""Column names, dtypes and partition layout for the impression events.

The schema matches ``raw.impressions`` in the dbt deployment and the DuckDB
table in ``duckdb_deployment``. The integer columns are narrowed to the
smallest type that fits: Polars keeps whole columns in memory, so a
``hour`` column stored as Int8 is one byte per row rather than eight.
"""
import polars as pl

EVENT_TYPES = ["a", "b", "c", "d", "e", "f"]

COLUMNS = [
    "user_id", "impression_id", "page_type",
    "date", "hour", "min", "second", "event_type",
]

SCHEMA: dict[str, pl.DataType] = {
    "user_id": pl.String,
    "impression_id": pl.String,
    "page_type": pl.Int8,
    "date": pl.Date,
    "hour": pl.Int8,
    "min": pl.Int8,
    "second": pl.Int8,
    "event_type": pl.String,
}

# Hive partition columns, in directory order:
# page_type=1/date=2026-01-01/hour=10/data.parquet
PARTITION_COLUMNS = ["page_type", "date", "hour"]

HIVE_SCHEMA: dict[str, pl.DataType] = {
    name: SCHEMA[name] for name in PARTITION_COLUMNS
}

# Columns physically stored inside each Parquet file (everything that is not
# encoded in the directory path).
FILE_COLUMNS = [c for c in COLUMNS if c not in PARTITION_COLUMNS]
