"""Explicit schemas for impression data.

Using an explicit schema instead of ``inferSchema=True`` is deliberate:

- ``inferSchema`` triggers an extra full pass over the data just to guess
  types, doubling read cost on large inputs.
- Inferred types are non-deterministic across runs/files (an all-null or
  all-integer column infers differently), which silently breaks downstream
  jobs when the input shifts.
- An explicit schema is a contract: a column added, dropped, or retyped
  upstream surfaces here instead of corrupting the table downstream.
"""

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Column Spark populates with the raw text of any row it could not parse
# against the schema (PERMISSIVE mode). Rows with a non-null value here are
# quarantined rather than written to the table.
CORRUPT_RECORD_COL = "_corrupt_record"

# Raw impression CSV as served by web_server_code. Header columns:
# user_id, impression_id, page_type, date, hour, min, second, event_type
#
# page_type / date are kept as strings: they are partition keys and partition
# path values are strings by definition (see session config that disables
# partition column type inference).
IMPRESSION_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("impression_id", StringType(), nullable=False),
        StructField("page_type", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("hour", IntegerType(), nullable=False),
        StructField("min", IntegerType(), nullable=False),
        StructField("second", IntegerType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
    ]
)


def schema_with_corrupt_column(schema: StructType) -> StructType:
    """Return a copy of ``schema`` with the corrupt-record column appended.

    Spark requires this column to be present in the read schema for it to
    capture unparseable rows in PERMISSIVE mode.
    """
    return StructType(
        schema.fields + [StructField(CORRUPT_RECORD_COL, StringType(), True)]
    )
