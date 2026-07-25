"""The impressions table, in Iceberg.

Same event model as the rest of this repository (the Spark jobs, the dbt
models, the DuckDB and ClickHouse deployments all read it), so the table format
is the only variable being demonstrated here.

Two schema decisions are worth calling out, because they are the reason to
reach for Iceberg rather than partitioned Parquet:

**Hidden partitioning.** The table is partitioned by ``days(event_ts)`` and
``page_type``, but there is no ``day`` column. Iceberg records the *transform*
in the partition spec and applies it to ``event_ts`` on write; on read it
derives partition predicates from a filter on ``event_ts`` itself. With Hive
layout you must materialize a redundant ``day`` column and every query must
remember to filter on it, or it silently scans everything. That "silently"
is the whole problem: the query still returns correct results, just after a
full scan, so nobody notices until the table is large.

**Partition specs are evolvable.** ``days()`` can later become ``hours()``
without rewriting a byte of existing data -- Iceberg tracks which spec each
data file was written under and plans across both. See
``schema_evolution.evolve_partition_spec``.
"""
from datetime import datetime, timedelta

IMPRESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    user_id       STRING,
    impression_id STRING,
    page_type     INT,
    event_ts      TIMESTAMP,
    event_type    STRING
)
USING iceberg
PARTITIONED BY (days(event_ts), page_type)
TBLPROPERTIES (
    'format-version' = '2',
    -- v2 enables merge-on-read, which is what makes MERGE INTO cheap enough
    -- to run per batch: updates land as delete files instead of rewriting
    -- whole data files. The cost is paid back at read time, and reclaimed by
    -- the compaction in maintenance.py.
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read',
    -- Target file size: too small and the metadata explodes, too large and
    -- pruning gets coarse. 128MB is a reasonable default for analytics.
    'write.target-file-size-bytes' = '134217728'
)
"""

EVENT_TYPES = ["a", "b", "c", "d", "e", "f"]


def create_table(spark, table: str = "db.impressions") -> None:
    """Create the namespace and the Iceberg table if they do not exist."""
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(IMPRESSIONS_DDL.format(table=table))


def sample_rows(count: int = 200, start: datetime | None = None,
                seed: int = 7) -> list[tuple]:
    """Deterministic impression rows spread over a few days and page types."""
    import random

    rng = random.Random(seed)
    start = start or datetime(2026, 6, 1, 0, 0, 0)
    rows = []
    for i in range(count):
        page_type = (i % 3) + 1
        # Deeper funnels on higher page types, matching the repo's data model.
        depth = rng.randint(1, 2 + page_type)
        impression_id = f"imp_{i:06d}"
        user_id = f"user_{rng.randint(1, 50):04d}"
        for step in range(min(depth, len(EVENT_TYPES))):
            rows.append((
                user_id,
                impression_id,
                page_type,
                start + timedelta(days=i % 4, minutes=i, seconds=step * 5),
                EVENT_TYPES[step],
            ))
    return rows


def seed(spark, table: str = "db.impressions", count: int = 200,
         start: datetime | None = None, seed_value: int = 7) -> int:
    """Insert sample rows and return how many were written.

    The DataFrame schema is taken from the *table's current schema* rather than
    hardcoded, so this keeps working after the columns have been renamed,
    widened, or added to. A producer that hardcodes column names is exactly
    what breaks the first time a schema evolves -- which rather defeats the
    purpose of a format that makes evolution safe.
    """
    rows = sample_rows(count, start, seed_value)
    rows_to_df(spark, table, rows).writeTo(table).append()
    return len(rows)


def rows_to_df(spark, table: str, rows: list[tuple]):
    """Build a DataFrame matching ``table``'s *current* schema.

    Shared by the seeder and the upsert path. Both would otherwise hardcode
    column names and break the moment a column is renamed -- which would rather
    defeat the point of a format that makes renames safe.
    """
    from pyspark.sql.functions import lit
    from pyspark.sql.types import StructType

    table_schema = spark.table(table).schema
    # The five base columns keep their positions; anything added later is
    # appended, so it is filled with NULL.
    base = StructType(table_schema.fields[:5])
    df = spark.createDataFrame(rows, base)
    for field in table_schema.fields[5:]:
        df = df.withColumn(field.name, lit(None).cast(field.dataType))
    return df.select(*[f.name for f in table_schema.fields])
