"""Impression-shaped test data for the debugging cases.

Uses the same columns as ``utils/schema.py``'s ``IMPRESSION_SCHEMA`` — the
data the real ``api_pull`` / ``aggregation`` jobs move — so the plans in
these cases are the plans you would actually see in this pipeline.

Everything is generated from ``spark.range`` and column expressions rather
than a Python list handed to ``createDataFrame``. That is deliberate on two
counts: it stays distributed at any row count, and building the fixtures with
a driver-side list would be the exact anti-pattern
:mod:`case_02_driver_collect_oom` is about.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The hot key. In the real data a handful of high-volume users (bots, internal
# load tests, a single embedded partner) dominate the distribution; every skew
# problem in this pipeline traces back to a key like this one.
HOT_USER_ID = "user_00000000-hot"

EVENT_TYPES = ["a", "b", "c", "d", "e", "f"]
PAGE_TYPES = ["1", "2", "3"]

DEFAULT_DATE = "2026-01-01"
DEFAULT_HOUR = 10


def impression_events(
    spark: SparkSession,
    rows: int = 200_000,
    distinct_users: int = 5_000,
    skew_ratio: float = 0.8,
    date: str = DEFAULT_DATE,
    hour: int = DEFAULT_HOUR,
    seed: int = 42,
) -> DataFrame:
    """Generate impression events with a configurable hot key.

    Args:
        rows: Total rows to generate.
        distinct_users: Size of the non-hot user population.
        skew_ratio: Fraction of rows assigned to :data:`HOT_USER_ID`. At the
            default 0.8, four in five rows share one join key — enough to
            make one shuffle partition dwarf the rest. Pass ``0.0`` for an
            evenly distributed frame.
        seed: Fixed so runs and tests are reproducible.
    """
    base = spark.range(0, rows).withColumnRenamed("id", "row_id")

    user_id = (
        F.when(
            F.rand(seed) < F.lit(skew_ratio),
            F.lit(HOT_USER_ID),
        )
        .otherwise(
            F.concat(
                F.lit("user_"),
                F.lpad(
                    (F.col("row_id") % distinct_users).cast("string"), 8, "0"
                ),
            )
        )
    )

    return (
        base
        .withColumn("user_id", user_id)
        .withColumn(
            "impression_id",
            F.concat(
                F.lit("imp_"),
                F.lpad(
                    (F.col("row_id") % 50_000).cast("string"), 10, "0"
                ),
            ),
        )
        .withColumn(
            "page_type",
            F.element_at(
                F.array(*[F.lit(value) for value in PAGE_TYPES]),
                (F.col("row_id") % len(PAGE_TYPES) + 1).cast("int"),
            ),
        )
        .withColumn("date", F.lit(date))
        .withColumn("hour", F.lit(hour).cast("int"))
        .withColumn("min", (F.col("row_id") % 60).cast("int"))
        .withColumn("second", (F.col("row_id") % 60).cast("int"))
        .withColumn(
            "event_type",
            F.element_at(
                F.array(*[F.lit(value) for value in EVENT_TYPES]),
                (F.col("row_id") % len(EVENT_TYPES) + 1).cast("int"),
            ),
        )
        .drop("row_id")
    )


def user_dimension(
    spark: SparkSession,
    distinct_users: int = 5_000,
) -> DataFrame:
    """Small user lookup table, including the hot key so joins match.

    Small enough to broadcast — which is the point of case_01: the fix is
    usually "let Spark broadcast this", not "write a salted join".
    """
    regular = (
        spark.range(0, distinct_users)
        .withColumn(
            "user_id",
            F.concat(
                F.lit("user_"),
                F.lpad(F.col("id").cast("string"), 8, "0"),
            ),
        )
        .withColumn(
            "segment",
            F.element_at(
                F.array(F.lit("free"), F.lit("pro"), F.lit("enterprise")),
                (F.col("id") % 3 + 1).cast("int"),
            ),
        )
        .withColumn(
            "country",
            F.element_at(
                F.array(F.lit("US"), F.lit("GB"), F.lit("DE"), F.lit("JP")),
                (F.col("id") % 4 + 1).cast("int"),
            ),
        )
        .drop("id")
    )

    hot = spark.createDataFrame(
        [(HOT_USER_ID, "enterprise", "US")],
        ["user_id", "segment", "country"],
    )
    return regular.unionByName(hot)


def write_impression_table(
    spark: SparkSession,
    path: str,
    dates: list[str] | None = None,
    hours: list[int] | None = None,
    rows_per_partition: int = 2_000,
) -> str:
    """Write a partitioned parquet impression table and return its path.

    Partitioned by ``page_type``/``date``/``hour``, matching what ``api_pull``
    writes, so the partition-pruning case reads a realistically-shaped table.
    """
    dates = dates or ["2026-01-01", "2026-01-02", "2026-01-03"]
    hours = hours or [9, 10, 11]

    frames = [
        impression_events(
            spark,
            rows=rows_per_partition,
            distinct_users=200,
            skew_ratio=0.0,
            date=date,
            hour=hour,
            seed=hash((date, hour)) % 10_000,
        )
        for date in dates
        for hour in hours
    ]

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)

    (
        combined.write
        .mode("overwrite")
        .partitionBy("page_type", "date", "hour")
        .parquet(path)
    )
    return path
