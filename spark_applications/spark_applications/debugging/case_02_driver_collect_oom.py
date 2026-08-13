"""Case 02 — the driver dies, not an executor.

A different failure from case 01 that gets diagnosed the same wrong way
("give it more memory"). The tell is *which* JVM ran out: an executor OOM is a
partitioning problem, a driver OOM is almost always a driver-side action that
should not be there.

Run: ``uv run python -m spark_applications.debugging.run --case 2``
"""

from pyspark.sql import functions as F

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    count_operator,
    exchange_count,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import get_debug_session

CASE_ID = "02"
TITLE = "Driver OOM — work that never left the driver"

SYMPTOM = """
The job dies before any output is written. Nothing in the executor logs; the
driver log ends with:

    java.lang.OutOfMemoryError: Java heap space
      at java.util.Arrays.copyOf(Arrays.java:3332)
      at org.apache.spark.sql.Dataset.collectFromPlan(Dataset.scala:3715)
      at org.apache.spark.sql.Dataset.$anonfun$collect$1(Dataset.scala:2989)

or, on a cluster that catches it first:

    Total size of serialized results of 214 tasks (4.1 GB) is bigger than
    spark.driver.maxResultSize (4.0 GB)

Bumping spark.driver.memory makes it survive one more input size, then fail
again. The size that breaks it tracks input volume, not cluster size.
"""

CAUSE = """
`collectFromPlan` in the stack trace is the whole diagnosis: every row was
pulled into a single JVM heap. Three ways this gets written without anyone
meaning to:

1. df.collect() / df.toPandas() on an unbounded result.
2. Iterating in Python — `for row in df.collect()` — to do a transformation
   that could have been a column expression.
3. len(df.collect()) as a row count, instead of df.count().

Common in code that grew: it worked at 10k rows in development, and nothing
about it looks distributed-unsafe until production volume arrives.

The plan tells you nothing is wrong here, because nothing *is* wrong with the
plan — the bug is in what Python does with the result. That is the lesson:
some Spark failures are invisible in explain() and only visible in the stack
trace and the driver's memory profile.
"""

RESOLUTION = """
Keep the computation in Spark and let it write from the executors.

    # BEFORE — every row through the driver's heap
    rows = df.collect()
    total = sum(row["event_count"] for row in rows)

    # AFTER — aggregation runs distributed, one row comes back
    total = df.agg(F.sum("event_count")).first()[0]

When you genuinely need data in the driver, bound it explicitly: .limit(n)
before .collect(), .take(n), or write to storage and read the result. For a
row count use .count(), never len(.collect()).
"""

NOTES = [
    "Read the stack trace for WHICH JVM died before tuning anything. "
    "Driver OOM (collectFromPlan, maxResultSize) = a driver-side action. "
    "Executor OOM (ExecutorLostFailure, container killed) = skew or "
    "partition sizing — that is case 01.",

    "spark.driver.maxResultSize is a guard rail, not a limit to raise. It "
    "firing means the code intends to move the whole result to one machine; "
    "raising it just converts a clean error into a heap OOM.",

    "toPandas() is collect() with extra steps and roughly double the peak "
    "memory (JVM rows plus the pandas copy). Arrow "
    "(spark.sql.execution.arrow.pyspark.enabled) reduces the conversion "
    "cost but not the fact that everything lands in the driver.",

    "show() and take(n) are safe — they push a limit into the plan. "
    "collect() does not.",

    "A large broadcast join causes driver OOM too: the small side is "
    "collected to the driver before being broadcast out. If this OOM "
    "appeared right after someone 'fixed' case 01 with a broadcast() hint, "
    "that hint is the cause — the side was not as small as assumed.",

    "This repo hit a version of this in DECISIONS.md #2: api_pull "
    "downloaded and gzip-decompressed the whole file on the driver before "
    "Spark saw it, so the job could not scale past driver memory.",
]


def build_broken(spark, rows: int = 50_000):
    """Aggregate by pulling every row into the driver.

    Returns the DataFrame that would be collected, plus the driver-side
    function that does the damage — kept separate so the test can assert on
    the pattern without actually exhausting the heap.
    """
    return fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)


def total_events_broken(df) -> int:
    """The anti-pattern: every row crosses into the driver's heap."""
    return len(df.collect())


def build_fixed(spark, rows: int = 50_000):
    """The same question asked as a distributed aggregation."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.agg(F.count(F.lit(1)).alias("total_events"))


def total_events_fixed(df) -> int:
    """One row crosses into the driver, regardless of input size."""
    return df.agg(F.count(F.lit(1)).alias("total")).first()["total"]


def diagnose(spark, rows: int = 50_000) -> Diagnosis:
    """Capture both plans and show what each returns to the driver."""
    broken_df = build_broken(spark, rows=rows)
    fixed_df = build_fixed(spark, rows=rows)

    broken_plan = capture_plan(broken_df)
    fixed_plan = capture_plan(fixed_df)

    # Rows that would cross the driver boundary. Safe to run here only
    # because `rows` is small; that is exactly the illusion that hides this
    # bug in development.
    broken_driver_rows = broken_df.count()
    fixed_driver_rows = fixed_df.count()

    return Diagnosis(
        case_id=CASE_ID,
        title=TITLE,
        symptom=SYMPTOM,
        cause=CAUSE,
        resolution=RESOLUTION,
        broken_plan=broken_plan,
        fixed_plan=fixed_plan,
        evidence=[
            Evidence(
                look_for="Rows returned to the driver by collect()",
                broken=f"{broken_driver_rows:,} (grows with input)",
                fixed=f"{fixed_driver_rows:,} (constant)",
            ),
            Evidence(
                look_for="HashAggregate nodes (work done in the cluster)",
                broken=str(count_operator(broken_plan, "HashAggregate")),
                fixed=str(count_operator(fixed_plan, "HashAggregate")),
            ),
            Evidence(
                look_for="Exchange nodes",
                broken=str(exchange_count(broken_plan)),
                fixed=str(exchange_count(fixed_plan)),
            ),
        ],
        metrics={
            "driver rows, 50k input": (
                f"{broken_driver_rows:,} vs {fixed_driver_rows:,}"
            ),
            "driver rows, 5bn input": "5,000,000,000 vs 1",
            "plan difference": (
                "none that flags the bug — the broken plan is a perfectly "
                "healthy scan. explain() cannot see a driver-side action."
            ),
        },
        notes=NOTES,
    )


def main():
    spark = get_debug_session("Debug-02-DriverOOM")
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
