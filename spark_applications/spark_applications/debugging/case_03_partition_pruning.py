"""Case 03 — a filtered read that scans the whole table anyway.

The job asks for one hour and reads three days. Nothing fails; it is just
slower and more expensive every month as the table grows, which is why this
one usually ships and lives in production for a year before anyone looks.

Run: ``uv run python -m spark_applications.debugging.run --case 3``
"""

import os
import shutil
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    partition_filters,
    pushed_filters,
    python_eval_nodes,
    scan_metrics,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import (
    apply_conf,
    get_debug_session,
    restore_conf,
)

CASE_ID = "03"
TITLE = "Partition pruning lost to a UDF"

SYMPTOM = """
`aggregation.py --date 2026-01-01 --hour 10` reads one hour of impressions.
The table is partitioned by page_type/date/hour, so it should touch a handful
of directories. Instead:

  - Athena/Databricks report scanned bytes equal to the whole table.
  - Runtime grows linearly with table age, not with the hour being processed.
  - The Spark UI's scan node shows "number of files read" in the thousands.

No error. Just a job whose cost is proportional to history rather than to the
work requested.
"""

CAUSE = """
The filter is applied through a Python UDF:

    df.filter(normalize_date(F.col("date")) == target)

Spark's optimiser has to prove a filter is safe to evaluate against partition
*metadata* before it can skip directories. It can see through casts and
built-in functions, but a Python UDF is an opaque black box — it cannot know
what the function does, so it cannot use it to eliminate partitions. The
filter is demoted to a row-level predicate evaluated after everything is read.

The plan shows both halves of this exactly:

  - the UDF'd predicate is *missing* from PartitionFilters, so the date
    dimension is not pruned at all
  - a BatchEvalPython node appears above the scan (every row shipped to a
    Python worker, just to decide whether to keep it)

Note what makes this hard to spot: PartitionFilters is not empty. The
plain `hour` predicate still prunes, so the list looks populated and healthy.
Only one of the two partition dimensions was lost. Measured on the fixture
table, that is 72 files across 9 partitions instead of 24 across 3 — the job
reads all three days to return one. The wider the un-pruned dimension (a year
of dates rather than three), the worse the ratio.

A useful correction to a widespread myth: Spark 3.5 *does* push casts,
to_date() and substring() into PartitionFilters. `F.col("hour").cast("int")
== 10` — what aggregation.py does — prunes correctly. UDFs are the thing
that actually breaks pruning, not casts.
"""

RESOLUTION = """
Express the predicate with built-in column functions so the optimiser can
read it:

    # BEFORE — opaque to the optimiser, no pruning
    df.filter(normalize_date(F.col("date")) == "2026-01-01")

    # AFTER — pruned before a single file is opened
    df.filter((F.col("date") == "2026-01-01")
              & (F.col("hour").cast("int") == 10))

If the transformation genuinely cannot be expressed in built-ins, apply the
UDF *after* filtering on the raw partition columns, so pruning happens first
and the UDF only sees the rows that survived.
"""

NOTES = [
    "PartitionFilters and PushedFilters are different things. "
    "PartitionFilters eliminate whole directories using path metadata — no "
    "file is opened. PushedFilters are handed to the Parquet reader to skip "
    "row groups inside files that were opened. Only the first avoids I/O "
    "entirely; a non-partition column can only ever produce the second.",

    "Do not just check that PartitionFilters is non-empty — check that "
    "EVERY partition column you filtered on appears in it. Partial pruning "
    "looks healthy at a glance and is the usual way this hides.",

    "Casts do NOT break pruning in Spark 3.5 (verified: "
    "'(cast(hour#656 as int) = 10)' appears inside PartitionFilters). Do "
    "not contort a filter to avoid a cast — it is cargo cult.",

    "The partition column must be in the filter to prune. Filtering on a "
    "correlated non-partition column (event_timestamp instead of date) "
    "prunes nothing, however obvious the relationship looks to a human.",

    "A filter applied after a wide transformation (join, groupBy) may not "
    "reach the scan. Filter as early as possible, ideally in the read.",

    "This is the highest-leverage thing to check on a cloud warehouse: "
    "S3/Athena/BigQuery bill scanned bytes, so lost pruning is a line item, "
    "not just latency.",
]

TARGET_DATE = "2026-01-01"
TARGET_HOUR = 10

# This case reads partition values as strings, which is what the real jobs
# do: utils/session.py disables partition column type inference for
# deterministic typing, and IMPRESSION_SCHEMA declares date/page_type as
# StringType. It matters here — with inference left on, Spark reads `date`
# back as a DateType, the identity UDF's declared StringType return no longer
# matches, and the broken path silently returns zero rows instead of too
# many. Pinning it keeps the case measuring pruning rather than a type
# coincidence.
CASE_CONF = {
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
}


def _normalize_date_udf():
    """An identity UDF standing in for the real thing.

    In production this is usually something innocent that arrived during a
    migration — trimming whitespace, or mapping a legacy date format.
    """
    return F.udf(lambda value: value, StringType())


def build_broken(spark, table_path: str):
    """Filter the partition column through a Python UDF."""
    normalize = _normalize_date_udf()
    return (
        spark.read.parquet(table_path)
        .filter(normalize(F.col("date")) == TARGET_DATE)
        .filter(F.col("hour").cast("int") == TARGET_HOUR)
    )


def build_fixed(spark, table_path: str):
    """Filter the partition columns directly."""
    return (
        spark.read.parquet(table_path)
        .filter(F.col("date") == TARGET_DATE)
        .filter(F.col("hour").cast("int") == TARGET_HOUR)
    )


def diagnose(spark, table_path: str | None = None) -> Diagnosis:
    """Capture both plans and the files each scan actually opens."""
    temp_dir = None
    previous = apply_conf(spark, CASE_CONF)

    if table_path is None:
        temp_dir = tempfile.mkdtemp(prefix="spark-debug-03-")
        table_path = fixtures.write_impression_table(
            spark,
            os.path.join(temp_dir, "impressions"),
            rows_per_partition=300,
        )

    try:
        broken_df = build_broken(spark, table_path)
        fixed_df = build_fixed(spark, table_path)

        broken_plan = capture_plan(broken_df)
        fixed_plan = capture_plan(fixed_df)

        broken_scan = scan_metrics(broken_df)
        fixed_scan = scan_metrics(fixed_df)

        def files(metrics: list[dict]) -> str:
            if not metrics:
                return "unavailable"
            scan = metrics[0]
            return (
                f"{scan.get('numFiles', '?')} files / "
                f"{scan.get('numPartitions', '?')} partitions / "
                f"{scan.get('filesSize', '?')} bytes"
            )

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
                    look_for="PartitionFilters (directories eliminated)",
                    broken=", ".join(partition_filters(broken_plan))
                    or "[] — nothing eliminated, full scan",
                    fixed=", ".join(partition_filters(fixed_plan)) or "[]",
                ),
                Evidence(
                    look_for="PushedFilters (row groups skipped in-file)",
                    broken=", ".join(pushed_filters(broken_plan)) or "[]",
                    fixed=", ".join(pushed_filters(fixed_plan)) or "[]",
                ),
                Evidence(
                    look_for="Python evaluation above the scan",
                    broken=", ".join(python_eval_nodes(broken_plan)) or "none",
                    fixed=", ".join(python_eval_nodes(fixed_plan)) or "none",
                ),
                Evidence(
                    look_for="Scan actually read",
                    broken=files(broken_scan),
                    fixed=files(fixed_scan),
                ),
            ],
            metrics={
                "rows returned (identical)": (
                    f"{broken_df.count():,} vs {fixed_df.count():,} — "
                    "the results agree; only the cost differs"
                ),
            },
            notes=NOTES,
        )
    finally:
        restore_conf(spark, previous)
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    spark = get_debug_session("Debug-03-PartitionPruning", adaptive=False)
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
