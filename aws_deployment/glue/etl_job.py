"""AWS Glue ETL job: raw impression CSV on S3 -> partitioned zstd parquet.

This script mirrors the hardening decisions made on the Spark side
(spark_applications/DECISIONS.md #2 and #6) so a file processed by Glue is
indistinguishable from one processed by ``spark_applications``:

- explicit schema, no ``inferSchema`` (one read pass, deterministic types);
- PERMISSIVE read with a corrupt-record column; unparseable rows are written
  to ``s3://<target>/quarantine/<run_id>/`` instead of poisoning the table;
- dynamic partition overwrite instead of ``append`` (re-running a file is
  idempotent and touches only the partitions it contains);
- repartition by the partition columns before writing (one file per
  partition directory, no small-files explosion);
- zstd parquet;
- no ``df.count()`` progress logging; one cached materialisation feeds the
  valid/quarantine split and the row counts, and a single structured JSON
  line is emitted at the end.

The impression schema is copied from
``spark_applications/spark_applications/utils/schema.py`` because the Glue
runtime cannot import that package. Keep the two in sync.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Arguments the Step Function always passes (see cloudformation/main.yaml,
# RunGlueETL state). JOB_NAME is injected by Glue itself.
REQUIRED_ARGS = [
    "JOB_NAME",
    "source_bucket",
    "source_key",
    "target_bucket",
    "database_name",
    "table_name",
]

# Optional arguments: --run_id is passed by the Step Function
# ($$.Execution.Name) but not by a manual console run. getResolvedOptions
# raises on a missing name, so it is only requested when present in argv.
# Do NOT list JOB_RUN_ID / JOB_NAME / JOB_ID / TempDir here: getResolvedOptions
# pre-registers those Glue-injected options itself and returns them in its
# result; asking for them again fails with "conflicting option string"
# (seen on a live Glue 5.0 run).
OPTIONAL_ARGS = ["run_id"]

PARTITION_COLS = ["page_type", "date", "hour"]

# Column Spark populates with the raw text of any row it could not parse
# against the schema (PERMISSIVE mode).
CORRUPT_RECORD_COL = "_corrupt_record"

# Copy of spark_applications.utils.schema.IMPRESSION_SCHEMA.
# page_type / date stay strings: they are partition keys and partition path
# values are strings by definition. hour is an int on both sides so the
# partition directories (hour=7) match what spark_applications writes.
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

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
log = logging.getLogger("glue_etl")


def log_event(event: str, **fields) -> None:
    """Emit one single-line JSON log record (indexable in CloudWatch)."""
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "job": "glue-etl",
        "event": event,
    }
    record.update(fields)
    log.info(json.dumps(record, default=str))


def read_schema() -> StructType:
    """Impression schema plus the corrupt-record column.

    Spark only captures unparseable rows in PERMISSIVE mode when the column
    named by ``columnNameOfCorruptRecord`` exists in the read schema.
    """
    return StructType(
        IMPRESSION_SCHEMA.fields
        + [StructField(CORRUPT_RECORD_COL, StringType(), True)]
    )


def resolve_args(argv: list[str]) -> dict:
    """Resolve required args plus whichever optional ones are present."""
    optional = [name for name in OPTIONAL_ARGS if f"--{name}" in argv]
    return getResolvedOptions(argv, REQUIRED_ARGS + optional)


def resolve_run_id(args: dict) -> str:
    """Step Function execution name, else Glue run id, else a timestamp."""
    return (
        args.get("run_id")
        or args.get("JOB_RUN_ID")
        or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )


def read_raw(spark, path: str) -> DataFrame:
    """Read the raw CSV with the explicit schema in PERMISSIVE mode.

    ``.gz`` inputs are decompressed by Spark in a distributed manner; nothing
    is pulled through the driver.
    """
    return (
        spark.read.schema(read_schema())
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .csv(path)
    )


def split(df: DataFrame) -> tuple[DataFrame, DataFrame, int, int]:
    """Split into (valid, quarantine) and count both, off one cached scan.

    The cache is not just an optimisation: Spark refuses a query that
    references only the internal corrupt-record column on a fresh
    file scan, and caching the parsed DataFrame first is the documented
    way around that. The two ``count()`` calls below hit the cache; they
    are the only actions besides the writes.
    """
    df = df.cache()
    is_corrupt = F.col(CORRUPT_RECORD_COL).isNotNull()

    valid = df.filter(~is_corrupt).drop(CORRUPT_RECORD_COL)
    quarantine = df.filter(is_corrupt).select(CORRUPT_RECORD_COL)

    total = df.count()
    quarantined = quarantine.count()
    return valid, quarantine, total - quarantined, quarantined


def write_partitioned(df: DataFrame, path: str) -> None:
    """Idempotent partitioned parquet write, one file per partition.

    - ``repartition`` by the partition keys so each partition directory gets
      a single file instead of (tasks x partitions) fragments.
    - ``partitionOverwriteMode=dynamic`` scopes the overwrite to the
      partitions present in ``df``; the rest of the table is untouched.
      Also set job-wide via ``--conf`` in main.yaml; the per-write option
      makes the script safe when run outside that job definition.
    - zstd: smaller files than snappy for a modest CPU cost, and bytes
      written here are bytes Athena scans later (FINOPS.md).
    """
    (
        df.repartition(*[F.col(c) for c in PARTITION_COLS])
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .option("compression", "zstd")
        .partitionBy(*PARTITION_COLS)
        .parquet(path)
    )


def write_quarantine(
    df: DataFrame, path: str, source_path: str, run_id: str
) -> None:
    """Write corrupt rows (raw text + provenance) for inspection/replay."""
    (
        df.withColumn("source_path", F.lit(source_path))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("quarantined_at", F.current_timestamp())
        .coalesce(1)
        .write.mode("overwrite")
        .option("compression", "zstd")
        .parquet(path)
    )


def main(argv: list[str]) -> None:
    started = time.monotonic()
    args = resolve_args(argv)
    run_id = resolve_run_id(args)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_path = f"s3://{args['source_bucket']}/{args['source_key']}"
    target_path = f"s3://{args['target_bucket']}/processed/"
    quarantine_path = f"s3://{args['target_bucket']}/quarantine/{run_id}/"

    log_event(
        "job_start",
        run_id=run_id,
        source=source_path,
        target=target_path,
        # Catalog registration of the processed table is the crawler's
        # job; these are logged so the run is traceable to its table.
        database=args["database_name"],
        table=args["table_name"],
    )

    raw = read_raw(spark, source_path)
    valid, quarantine, rows_written, rows_quarantined = split(raw)

    write_partitioned(valid, target_path)
    if rows_quarantined > 0:
        write_quarantine(quarantine, quarantine_path, source_path, run_id)

    raw.unpersist()
    job.commit()

    # Single terminal record: the counts came from the cached split, not
    # from extra count() jobs on the write lineage.
    log_event(
        "job_complete",
        run_id=run_id,
        source=source_path,
        target=target_path,
        quarantine=quarantine_path if rows_quarantined else None,
        partition_cols=PARTITION_COLS,
        rows_read=rows_written + rows_quarantined,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
        compression="zstd",
        duration_s=round(time.monotonic() - started, 3),
    )


main(sys.argv)
