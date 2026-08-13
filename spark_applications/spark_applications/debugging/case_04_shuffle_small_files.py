"""Case 04 — the job is fast and the table it writes is unusable.

The write succeeds in two minutes. Every downstream reader then slows to a
crawl. The damage is done by the *shape* of the output, not its content, so
nothing in the job's own logs looks wrong.

Run: ``uv run python -m spark_applications.debugging.run --case 4``
"""

import os
import shutil
import tempfile

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    exchange_count,
    exchange_partitionings,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import get_debug_session

CASE_ID = "04"
TITLE = "Small-files explosion on a partitioned write"

SYMPTOM = """
The hourly job finishes fine. Then:

  - the next job's *file listing* takes longer than its computation
  - the table has ~20,000 files averaging 8 KB for ~150 MB of data
  - S3 costs rise on LIST/GET requests rather than on storage
  - the Hive metastore / Glue catalog slows down on every partition query

    org.apache.spark.SparkException: Job aborted due to stage failure:
    Total size of serialized results ... FileAlreadyExistsException

or, on HDFS, the NameNode heap alert fires. Each file carries fixed metadata
overhead, so ten thousand tiny files cost far more to manage than one large
one holding the same bytes.
"""

CAUSE = """
Arithmetic, not a bug in the code:

    output files = (shuffle partitions) x (distinct partition-key combos)

With spark.sql.shuffle.partitions at its 200 default and a write partitioned
by page_type/date/hour, each of the 200 tasks holds a few rows for most
partition directories, and every task writes its own file into every
directory it touches. 200 tasks x 100 partition dirs = 20,000 files.

Hour-grain partitioning makes this worse, because it multiplies the number of
directories by 24 while dividing the data per directory by 24.

The plan shows the number of Exchange nodes but not the file count — this is
a case where the plan is necessary and not sufficient. You confirm it by
listing the output directory.
"""

RESOLUTION = """
Repartition by the same columns you partition the write by, so each partition
directory is produced by exactly one task:

    # BEFORE — every task writes into every directory
    df.write.partitionBy("page_type", "date", "hour").parquet(path)

    # AFTER — one writer per directory, one file per directory
    (df.repartition("page_type", "date", "hour")
       .write.partitionBy("page_type", "date", "hour").parquet(path))

This is what utils/storage.py::_compact does for the real jobs (DECISIONS.md
#2).

The fix is usually cheaper than it looks. If the job already shuffles before
the write — nearly all of them do — Spark collapses the redundant
repartition, so you are changing that shuffle's partitioning scheme from
RoundRobinPartitioning to hashpartitioning on the write keys rather than
adding a shuffle. The Exchange count in the plan is unchanged; only the
scheme differs. It genuinely adds a shuffle only when the write follows a
narrow pipeline with no shuffle at all.
"""

NOTES = [
    "Aim for files of roughly 128 MB - 1 GB. Below ~10 MB, per-file "
    "overhead dominates; above a few GB you lose read parallelism.",

    "repartition(cols) and partitionBy(cols) are different operations that "
    "have to agree. repartition controls how many TASKS hold the data; "
    "partitionBy controls the DIRECTORY layout. Matching them is what "
    "collapses the file count.",

    "Use coalesce(n) rather than repartition(n) only when reducing "
    "partitions with no shuffle is acceptable — coalesce does not "
    "redistribute, so it can leave the remaining partitions badly uneven, "
    "and it propagates upward to reduce the parallelism of the computation "
    "that feeds it.",

    "AQE's coalescePartitions shrinks small shuffle partitions "
    "automatically, but it operates on the shuffle, not on the "
    "partitionBy fan-out — it does not solve this on its own.",

    "If the fix makes the write slow, the repartition has probably created "
    "skew: one partition-key combo holding most rows now has one writer. "
    "That is case 01 wearing a different hat; repartition on a finer key or "
    "accept more files for the hot directory.",

    "Reconsider the partition grain before tuning. Partitioning by hour is "
    "only worth it if queries actually filter by hour AND each hour holds "
    "enough data to justify a directory. Otherwise partition by date and "
    "let the sort order handle hour.",
]


DATES = ["2026-01-01", "2026-01-02", "2026-01-03"]
HOURS = [9, 10, 11]
TASKS = 16


def _multi_partition_events(spark, rows: int):
    """Events spanning several date/hour combos, spread over TASKS tasks.

    The round-robin repartition stands in for whatever shuffle the real job
    already did (a groupBy, a join) before reaching its write.
    """
    per_combo = max(rows // (len(DATES) * len(HOURS)), 1)
    frames = [
        fixtures.impression_events(
            spark, rows=per_combo, distinct_users=200, skew_ratio=0.0,
            date=date, hour=hour, seed=hash((date, hour)) % 10_000,
        )
        for date in DATES
        for hour in HOURS
    ]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined.repartition(TASKS)


def build_broken(spark, rows: int = 20_000):
    """Written straight out: every task writes into every directory."""
    return _multi_partition_events(spark, rows)


def build_fixed(spark, rows: int = 20_000):
    """Repartitioned on the write layout: one task per directory."""
    return _multi_partition_events(spark, rows).repartition(
        "page_type", "date", "hour"
    )


PARTITION_COLS = ["page_type", "date", "hour"]


def _write_and_count(df, path: str) -> tuple[int, int]:
    """Write partitioned parquet, returning (file count, directory count)."""
    df.write.mode("overwrite").partitionBy(*PARTITION_COLS).parquet(path)

    files = 0
    directories = 0
    for _, _, filenames in os.walk(path):
        data_files = [
            name for name in filenames
            if name.endswith(".parquet") and not name.startswith(".")
        ]
        if data_files:
            directories += 1
            files += len(data_files)
    return files, directories


def diagnose(spark, rows: int = 20_000) -> Diagnosis:
    """Write both layouts and count what lands on disk."""
    temp_dir = tempfile.mkdtemp(prefix="spark-debug-04-")
    try:
        broken_df = build_broken(spark, rows=rows)
        fixed_df = build_fixed(spark, rows=rows)

        broken_plan = capture_plan(broken_df)
        fixed_plan = capture_plan(fixed_df)

        added_shuffles = exchange_count(fixed_plan) - exchange_count(
            broken_plan
        )

        broken_files, broken_dirs = _write_and_count(
            broken_df, os.path.join(temp_dir, "broken")
        )
        fixed_files, fixed_dirs = _write_and_count(
            fixed_df, os.path.join(temp_dir, "fixed")
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
                    look_for="Data files written",
                    broken=str(broken_files),
                    fixed=str(fixed_files),
                ),
                Evidence(
                    look_for="Partition directories (identical layout)",
                    broken=str(broken_dirs),
                    fixed=str(fixed_dirs),
                ),
                Evidence(
                    look_for="Files per directory",
                    broken=f"{broken_files / max(broken_dirs, 1):.1f}",
                    fixed=f"{fixed_files / max(fixed_dirs, 1):.1f}",
                ),
                Evidence(
                    look_for="Shuffle partitioning scheme",
                    broken=", ".join(exchange_partitionings(broken_plan)),
                    fixed=", ".join(exchange_partitionings(fixed_plan)),
                ),
                Evidence(
                    look_for="Exchange nodes",
                    broken=str(exchange_count(broken_plan)),
                    fixed=str(exchange_count(fixed_plan)),
                ),
            ],
            metrics={
                "file reduction": (
                    f"{broken_files} -> {fixed_files} files for the same "
                    f"{rows:,} rows in the same {fixed_dirs} directories"
                ),
                "the fix costs": (
                    f"{added_shuffles}"
                    " extra Exchange — Spark collapses the redundant "
                    "round-robin into the hash repartition, so this replaces "
                    "the partitioning scheme of a shuffle already being paid "
                    "for rather than adding one"
                ),
                "arithmetic": (
                    f"{TASKS} tasks x {broken_dirs} directories would be "
                    f"{TASKS * broken_dirs} files at worst; observed "
                    f"{broken_files}"
                ),
            },
            notes=NOTES,
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    spark = get_debug_session("Debug-04-SmallFiles", adaptive=False)
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
