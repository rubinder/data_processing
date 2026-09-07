"""Runtime measurements for the debugging cases, from the Spark REST API.

``run.py`` shows plan evidence; this collects the *runtime* numbers
``CLUSTER_RUN.md`` asks for: wall-clock per leg and, for the heaviest stage
of each leg, max-vs-median task duration and shuffle-read bytes, spill, and
whether AQE split a skewed partition. Everything comes from the driver's own
UI REST endpoint (``spark.ui.enabled`` must be on, which it is on EMR and
Databricks), so it needs no history server and no log scraping.

    spark-submit ... cluster_measure.py --case 1 --rows 20000000
    spark-submit ... cluster_measure.py --case 7 --rows 20000000
    spark-submit ... cluster_measure.py --production --rows 20000000

Prints a markdown table to stdout (EMR: the step's stdout log).
"""

import argparse
import json
import statistics
import time
import urllib.request
from dataclasses import dataclass

from pyspark.sql import SparkSession

from spark_applications.aggregation import (
    USER_KEY,
    aggregate_impressions,
    enrich_with_users,
)
from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    join_strategies,
    skew_join_applied,
)
from spark_applications.debugging.session import apply_conf, restore_conf


@dataclass
class LegResult:
    case: str
    leg: str
    rows: int
    seconds: float
    join: str
    stage_id: int | None
    task_max_s: float | None
    task_median_s: float | None
    shuffle_read_max_mb: float | None
    shuffle_read_median_mb: float | None
    spill_disk_mb: float | None
    aqe_skew_split: str

    def row(self) -> str:
        def f(v, nd=1):
            return "-" if v is None else f"{v:.{nd}f}"
        ratio = (
            "-" if not self.task_median_s or self.task_max_s is None
            else f"{self.task_max_s / self.task_median_s:.1f}x"
        )
        read_max = self.shuffle_read_max_mb
        read_med = self.shuffle_read_median_mb
        sratio = (
            "-" if not read_med or read_max is None
            else f"{read_max / read_med:.1f}x"
        )
        return (
            f"| {self.case} | {self.leg} | {self.rows:,} | {self.seconds:.1f} "
            f"| {self.join} | {f(self.task_max_s)} / {f(self.task_median_s)} "
            f"({ratio}) | {f(self.shuffle_read_max_mb)} / "
            f"{f(self.shuffle_read_median_mb)} ({sratio}) "
            f"| {f(self.spill_disk_mb)} | {self.aqe_skew_split} |"
        )


HEADER = (
    "| case | leg | rows | wall-clock s | join | task max / median s "
    "| shuffle read max / median MB | disk spill MB | AQE skew split |\n"
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
)


def _api(spark: SparkSession, path: str):
    base = spark.sparkContext.uiWebUrl
    app_id = spark.sparkContext.applicationId
    with urllib.request.urlopen(
        f"{base}/api/v1/applications/{app_id}{path}", timeout=30
    ) as resp:
        return json.load(resp)


def _executed_plan(spark: SparkSession) -> str | None:
    """Physical plan text of the most recent completed SQL execution."""
    try:
        executions = _api(spark, "/sql?details=true&length=10000")
    except Exception:  # noqa: BLE001 - the UI endpoint may be proxied away
        return None
    completed = [e for e in executions if e.get("status") == "COMPLETED"]
    if not completed:
        return None
    latest = max(completed, key=lambda e: e["id"])
    return latest.get("planDescription")


def _heaviest_stage(spark: SparkSession, since_stage: int) -> dict | None:
    """The completed stage (id > since_stage) with the most shuffle read."""
    stages = [
        s for s in _api(spark, "/stages?status=complete")
        if s["stageId"] > since_stage
    ]
    if not stages:
        return None
    return max(stages, key=lambda s: (s.get("shuffleReadBytes", 0),
                                      s.get("executorRunTime", 0)))


def _task_stats(spark: SparkSession, stage: dict) -> dict:
    tasks = _api(
        spark,
        f"/stages/{stage['stageId']}/{stage['attemptId']}/taskList"
        "?length=100000",
    )
    durations = [t.get("duration", 0) / 1000 for t in tasks
                 if t.get("status") == "SUCCESS"]
    reads = [
        (t.get("taskMetrics", {}).get("shuffleReadMetrics", {})
         .get("remoteBytesRead", 0)
         + t.get("taskMetrics", {}).get("shuffleReadMetrics", {})
         .get("localBytesRead", 0)) / 1e6
        for t in tasks if t.get("status") == "SUCCESS"
    ]
    spill = sum(t.get("taskMetrics", {}).get("diskBytesSpilled", 0)
                for t in tasks) / 1e6
    return {
        "task_max_s": max(durations) if durations else None,
        "task_median_s": statistics.median(durations) if durations else None,
        "shuffle_read_max_mb": max(reads) if reads else None,
        "shuffle_read_median_mb": statistics.median(reads) if reads else None,
        "spill_disk_mb": spill,
    }


def measure_leg(spark, case: str, leg: str, rows: int, conf: dict,
                build) -> LegResult:
    # Flushed so a step log shows how far the run got if a later leg dies.
    print(f"[cluster_measure] case {case}: {leg} ({rows:,} rows)", flush=True)
    previous = apply_conf(spark, conf)
    try:
        last_stage = max(
            (s["stageId"] for s in _api(spark, "/stages")), default=-1
        )
        df = build()
        start = time.monotonic()
        df.write.format("noop").mode("overwrite").save()
        seconds = time.monotonic() - start
        # The executed (post-AQE) plan of the write, from the SQL REST
        # endpoint. explain_tools.capture_final_plan would collect() the
        # DataFrame to force execution, which for the row-level frames of
        # case 07 means pulling every row onto the driver (it killed a 6g
        # driver on EMR). The write already executed; read its plan back.
        plan = _executed_plan(spark) or capture_plan(df)
    finally:
        restore_conf(spark, previous)

    stage = _heaviest_stage(spark, last_stage)
    stats = _task_stats(spark, stage) if stage else {
        "task_max_s": None, "task_median_s": None,
        "shuffle_read_max_mb": None, "shuffle_read_median_mb": None,
        "spill_disk_mb": None,
    }
    return LegResult(
        case=case, leg=leg, rows=rows, seconds=round(seconds, 1),
        join=", ".join(join_strategies(plan)) or "none",
        stage_id=stage["stageId"] if stage else None,
        aqe_skew_split="yes" if skew_join_applied(plan) else "no",
        **stats,
    )


def case_01(spark, rows: int) -> list[LegResult]:
    from spark_applications.debugging import case_01_skewed_join as c

    return [
        measure_leg(spark, "01", "broken (SMJ, no AQE)", rows, c.BROKEN_CONF,
                    lambda: c.build_broken(spark, rows=rows)),
        measure_leg(spark, "01", "fixed (broadcast)", rows, c.FIXED_CONF,
                    lambda: c.build_fixed(spark, rows=rows)),
        measure_leg(spark, "01", "AQE only", rows, c.AQE_CONF,
                    lambda: c.build_pipeline(spark, rows=rows)),
    ]


def case_07(spark, rows: int) -> list[LegResult]:
    from spark_applications.debugging import (
        case_07_python_udf_to_pandas_udf as c,
    )

    return [
        measure_leg(spark, "07", "Python UDF", rows, {},
                    lambda: c.build_broken(spark, rows=rows)),
        measure_leg(spark, "07", "pandas UDF", rows, {},
                    lambda: c.build_fixed(spark, rows=rows)),
        measure_leg(spark, "07", "native", rows, {},
                    lambda: c.build_native(spark, rows=rows)),
    ]


def production(spark, rows: int) -> list[LegResult]:
    from spark_applications.debugging.production_metrics import (
        FAILURE_CONF,
        NO_BROADCAST_CONF,
        PRODUCTION_CONF,
    )

    events = fixtures.impression_events(
        spark, rows=rows, distinct_users=5_000, skew_ratio=0.8
    )
    users = fixtures.user_dimension(spark, distinct_users=5_000)
    extra = [c for c in users.columns if c != USER_KEY]
    return [
        measure_leg(spark, "prod", "before (plain join, SMJ)", rows,
                    FAILURE_CONF,
                    lambda: aggregate_impressions(
                        events.join(users, on=USER_KEY, how="left"), extra)),
        measure_leg(spark, "prod", "broadcast", rows, PRODUCTION_CONF,
                    lambda: aggregate_impressions(
                        enrich_with_users(events, users, "broadcast"),
                        extra)),
        measure_leg(spark, "prod", "salted", rows, NO_BROADCAST_CONF,
                    lambda: aggregate_impressions(
                        enrich_with_users(events, users, "salted"), extra)),
    ]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--case", choices=["1", "7"], action="append",
                        default=[])
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--rows", type=int, default=20_000_000)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = SparkSession.builder.appName("ClusterMeasure").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    results: list[LegResult] = []
    try:
        for case in args.case:
            results += case_01(spark, args.rows) if case == "1" else \
                case_07(spark, args.rows)
        if args.production:
            results += production(spark, args.rows)
        env = spark.sparkContext.getConf()
        print(
            f"\nCluster measurement — {args.rows:,} rows, "
            f"master={env.get('spark.master')}, "
            f"executors={env.get('spark.executor.instances', 'dynamic')}, "
            f"executor.memory={env.get('spark.executor.memory', 'default')}, "
            "shuffle.partitions="
            f"{spark.conf.get('spark.sql.shuffle.partitions')}\n"
        )
        print(HEADER)
        for r in results:
            print(r.row())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
