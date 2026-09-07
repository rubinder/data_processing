"""Plan-level before/after metrics for the production aggregation job.

Case 01 was diagnosed on a synthetic pipeline. This runs the *real*
``aggregation.enrich_with_users`` + ``aggregate_impressions`` path under four
configurations and reports the plan shape (join strategy, shuffle count and
partitioning) plus a local wall-clock time for each:

    baseline    aggregation with no user enrichment (what shipped before)
    before      enrichment as a plain join with the case-01 failure
                conditions (no broadcast available, AQE off): the
                sort-merge join on the skewed user_id
    broadcast   enrichment as shipped: broadcast hint + tuned session conf
    salted      enrichment via ``--join-strategy salted`` with broadcast
                unavailable

The plan-level numbers (exchanges, join operator, partitioning keys) are
exact and hold at any scale. The timings are ``local[*]`` and therefore only
directionally meaningful: they understate network shuffle cost and per-row
serialization, and the partitions stay under AQE's skew threshold. The
cluster re-measurement protocol is in ``CLUSTER_RUN.md``; this script is what
it runs, with ``--rows`` raised.

    uv run python -m spark_applications.debugging.production_metrics
    uv run python -m spark_applications.debugging.production_metrics \\
        --rows 2000000
"""

import argparse
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from spark_applications.aggregation import (
    USER_KEY,
    aggregate_impressions,
    enrich_with_users,
)
from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_final_plan,
    exchange_count,
    exchange_partitionings,
    join_strategies,
    skew_join_applied,
)
from spark_applications.debugging.session import (
    apply_conf,
    get_debug_session,
    restore_conf,
)
from spark_applications.utils.session import _COMMON_CONF

# Only the runtime-settable keys of the production session conf; the rest
# (packages, extensions) need a fresh JVM and do not affect plan shape.
PRODUCTION_CONF = {
    k: v for k, v in _COMMON_CONF.items()
    if k.startswith("spark.sql.")
}

# Case 01's broken conditions: Spark cannot broadcast and AQE cannot help.
FAILURE_CONF = {
    **PRODUCTION_CONF,
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.enabled": "false",
}

# Broadcast unavailable but AQE on — the environment in which salting is the
# right call (dimension genuinely too large to broadcast).
NO_BROADCAST_CONF = {
    **PRODUCTION_CONF,
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
}


@dataclass
class Measurement:
    scenario: str
    join: str
    exchanges: int
    partitionings: str
    skew_split: str
    rows_out: int
    seconds: float


def _events_and_users(spark: SparkSession, rows: int):
    events = fixtures.impression_events(
        spark, rows=rows, distinct_users=5_000, skew_ratio=0.8
    )
    users = fixtures.user_dimension(spark, distinct_users=5_000)
    return events, users


def _measure(spark, scenario: str, conf: dict, build) -> Measurement:
    previous = apply_conf(spark, conf)
    try:
        df: DataFrame = build()
        start = time.monotonic()
        rows_out = df.count()
        seconds = time.monotonic() - start
        plan = capture_final_plan(df)
    finally:
        restore_conf(spark, previous)

    partitionings = sorted(
        {p for p in exchange_partitionings(plan) if "salted_key" in p
         or USER_KEY in p}
    )
    return Measurement(
        scenario=scenario,
        join=", ".join(join_strategies(plan)) or "none",
        exchanges=exchange_count(plan),
        partitionings="; ".join(partitionings) or "none on the join key",
        skew_split="yes" if skew_join_applied(plan) else "no",
        rows_out=rows_out,
        seconds=round(seconds, 2),
    )


def measure_all(spark: SparkSession, rows: int) -> list[Measurement]:
    events, users = _events_and_users(spark, rows)
    extra = [c for c in users.columns if c != USER_KEY]

    return [
        _measure(
            spark, "baseline (no enrichment)", PRODUCTION_CONF,
            lambda: aggregate_impressions(events),
        ),
        _measure(
            spark, "before: plain join, case-01 conditions", FAILURE_CONF,
            lambda: aggregate_impressions(
                events.join(users, on=USER_KEY, how="left"), extra
            ),
        ),
        _measure(
            spark, "broadcast (shipped default)", PRODUCTION_CONF,
            lambda: aggregate_impressions(
                enrich_with_users(events, users, "broadcast"), extra
            ),
        ),
        _measure(
            spark, "salted (broadcast unavailable)", NO_BROADCAST_CONF,
            lambda: aggregate_impressions(
                enrich_with_users(events, users, "salted"), extra
            ),
        ),
    ]


def render(rows: int, measurements: list[Measurement]) -> str:
    lines = [
        f"Production aggregation — plan-level before/after ({rows:,} events, "
        f"80% on one user_id, local[*])",
        "",
        "| scenario | join | exchanges | shuffle on | AQE skew split | "
        "rows out | seconds |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for m in measurements:
        lines.append(
            f"| {m.scenario} | {m.join} | {m.exchanges} | {m.partitionings} "
            f"| {m.skew_split} | {m.rows_out:,} | {m.seconds} |"
        )
    lines += [
        "",
        "Timings are local[*] and directional only; see CLUSTER_RUN.md for "
        "the cluster protocol.",
    ]
    return "\n".join(lines)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--rows", type=int, default=200_000)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_debug_session("ProductionMetrics")
    try:
        print(render(args.rows, measure_all(spark, args.rows)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
