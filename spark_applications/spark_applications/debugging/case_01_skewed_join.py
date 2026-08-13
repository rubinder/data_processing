"""Case 01 — a join where one task never finishes.

The classic skew symptom, and the one most often "fixed" by the wrong thing.
The instinct is to reach for a salted join. Usually the right answer is to
notice the small side is broadcastable and find out why Spark didn't broadcast
it.

Run: ``uv run python -m spark_applications.debugging.run --case 1``
"""

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    aqe_shuffle_reads,
    capture_final_plan,
    capture_plan,
    exchange_count,
    is_final_plan,
    join_strategies,
    skew_join_applied,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import (
    apply_conf,
    get_debug_session,
    restore_conf,
)

CASE_ID = "01"
TITLE = "Skewed join — 199 of 200 tasks finish in seconds"

SYMPTOM = """
The stage sits at 199/200 tasks for forty minutes. The Spark UI shows the
straggler's shuffle-read bytes are ~200x the median, and it eventually dies:

    ExecutorLostFailure (executor 7 exited caused by one of the running tasks)
    Reason: Container killed by YARN for exceeding memory limits.
    22.4 GB of 22 GB physical memory used.

Restarting with more executor memory buys one more retry, then fails again.
More memory is not the fix — the data is not evenly divided.
"""

CAUSE = """
80% of impression rows carry a single user_id (a bot / internal load-test
account). The join shuffles on user_id, so hash partitioning puts all of those
rows in one partition. One task therefore gets 80% of the data, and no amount
of parallelism or executor memory changes that — partitioning by a key can
never be more granular than the key's distribution.

The plan says this before you run anything: a SortMergeJoin means *both* sides
are shuffled on the join key, and the join key is the skewed column.
"""

RESOLUTION = """
Broadcast the small side. The user dimension is a few thousand rows; once it
is broadcast there is no shuffle on the join key at all, so the skew becomes
irrelevant rather than merely tolerable. In the plan SortMergeJoin (with its
two Exchange nodes) collapses to BroadcastHashJoin.

Spark broadcasts automatically when it believes a side is under
spark.sql.autoBroadcastJoinThreshold (10MB default). When it doesn't, the
usual reason is that it cannot size the side: no table statistics, or the
small side is itself the output of a join/aggregation whose row count Spark
has to guess. Fix the estimate (ANALYZE TABLE / cache / a checkpoint) or force
it with a broadcast() hint.
"""

NOTES = [
    "Try broadcast first, AQE second, salting last. Salting doubles the "
    "code you have to maintain and explodes the small side by the salt "
    "factor; it earns that cost only when both sides are genuinely large.",

    "AQE's skew-join handling (spark.sql.adaptive.skewJoin.enabled, on in "
    "utils/session.py) splits an oversized partition into sub-partitions at "
    "runtime. It only triggers above BOTH skewedPartitionFactor (5x median) "
    "and skewedPartitionThresholdInBytes (256MB default). A partition that "
    "is badly skewed but under 256MB gets no help — a common reason AQE "
    "'does nothing' on a mid-sized job.",

    "AQE rewrites are invisible to df.explain(). You must read the executed "
    "plan after an action to see AQEShuffleRead / isFinalPlan=true. See the "
    "MEASURED block above and the note in explain_tools.capture_final_plan.",

    "Broadcast is not free: the driver collects the small side, so a "
    "broadcast that is too large moves the OOM from an executor to the "
    "driver. That failure looks completely different — see case 02.",

    "If both sides really are large, salt it: spark_applications/"
    "salted_join.py has the add_salt / explode / remove_salt pattern.",

    "Check for a degenerate key before anything else: a null or sentinel "
    "value ('', 'unknown', -1) in the join column produces the same "
    "single-partition pileup and is usually a data bug, not a tuning "
    "problem. Filter nulls out before the join.",
]

# Broadcast disabled, AQE off: forces the sort-merge join so the shuffle and
# its skew are visible in the plan rather than quietly handled at runtime.
BROKEN_CONF = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.enabled": "false",
}

# Broadcast re-enabled at the 10MB default.
FIXED_CONF = {
    "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    "spark.sql.adaptive.enabled": "false",
}

# AQE on, broadcast still off: shows what AQE does to the skewed sort-merge
# join when broadcasting is not an option.
AQE_CONF = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}


def build_pipeline(spark, rows: int = 200_000, skew_ratio: float = 0.8):
    """The join under investigation: impressions enriched with user segment."""
    events = fixtures.impression_events(
        spark, rows=rows, distinct_users=2_000, skew_ratio=skew_ratio
    )
    users = fixtures.user_dimension(spark, distinct_users=2_000)
    return (
        events.join(users, on="user_id", how="inner")
        .groupBy("segment", "page_type")
        .count()
    )


def build_broken(spark, **kwargs):
    """The pipeline as it plans with broadcasting unavailable."""
    return build_pipeline(spark, **kwargs)


def build_fixed(spark, **kwargs):
    """Identical code — only the broadcast threshold differs."""
    return build_pipeline(spark, **kwargs)


def diagnose(spark, rows: int = 200_000) -> Diagnosis:
    """Capture the broken and fixed plans plus the post-AQE final plan."""
    previous = apply_conf(spark, BROKEN_CONF)
    try:
        broken_plan = capture_plan(build_broken(spark, rows=rows))
    finally:
        restore_conf(spark, previous)

    previous = apply_conf(spark, FIXED_CONF)
    try:
        fixed_plan = capture_plan(build_fixed(spark, rows=rows))
    finally:
        restore_conf(spark, previous)

    # The third path: no broadcast available, AQE left to deal with it. The
    # rewrites only appear in the *executed* plan.
    previous = apply_conf(spark, AQE_CONF)
    try:
        aqe_df = build_pipeline(spark, rows=rows)
        aqe_initial = capture_plan(aqe_df, mode="simple")
        aqe_final = capture_final_plan(aqe_df)
    finally:
        restore_conf(spark, previous)

    reads = aqe_shuffle_reads(aqe_final)

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
                look_for="Join operator",
                broken=", ".join(join_strategies(broken_plan)),
                fixed=", ".join(join_strategies(fixed_plan)),
            ),
            Evidence(
                look_for="Exchange (shuffle) nodes",
                broken=str(exchange_count(broken_plan)),
                fixed=str(exchange_count(fixed_plan)),
            ),
        ],
        metrics={
            "AQE path: explain() reports": (
                "isFinalPlan=true" if is_final_plan(aqe_initial)
                else "initial plan only — no AQE rewrites visible"
            ),
            "AQE path: executed plan reports": (
                "isFinalPlan=true" if is_final_plan(aqe_final)
                else "isFinalPlan=false"
            ),
            "AQE path: AQEShuffleRead nodes": ", ".join(reads) or "none",
            "AQE path: skewed partition split": (
                "yes" if skew_join_applied(aqe_final) else
                "no — partitions are under skewedPartitionThresholdInBytes "
                "(256MB); AQE coalesced but did not split. This is the "
                "common case on mid-sized jobs and is why AQE alone is not "
                "a skew strategy."
            ),
        },
        notes=NOTES,
    )


def main():
    spark = get_debug_session("Debug-01-SkewedJoin")
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
