"""Tests for aggregation job."""

import os

from spark_applications.aggregation import aggregate_impressions


def test_aggregate_impressions(spark):
    data = [
        ("user_1", "imp_1", "1", "2026-01-01", 10, 30, 1, "a"),
        ("user_1", "imp_1", "1", "2026-01-01", 10, 30, 2, "b"),
        ("user_1", "imp_1", "1", "2026-01-01", 10, 30, 3, "c"),
        ("user_2", "imp_2", "1", "2026-01-01", 10, 31, 5, "a"),
        ("user_2", "imp_2", "1", "2026-01-01", 10, 31, 8, "b"),
        ("user_3", "imp_3", "2", "2026-01-01", 10, 32, 10, "a"),
    ]
    columns = [
        "user_id", "impression_id", "page_type",
        "date", "hour", "min", "second", "event_type",
    ]
    df = spark.createDataFrame(data, columns)

    result = aggregate_impressions(df)
    rows = {
        row.impression_id: row for row in result.collect()
    }

    assert len(rows) == 3

    # user_1/imp_1: 3 events, 3 distinct, seconds 1-3
    assert rows["imp_1"].event_count == 3
    assert rows["imp_1"].distinct_events == 3
    assert rows["imp_1"].first_event_second == 1
    assert rows["imp_1"].last_event_second == 3

    # user_2/imp_2: 2 events, 2 distinct, seconds 5-8
    assert rows["imp_2"].event_count == 2
    assert rows["imp_2"].distinct_events == 2
    assert rows["imp_2"].first_event_second == 5
    assert rows["imp_2"].last_event_second == 8

    # user_3/imp_3: 1 event, 1 distinct, second 10
    assert rows["imp_3"].event_count == 1
    assert rows["imp_3"].distinct_events == 1
    assert rows["imp_3"].first_event_second == 10
    assert rows["imp_3"].last_event_second == 10


def test_aggregation_with_partitioned_data(spark, tmp_path):
    """Test reading from parquet and aggregating."""
    data = [
        ("user_1", "imp_1", "1", "2026-01-01", 10, 30, 1, "a"),
        ("user_1", "imp_1", "1", "2026-01-01", 10, 30, 5, "b"),
        ("user_2", "imp_2", "2", "2026-01-01", 10, 31, 2, "a"),
    ]
    columns = [
        "user_id", "impression_id", "page_type",
        "date", "hour", "min", "second", "event_type",
    ]
    df = spark.createDataFrame(data, columns)

    # Write to parquet
    output_path = os.path.join(str(tmp_path), "impressions")
    df.write.mode("overwrite").partitionBy("page_type").parquet(output_path)

    # Read back and aggregate
    read_df = spark.read.parquet(output_path)
    result = aggregate_impressions(read_df)

    assert result.count() == 2


# --- user-dimension enrichment: the hot-key join path (Tasks.md #2) ---------

from pyspark.sql import functions as F  # noqa: E402

from spark_applications.aggregation import enrich_with_users  # noqa: E402
from spark_applications.debugging import fixtures  # noqa: E402
from spark_applications.debugging.explain_tools import (  # noqa: E402
    capture_plan,
    join_strategies,
)
from spark_applications.debugging.session import (  # noqa: E402
    apply_conf,
    restore_conf,
)


def _skewed_events(spark):
    # 80% of rows on one user_id — the shape that case 01 is about.
    return fixtures.impression_events(
        spark, rows=3_000, distinct_users=50, skew_ratio=0.8
    )


def test_enrich_broadcast_is_default_and_plans_a_broadcast_join(spark):
    """Case 01's conclusion: broadcast first. The default strategy must
    produce a BroadcastHashJoin, so the skewed key is never shuffled."""
    events = _skewed_events(spark)
    users = fixtures.user_dimension(spark, distinct_users=50)

    enriched = enrich_with_users(events, users)

    strategies = join_strategies(capture_plan(enriched))
    assert strategies == ["BroadcastHashJoin"]
    assert "segment" in enriched.columns
    assert "country" in enriched.columns
    # Left join: every event survives even if its user is unknown.
    assert enriched.count() == events.count()


def test_enrich_broadcast_hint_survives_broadcast_being_disabled(spark):
    """The hint forces the broadcast even when Spark would decline (no
    stats, or autoBroadcastJoinThreshold=-1) — that is the whole point of
    hinting rather than relying on the estimate."""
    events = _skewed_events(spark)
    users = fixtures.user_dimension(spark, distinct_users=50)

    previous = apply_conf(spark, {
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    })
    try:
        plan = capture_plan(enrich_with_users(events, users))
    finally:
        restore_conf(spark, previous)

    assert join_strategies(plan) == ["BroadcastHashJoin"]


def test_enrich_salted_shuffles_on_the_salted_key(spark):
    """Salting last: when both sides are genuinely large the join key is
    replaced by ``salted_key`` so the hot user_id is spread over
    ``salt_range`` partitions instead of landing in one."""
    events = _skewed_events(spark)
    users = fixtures.user_dimension(spark, distinct_users=50)

    previous = apply_conf(spark, {
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.enabled": "false",
    })
    try:
        salted = enrich_with_users(events, users, strategy="salted")
        plan = capture_plan(salted)
    finally:
        restore_conf(spark, previous)

    assert "BroadcastHashJoin" not in join_strategies(plan)
    assert "salted_key" in plan
    # No salt bookkeeping leaks into the enriched frame.
    assert "salt" not in salted.columns
    assert "salted_key" not in salted.columns
    assert salted.columns.count("user_id") == 1


def test_enrich_strategies_produce_identical_aggregates(spark):
    """Whichever join strategy is chosen, the business result is the same."""
    events = _skewed_events(spark)
    users = fixtures.user_dimension(spark, distinct_users=50)

    by_broadcast = aggregate_impressions(
        enrich_with_users(events, users, strategy="broadcast"),
        extra_group_cols=["segment"],
    )
    by_salt = aggregate_impressions(
        enrich_with_users(events, users, strategy="salted"),
        extra_group_cols=["segment"],
    )

    assert "segment" in by_broadcast.columns
    assert sorted(by_broadcast.collect()) == sorted(by_salt.collect())
    # The hot user's rows all carry its segment from the dimension.
    hot = by_broadcast.filter(F.col("user_id") == fixtures.HOT_USER_ID)
    assert hot.count() > 0
    assert {r.segment for r in hot.collect()} == {"enterprise"}


def test_enrich_rejects_unknown_strategy(spark):
    import pytest

    events = _skewed_events(spark)
    users = fixtures.user_dimension(spark, distinct_users=50)
    with pytest.raises(ValueError, match="strategy"):
        enrich_with_users(events, users, strategy="magic")


def test_parse_args_accepts_user_dimension_and_strategy():
    from spark_applications.aggregation import parse_args

    args = parse_args([
        "--mode", "local", "--date", "2026-01-01", "--hour", "10",
    ])
    assert args.user_dimension is None
    assert args.join_strategy == "broadcast"

    args = parse_args([
        "--mode", "local", "--date", "2026-01-01", "--hour", "10",
        "--user-dimension", "users", "--join-strategy", "salted",
    ])
    assert args.user_dimension == "users"
    assert args.join_strategy == "salted"
