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
