"""Tests for salted_join functions."""

from spark_applications.salted_join import (
    SALT_RANGE,
    add_salt_column,
    explode_for_salt,
    remove_salt_columns,
)


def test_add_salt_column(spark):
    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(data, ["id", "value"])

    result = add_salt_column(df, "id")

    assert "salt" in result.columns
    assert "salted_key" in result.columns
    assert result.count() == 3

    # Salt values should be within range
    salt_values = [row.salt for row in result.collect()]
    for val in salt_values:
        assert 0 <= val < SALT_RANGE


def test_explode_for_salt(spark):
    data = [(1, "lookup_a"), (2, "lookup_b")]
    df = spark.createDataFrame(data, ["id", "name"])

    result = explode_for_salt(df, "id")

    assert "salt" in result.columns
    assert "salted_key" in result.columns
    # Each row should be exploded to SALT_RANGE rows
    assert result.count() == 2 * SALT_RANGE


def test_remove_salt_columns(spark):
    data = [(1, "a", 5, "1_5")]
    df = spark.createDataFrame(
        data, ["id", "value", "salt", "salted_key"]
    )

    result = remove_salt_columns(df)

    assert "salt" not in result.columns
    assert "salted_key" not in result.columns
    assert set(result.columns) == {"id", "value"}


def test_salted_join_end_to_end(spark):
    # Large skewed DF
    large_data = [(1, f"val_{i}") for i in range(40)] + \
                 [(2, f"val_{i}") for i in range(5)] + \
                 [(3, f"val_{i}") for i in range(5)]
    large_df = spark.createDataFrame(large_data, ["id", "value"])

    # Small lookup DF
    small_data = [(1, "lookup_a"), (2, "lookup_b"), (3, "lookup_c")]
    small_df = spark.createDataFrame(small_data, ["id", "name"])

    salted_large = add_salt_column(large_df, "id")
    exploded_small = explode_for_salt(small_df, "id")

    joined = salted_large.join(
        exploded_small, on="salted_key", how="inner"
    )
    result = remove_salt_columns(joined)

    # All 50 rows from large_df should match
    assert result.count() == 50
