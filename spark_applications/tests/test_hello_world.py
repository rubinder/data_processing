"""Tests for hello_world job."""


def test_create_dataframe(spark):
    data = [
        ("hello", 1),
        ("world", 2),
        ("from", 3),
        ("spark", 4),
        ("applications", 5),
    ]
    df = spark.createDataFrame(data, ["word", "count"])

    assert df.count() == 5
    assert set(df.columns) == {"word", "count"}


def test_sum_aggregation(spark):
    data = [
        ("hello", 1),
        ("world", 2),
        ("from", 3),
        ("spark", 4),
        ("applications", 5),
    ]
    df = spark.createDataFrame(data, ["word", "count"])

    total = df.agg({"count": "sum"}).collect()[0][0]
    assert total == 15
