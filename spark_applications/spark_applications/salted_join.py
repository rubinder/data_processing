"""Salted join for handling data skew.

Salting is the *last* resort in the order established by debugging case 01
(``debugging/case_01_skewed_join.py``): broadcast the small side first, let
AQE split skewed partitions second, and salt only when both sides are
genuinely too large to broadcast. It costs a second code path to maintain
and multiplies the small side by ``salt_range``; it earns that cost only when
a hot key would otherwise pin one task with most of the data.

The production entry point is :func:`salted_join`; ``aggregation.py`` calls
it when ``--join-strategy salted`` is requested. The three building blocks
below are kept public because the debugging cases and tests reference them.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SALT_RANGE = 10


def add_salt_column(
    df: DataFrame, key_col: str, salt_range: int = SALT_RANGE
) -> DataFrame:
    """Add a random salt column and a salted key to a DataFrame.

    Used on the large/skewed side of a join.
    """
    return (
        df
        .withColumn("salt", (F.rand() * salt_range).cast("int"))
        .withColumn(
            "salted_key",
            F.concat(F.col(key_col).cast("string"), F.lit("_"), F.col("salt")),
        )
    )


def explode_for_salt(
    df: DataFrame, key_col: str, salt_range: int = SALT_RANGE
) -> DataFrame:
    """Explode a small DataFrame to match all possible salt values.

    Used on the small/lookup side of a join.
    """
    return (
        df
        .withColumn(
            "salt",
            F.explode(F.array([F.lit(i) for i in range(salt_range)])),
        )
        .withColumn(
            "salted_key",
            F.concat(F.col(key_col).cast("string"), F.lit("_"), F.col("salt")),
        )
    )


def remove_salt_columns(df: DataFrame) -> DataFrame:
    """Remove salt and salted_key columns after join."""
    return df.drop("salt", "salted_key")


def salted_join(
    large: DataFrame,
    small: DataFrame,
    key_col: str,
    how: str = "inner",
    salt_range: int = SALT_RANGE,
) -> DataFrame:
    """Equi-join ``large`` to ``small`` on ``key_col`` with a salted key.

    A drop-in for ``large.join(small, on=key_col, how=how)``: the output has
    ``key_col`` once, followed by the remaining columns of ``large`` and then
    of ``small``, with no salt bookkeeping columns.

    The large side gets a random salt in ``[0, salt_range)``; the small side
    is exploded to every salt value so each salted key still finds its match.
    The shuffle therefore partitions on ``salted_key``, spreading a hot key
    over ``salt_range`` partitions instead of one. ``how`` may be ``inner``
    or ``left`` (left-preserving on ``large``); right/full joins would need
    the explode on the other side and are not supported.
    """
    if how not in ("inner", "left", "left_outer", "leftouter"):
        raise ValueError(
            f"salted_join supports inner/left joins, got how={how!r}"
        )

    salted_large = add_salt_column(large, key_col, salt_range)
    # Keep only salted_key + payload on the small side: its copy of the key
    # and salt would otherwise collide with the large side's after the join.
    small_payload = [c for c in small.columns if c != key_col]
    exploded_small = explode_for_salt(small, key_col, salt_range).select(
        "salted_key", *small_payload
    )

    joined = salted_large.join(exploded_small, on="salted_key", how=how)
    large_payload = [c for c in large.columns if c != key_col]
    return remove_salt_columns(joined).select(
        key_col, *large_payload, *small_payload
    )


def main():
    spark = SparkSession.builder \
        .appName("SaltedJoin") \
        .getOrCreate()

    # Create a large skewed DataFrame (id=1 appears ~80%)
    large_data = [(1, f"val_{i}") for i in range(40)] + \
                 [(2, f"val_{i}") for i in range(5)] + \
                 [(3, f"val_{i}") for i in range(5)]
    large_df = spark.createDataFrame(large_data, ["id", "value"])

    # Create a small lookup DataFrame
    small_data = [(1, "lookup_a"), (2, "lookup_b"), (3, "lookup_c")]
    small_df = spark.createDataFrame(small_data, ["id", "name"])

    print("=== Large DF (skewed, id=1 ~80%) ===")
    large_df.groupBy("id").count().show()

    # Perform salted join
    result = salted_join(large_df, small_df, "id")

    print("=== Salted Join Result ===")
    result.show(truncate=False)
    print(f"Result row count: {result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
