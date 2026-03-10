"""Salted join PySpark job for handling data skew."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SALT_RANGE = 10


def add_salt_column(df: DataFrame, key_col: str) -> DataFrame:
    """Add a random salt column and a salted key to a DataFrame.

    Used on the large/skewed side of a join.
    """
    return (
        df
        .withColumn("salt", (F.rand() * SALT_RANGE).cast("int"))
        .withColumn(
            "salted_key",
            F.concat(F.col(key_col).cast("string"), F.lit("_"), F.col("salt")),
        )
    )


def explode_for_salt(df: DataFrame, key_col: str) -> DataFrame:
    """Explode a small DataFrame to match all possible salt values.

    Used on the small/lookup side of a join.
    """
    return (
        df
        .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_RANGE)])))
        .withColumn(
            "salted_key",
            F.concat(F.col(key_col).cast("string"), F.lit("_"), F.col("salt")),
        )
    )


def remove_salt_columns(df: DataFrame) -> DataFrame:
    """Remove salt and salted_key columns after join."""
    return df.drop("salt", "salted_key")


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
    salted_large = add_salt_column(large_df, "id")
    exploded_small = explode_for_salt(small_df, "id")

    joined = salted_large.join(
        exploded_small,
        on="salted_key",
        how="inner",
    )

    result = remove_salt_columns(joined).select(
        salted_large["id"], "value", "name"
    )

    print("=== Salted Join Result ===")
    result.show(truncate=False)
    print(f"Result row count: {result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
