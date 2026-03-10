"""SparkSession builder helper."""

from pyspark.sql import SparkSession

from spark_applications.utils.mode import Mode


def get_spark_session(app_name: str, mode: Mode) -> SparkSession:
    """Build a SparkSession configured for the given mode."""
    builder = SparkSession.builder.appName(app_name)

    if mode == Mode.LOCAL:
        builder = (
            builder
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0",
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    elif mode == Mode.AWS:
        builder = (
            builder
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4",
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    elif mode == Mode.DATABRICKS:
        # On Databricks the session is pre-configured
        pass

    return builder.getOrCreate()
