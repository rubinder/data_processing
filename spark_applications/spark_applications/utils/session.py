"""SparkSession builder helper."""

from pyspark.sql import SparkSession

from spark_applications.utils.mode import Mode


# Engine-level defaults applied to the non-Databricks modes we build
# ourselves. (On Databricks the session is pre-configured by the platform.)
_COMMON_CONF = {
    # Scope "overwrite" writes to the partitions present in the DataFrame so a
    # single (page_type, date, hour) re-run is idempotent and does not delete
    # the rest of the table.
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    # Treat partition path values as strings rather than letting Spark guess
    # their types from directory names (e.g. page_type=1 -> int). Deterministic
    # typing keeps the read contract stable across runs.
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
    # Let AQE coalesce shuffle partitions and handle skew at runtime.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
}


def get_spark_session(app_name: str, mode: Mode) -> SparkSession:
    """Build a SparkSession configured for the given mode."""
    builder = SparkSession.builder.appName(app_name)

    if mode != Mode.DATABRICKS:
        for key, value in _COMMON_CONF.items():
            builder = builder.config(key, value)

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
