"""SparkSession builder helper."""

import os

from pyspark.sql import SparkSession

from spark_applications.utils.mode import Mode

# OpenLineage Spark integration (Scala 2.12 build, matches the Delta and
# hadoop-aws artifacts below). Only added to spark.jars.packages when
# OPENLINEAGE_URL is set, so a bare local run pulls nothing extra.
OPENLINEAGE_PACKAGE = "io.openlineage:openlineage-spark_2.12:1.53.0"
DEFAULT_LINEAGE_NAMESPACE = "data_processing"

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


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
    # --- Join / skew thresholds (debugging case 01) ------------------------
    # Broadcast first. The 10MB default is a guess about the *estimated* size
    # of the build side, and Spark's estimate for the output of a filter or
    # aggregation is routinely off by an order of magnitude, so a dimension
    # that would happily fit gets planned as a sort-merge join on the skewed
    # key. 64MB (estimated, on-disk) is roughly 200-250MB as an in-memory
    # hash relation per executor and a single driver-side collect — well
    # within the executor/driver sizes used here, and still an order of
    # magnitude below anything that should be salted instead. The AQE
    # variant governs runtime SMJ->BHJ conversion; keep them equal so the
    # planner and AQE agree on what "small" means.
    "spark.sql.autoBroadcastJoinThreshold": "64MB",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "64MB",
    # AQE second. Case 01 measured that AQE's skew split did nothing because
    # every partition sat under the 256MB default while still being 5x the
    # median. Lower the bar so mid-sized skew is actually split. Spark
    # requires skewedPartitionThresholdInBytes >= advisoryPartitionSizeInBytes
    # (the split target), so both move together. skewedPartitionFactor
    # stays at its default of 5x the median.
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    # --- Storage cost (aws_deployment/FINOPS.md) ---------------------------
    # zstd compresses parquet 20-30% smaller than snappy at a modest CPU cost
    # on write and near-parity on read. Every downstream scan (Athena bills
    # bytes scanned) and every S3 GB-month gets cheaper; the write is the
    # one-time cost.
    "spark.sql.parquet.compression.codec": "zstd",
}


def openlineage_conf(
    url: str | None,
    namespace: str = DEFAULT_LINEAGE_NAMESPACE,
    app_name: str | None = None,
) -> dict[str, str]:
    """Spark configs that make the job emit OpenLineage run events.

    Returns an empty mapping when ``url`` is unset so lineage is strictly
    opt-in. The listener reports every read/write dataset of the job (S3
    paths, Delta tables) to the OpenLineage backend (Marquez in
    ``lineage_deployment/``); Airflow's provider adds the parent-run facet
    that links the Spark run to the task that launched it.
    """
    if not url:
        return {}
    conf = {
        "spark.extraListeners": (
            "io.openlineage.spark.agent.OpenLineageSparkListener"
        ),
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": url,
        "spark.openlineage.namespace": namespace,
    }
    if app_name:
        conf["spark.openlineage.appName"] = app_name
    return conf


def _merge_packages(existing: str, extra: list[str]) -> str:
    """Append Maven coordinates to a ``spark.jars.packages`` value."""
    packages = [p for p in existing.split(",") if p]
    for package in extra:
        if package not in packages:
            packages.append(package)
    return ",".join(packages)


def get_spark_session(app_name: str, mode: Mode) -> SparkSession:
    """Build a SparkSession configured for the given mode."""
    builder = SparkSession.builder.appName(app_name)

    lineage = openlineage_conf(
        os.getenv("OPENLINEAGE_URL"),
        os.getenv("OPENLINEAGE_NAMESPACE", DEFAULT_LINEAGE_NAMESPACE),
        app_name,
    )
    lineage_packages = [OPENLINEAGE_PACKAGE] if lineage else []

    if mode != Mode.DATABRICKS:
        for key, value in _COMMON_CONF.items():
            builder = builder.config(key, value)
    for key, value in lineage.items():
        builder = builder.config(key, value)

    if mode == Mode.LOCAL:
        builder = (
            builder
            .master("local[*]")
            .config(
                "spark.jars.packages",
                _merge_packages(DELTA_PACKAGE, lineage_packages),
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
                _merge_packages(
                    f"{DELTA_PACKAGE},{HADOOP_AWS_PACKAGE}", lineage_packages
                ),
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
        # On Databricks the session is pre-configured. The OpenLineage jar
        # must be attached as a cluster library there (spark.jars.packages
        # is not honoured on an already-running cluster); the listener
        # configs above still apply.
        pass

    return builder.getOrCreate()
