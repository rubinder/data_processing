"""SparkSession configured for Apache Iceberg.

Two catalog modes, because they answer different questions:

``hadoop``  a filesystem catalog rooted at a local directory. No services, no
            network. Used by the tests and by anyone who wants to see Iceberg
            behave without standing anything up. Its weakness is real and
            worth knowing: a filesystem catalog relies on atomic rename for
            commits, which object stores do not guarantee, so it is a
            development catalog and not a production one.
``rest``    the Iceberg REST catalog against S3/MinIO, which is what
            docker-compose.yaml starts. The REST catalog owns commit
            atomicity, so it works correctly on object storage and is the
            shape a real deployment takes.

Both are driven from the same environment variables so a job body never has to
know which one it is running against.
"""
import os
import sys

#: Iceberg runtime matching Spark 3.5 / Scala 2.12, per the repo's Spark
#: version. The Spark major version is baked into the artifact name, so this
#: has to move in lockstep with any Spark upgrade.
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"

#: Needed only for the REST/S3 mode; pulled in separately so the local mode
#: does not download AWS jars it will never use.
S3_PACKAGES = (
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

CATALOG = os.getenv("ICEBERG_CATALOG", "local")
EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def get_spark_session(app_name: str, catalog_type: str | None = None,
                      warehouse: str | None = None):
    """Build a SparkSession with an Iceberg catalog registered.

    The Iceberg SQL extensions are what enable the statements this module is
    built to demonstrate -- ``ALTER TABLE ... RENAME COLUMN``, ``MERGE INTO``,
    ``CALL ... rollback_to_snapshot``, ``ADD PARTITION FIELD``. Without the
    extensions those parse as plain Spark SQL and either fail or silently do
    something different.
    """
    from pyspark.sql import SparkSession

    # Spark launches Python workers with whatever `python3` is on PATH, which
    # inside a virtualenv is usually NOT the interpreter running the driver.
    # The mismatch surfaces at first shuffle as PYTHON_VERSION_MISMATCH, long
    # after the session started successfully. Pinning both to sys.executable
    # makes the venv the single source of truth.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    catalog_type = catalog_type or os.getenv("ICEBERG_CATALOG_TYPE", "hadoop")
    warehouse = warehouse or os.getenv(
        "ICEBERG_WAREHOUSE", os.path.join(os.getcwd(), "iceberg-warehouse")
    )

    packages = ICEBERG_RUNTIME
    if catalog_type == "rest":
        packages = f"{ICEBERG_RUNTIME},{S3_PACKAGES}"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        # Makes `USE local` / unqualified names resolve to the Iceberg catalog
        # instead of Spark's built-in one.
        .config("spark.sql.defaultCatalog", CATALOG)
        # Iceberg writes are atomic per commit, so a failed job leaves no
        # partial data; this keeps Spark from also trying to manage output
        # committers itself.
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources."
                "SQLHadoopMapReduceCommitProtocol")
    )

    if catalog_type == "rest":
        builder = (
            builder
            .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
            .config(f"spark.sql.catalog.{CATALOG}.uri",
                    os.getenv("ICEBERG_REST_URI", "http://localhost:8181"))
            .config(f"spark.sql.catalog.{CATALOG}.warehouse",
                    os.getenv("ICEBERG_S3_WAREHOUSE", "s3://warehouse/"))
            .config(f"spark.sql.catalog.{CATALOG}.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint",
                    os.getenv("AWS_S3_ENDPOINT", "http://localhost:9100"))
            .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        )
    else:
        builder = (
            builder
            .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        )

    if os.getenv("SPARK_MASTER"):
        builder = builder.master(os.environ["SPARK_MASTER"])
    else:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    return spark
