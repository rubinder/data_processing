"""Shared test fixtures."""

import os

import pytest
from pyspark.sql import SparkSession

# PySpark 3.5 on Java 17+ requires this flag
os.environ.setdefault(
    "SPARK_SUBMIT_OPTS",
    "--add-exports java.base/javax.security.auth=ALL-UNNAMED "
    "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-exports java.base/sun.security.action=ALL-UNNAMED "
    "-Djava.security.manager=allow",
)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--master local[2] pyspark-shell")


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for tests."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-exports java.base/javax.security.auth=ALL-UNNAMED "
            "--add-exports java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-exports java.base/sun.security.action=ALL-UNNAMED "
            "-Djava.security.manager=allow",
        )
        .getOrCreate()
    )
    yield session
    session.stop()
