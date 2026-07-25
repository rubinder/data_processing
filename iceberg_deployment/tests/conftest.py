"""Shared fixtures: a real local Spark session with a real Iceberg catalog.

Nothing here is mocked. The tests create actual Iceberg tables on the local
filesystem, write actual Parquet, and read actual snapshot metadata, so a
passing test is evidence the table format behaves as the module claims.

Session-scoped because a SparkSession takes ~10 s to start; each test gets its
own table name instead of its own session.
"""
import os
import shutil
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _java_home() -> str | None:
    """Locate a JDK if JAVA_HOME is not already set.

    PySpark needs one and macOS does not export JAVA_HOME by default, which
    otherwise surfaces as an unhelpful "JAVA_HOME is not set" at session start.
    """
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for version in ("17", "11"):
        found = os.popen(f"/usr/libexec/java_home -v {version} 2>/dev/null").read()
        if found.strip():
            return found.strip()
    return None


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    java_home = _java_home()
    if not java_home:
        pytest.skip("no JDK found; PySpark cannot start")
    os.environ["JAVA_HOME"] = java_home

    from iceberg_deployment.session import get_spark_session

    warehouse = tempfile.mkdtemp(prefix="iceberg-tests-")
    session = get_spark_session(
        "iceberg-tests", catalog_type="hadoop", warehouse=warehouse
    )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def table(spark, request):
    """A fresh, seeded Iceberg table per test, named after the test."""
    from iceberg_deployment import impressions

    name = f"db.{request.node.name.replace('[', '_').replace(']', '')}"[:120]
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
    impressions.create_table(spark, name)
    impressions.seed(spark, name, count=60)
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")


@pytest.fixture
def empty_table(spark, request):
    """A fresh Iceberg table with no rows."""
    from iceberg_deployment import impressions

    name = f"db.e_{request.node.name.replace('[', '_').replace(']', '')}"[:120]
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
    impressions.create_table(spark, name)
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
