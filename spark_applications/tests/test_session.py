"""Tests for the production SparkSession configuration (Tasks.md #2, #6).

The session builder cannot be exercised end-to-end in the test process (one
JVM session per process, already owned by the ``spark`` fixture), so these
pin the *configuration* the builder applies: the tuned join/skew thresholds
from debugging case 01 and the OpenLineage wiring, which is a pure function.
"""

from spark_applications.utils import session


def _bytes(value: str) -> int:
    units = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}
    value = value.lower().rstrip("b")
    return int(value[:-1]) * units[value[-1]]


def test_broadcast_threshold_raised_above_spark_default():
    conf = session._COMMON_CONF
    threshold = _bytes(conf["spark.sql.autoBroadcastJoinThreshold"])
    assert threshold > 10 * 1024 ** 2
    # AQE's runtime broadcast conversion uses its own threshold; keep them
    # aligned so the planner and AQE agree on what is "small".
    assert (
        conf["spark.sql.adaptive.autoBroadcastJoinThreshold"]
        == conf["spark.sql.autoBroadcastJoinThreshold"]
    )


def test_aqe_skew_split_triggers_below_the_256mb_default():
    """Case 01 measured that AQE split nothing because every partition sat
    under the 256MB default. Mid-sized jobs need a lower bar."""
    conf = session._COMMON_CONF
    skew = _bytes(
        conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]
    )
    advisory = _bytes(conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"])
    assert skew < 256 * 1024 ** 2
    # Spark requires the skew threshold >= the advisory size or splitting
    # produces partitions it immediately wants to coalesce again.
    assert skew >= advisory
    assert conf["spark.sql.adaptive.skewJoin.enabled"] == "true"


def test_parquet_codec_is_zstd():
    codec = session._COMMON_CONF["spark.sql.parquet.compression.codec"]
    assert codec == "zstd"


def test_openlineage_conf_disabled_without_url():
    assert session.openlineage_conf(url=None) == {}
    assert session.openlineage_conf(url="") == {}


def test_openlineage_conf_wires_listener_and_transport():
    conf = session.openlineage_conf(
        url="http://marquez:5000", namespace="ns", app_name="ApiPull",
    )
    assert conf["spark.extraListeners"] == (
        "io.openlineage.spark.agent.OpenLineageSparkListener"
    )
    assert conf["spark.openlineage.transport.type"] == "http"
    assert conf["spark.openlineage.transport.url"] == "http://marquez:5000"
    assert conf["spark.openlineage.namespace"] == "ns"
    assert conf["spark.openlineage.appName"] == "ApiPull"


def test_merge_packages_appends_without_duplicates():
    merged = session._merge_packages(
        "io.delta:delta-spark_2.12:3.2.0", [session.OPENLINEAGE_PACKAGE]
    )
    assert merged.split(",") == [
        "io.delta:delta-spark_2.12:3.2.0", session.OPENLINEAGE_PACKAGE
    ]
    again = session._merge_packages(merged, [session.OPENLINEAGE_PACKAGE])
    assert again == merged
    assert session._merge_packages("", [session.OPENLINEAGE_PACKAGE]) == (
        session.OPENLINEAGE_PACKAGE
    )
