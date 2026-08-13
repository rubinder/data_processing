"""SparkSession builder for the debugging cases.

Separate from ``utils/session.py`` on purpose. The production session enables
AQE, adaptive skew-join handling and shuffle coalescing — which is correct,
and which also means several of the pathologies in this package **fix
themselves before you can look at them**. To show what the broken plan looks
like, the case has to be able to turn those off.

That is not a contrivance: AQE only kicks in at runtime and only within the
bounds you give it. Knowing what the plan looks like without it is how you
recognise the cases where it did not save you.
"""

from pyspark.sql import SparkSession

# Kept small so the cases run on a laptop in seconds. A real diagnosis would
# use the cluster's real partition count; the plan *shape* is the same.
DEFAULT_SHUFFLE_PARTITIONS = 8


def get_debug_session(
    app_name: str = "SparkDebugging",
    adaptive: bool = True,
    broadcast_threshold: str | None = None,
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build (or reconfigure) a local SparkSession for a debugging case.

    Args:
        app_name: Spark application name.
        adaptive: Enable AQE. Pass ``False`` to see the pre-AQE plan.
        broadcast_threshold: Value for
            ``spark.sql.autoBroadcastJoinThreshold``. ``"-1"`` disables
            broadcast joins, forcing a sort-merge join so the shuffle is
            visible. ``None`` leaves the Spark default (10MB).
        shuffle_partitions: ``spark.sql.shuffle.partitions``.
        extra_conf: Any additional key/value config for the case.

    Note:
        A JVM-level SparkSession cannot be rebuilt within one process, so when
        a session already exists this sets the SQL configs on it instead.
        Every config touched here is runtime-settable, which is why the cases
        can flip between broken and fixed in a single run.
    """
    conf = {
        "spark.sql.adaptive.enabled": str(adaptive).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(adaptive).lower(),
        "spark.sql.adaptive.skewJoin.enabled": str(adaptive).lower(),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        # Deterministic typing for partition path values, matching
        # utils/session.py so the pruning case behaves like the real jobs.
        "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }
    if broadcast_threshold is not None:
        conf["spark.sql.autoBroadcastJoinThreshold"] = broadcast_threshold
    if extra_conf:
        conf.update(extra_conf)

    existing = SparkSession.getActiveSession()
    if existing is not None:
        apply_conf(existing, conf)
        return existing

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    for key, value in conf.items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def apply_conf(spark: SparkSession, conf: dict[str, str]) -> dict[str, str]:
    """Set SQL configs on a live session, returning the previous values.

    The returned mapping is what :func:`restore_conf` needs to put the session
    back, so a case can break a setting without leaking it into the next case
    in the same run.
    """
    previous = {}
    for key, value in conf.items():
        try:
            previous[key] = spark.conf.get(key)
        except Exception:
            previous[key] = None
        spark.conf.set(key, value)
    return previous


def restore_conf(spark: SparkSession, previous: dict[str, str]) -> None:
    """Undo an :func:`apply_conf`, unsetting keys that had no prior value."""
    for key, value in previous.items():
        if value is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, value)
