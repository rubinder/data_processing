"""Data-quality helpers: schema-contract enforcement and reconciliation.

The point of these is to make bad or schema-drifting input *visible and
contained* instead of silently corrupting downstream tables. A row that does
not match the contract is routed to a quarantine location; the rest of the
batch still lands.
"""

import statistics
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from spark_applications.utils.schema import CORRUPT_RECORD_COL


@dataclass
class QualitySplit:
    """Result of splitting a batch into conforming vs. quarantined rows."""

    valid: DataFrame
    quarantined: DataFrame
    total: int


def _conforms_predicate(df: DataFrame, required_cols: list[str]) -> Column:
    """Build a predicate that is true for rows that satisfy the contract.

    A row conforms when Spark parsed it cleanly (no corrupt-record marker)
    and every required column is non-null.
    """
    predicate = F.col(CORRUPT_RECORD_COL).isNull()
    for col in required_cols:
        predicate = predicate & F.col(col).isNotNull()
    return predicate


def split_on_contract(
    df: DataFrame, required_cols: list[str]
) -> QualitySplit:
    """Split a DataFrame read in PERMISSIVE mode into valid and quarantined.

    ``df`` must have been read with the corrupt-record column present (see
    ``schema.schema_with_corrupt_column``). The split is cached because both
    sides are consumed (one written to the table, one to quarantine) and we do
    not want to re-read/re-parse the source twice.
    """
    df = df.cache()
    total = df.count()
    conforms = _conforms_predicate(df, required_cols)
    valid = df.filter(conforms).drop(CORRUPT_RECORD_COL)
    quarantined = df.filter(~conforms)
    return QualitySplit(valid=valid, quarantined=quarantined, total=total)


def deduplicate(
    df: DataFrame, keys: list[str], order_by: str
) -> DataFrame:
    """Keep one row per ``keys``, the latest by ``order_by`` (desc).

    CDC and at-least-once delivery mean the same logical record can arrive
    more than once; collapsing to the latest version per key makes downstream
    processing idempotent regardless of redelivery.
    """
    window = Window.partitionBy(*keys).orderBy(F.col(order_by).desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def reconcile_counts(
    expected: int, actual: int, *, label: str, tolerance: float = 0.0
) -> None:
    """Raise if ``actual`` deviates from ``expected`` beyond ``tolerance``.

    Used to assert that no rows were silently dropped between stages
    (e.g. raw landed vs. rows written). ``tolerance`` is a fraction
    (0.0 = exact match required).
    """
    if expected == 0:
        if actual != 0:
            raise ValueError(
                f"{label}: expected 0 rows but found {actual}"
            )
        return
    drift = abs(expected - actual) / expected
    if drift > tolerance:
        raise ValueError(
            f"{label}: row-count reconciliation failed — expected "
            f"{expected}, got {actual} (drift {drift:.2%} > "
            f"tolerance {tolerance:.2%})"
        )


# --- volume / anomaly checks -------------------------------------------------

DEFAULT_MIN_VOLUME_RATIO = 0.5
DEFAULT_MAX_VOLUME_RATIO = 2.0
DEFAULT_MAX_QUARANTINE_RATIO = 0.01


@dataclass
class VolumeCheck:
    """Outcome of comparing this run's row count to its history.

    ``status`` is one of ``ok``, ``anomaly`` or ``no_baseline``. The check
    never raises on its own: an unusually small hour may be legitimate (a
    holiday, an upstream outage that is somebody else's incident), so the
    caller decides whether an anomaly is a warning or a failure.
    """

    status: str
    current: int
    baseline: float | None
    ratio: float | None
    reason: str | None = None

    def as_fields(self) -> dict:
        return {
            "volume_status": self.status,
            "volume_current": self.current,
            "volume_baseline": self.baseline,
            "volume_ratio": self.ratio,
            "volume_reason": self.reason,
        }


def check_volume(
    current: int,
    baselines: list[int],
    *,
    min_ratio: float = DEFAULT_MIN_VOLUME_RATIO,
    max_ratio: float = DEFAULT_MAX_VOLUME_RATIO,
) -> VolumeCheck:
    """Compare ``current`` rows to the median of ``baselines``.

    Baselines are the same slice on previous days (same page_type and hour),
    so the comparison is like-for-like across the daily cycle. The median,
    not the mean, so one bad day in the history (an earlier outage that
    landed zero rows) does not drag the expectation down and mask a repeat.

    Zero rows is always an anomaly: a source that returned nothing needs a
    human even when there is no history to compare against.
    """
    if current == 0:
        return VolumeCheck(
            status="anomaly", current=0, baseline=None, ratio=None,
            reason="zero rows landed",
        )
    if not baselines:
        return VolumeCheck(
            status="no_baseline", current=current, baseline=None, ratio=None,
        )

    baseline = float(statistics.median(baselines))
    if baseline == 0:
        return VolumeCheck(
            status="no_baseline", current=current, baseline=0.0, ratio=None,
            reason="baseline median is zero",
        )

    ratio = current / baseline
    if ratio < min_ratio:
        reason = (
            f"{current} rows is {ratio:.0%} of the baseline {baseline:.0f}, "
            f"below the {min_ratio:.0%} floor"
        )
        return VolumeCheck("anomaly", current, baseline, ratio, reason)
    if ratio > max_ratio:
        reason = (
            f"{current} rows is {ratio:.0%} of the baseline {baseline:.0f}, "
            f"above the {max_ratio:.0%} ceiling"
        )
        return VolumeCheck("anomaly", current, baseline, ratio, reason)
    return VolumeCheck("ok", current, baseline, ratio)


def check_quarantine_ratio(
    rows_read: int,
    rows_quarantined: int,
    *,
    max_ratio: float = DEFAULT_MAX_QUARANTINE_RATIO,
) -> None:
    """Raise if too large a share of the batch failed the contract.

    A handful of malformed rows is the normal cost of doing business and the
    quarantine path exists to absorb them. A *large* share means the source
    changed shape (a renamed column, a new format) and the rest of the batch
    should not be trusted either — fail loudly rather than land 1% of an
    hour and call it done.
    """
    if rows_read == 0:
        return
    ratio = rows_quarantined / rows_read
    if ratio > max_ratio:
        raise ValueError(
            f"quarantine ratio {ratio:.2%} ({rows_quarantined} of "
            f"{rows_read} rows) exceeds the {max_ratio:.2%} threshold — "
            "the source has probably changed shape; not landing this batch"
        )
