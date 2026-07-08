"""Data-quality helpers: schema-contract enforcement and reconciliation.

The point of these is to make bad or schema-drifting input *visible and
contained* instead of silently corrupting downstream tables. A row that does
not match the contract is routed to a quarantine location; the rest of the
batch still lands.
"""

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
