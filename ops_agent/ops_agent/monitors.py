"""Monitor definitions and verdict rules.

A monitor is a SQL query returning one column named ``metric``, plus a rule
for judging that number against its own history. Definitions live in
``feeds/*.yaml``; this module owns the *shape* of a monitor and the rules.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class MonitorDef:
    name: str
    table: str
    kind: str
    query: str
    column: str | None = None
    severity: str = "additive"
    params: dict | None = None
    # Extra table aliases beyond the implicit ``t`` -> ``table``.
    tables: dict[str, str] | None = None
    # "sql": ``query`` runs through ``engine.sql()``. "snapshot_added": the
    # metric is the rows added by the latest non-rebuild snapshot, for
    # append-only tables where cumulative ``count(*)`` cannot see a collapse.
    source: str = "sql"


@dataclass(frozen=True)
class MonitorResult:
    monitor: str
    table: str
    column: str | None
    kind: str
    metric: float | None
    baseline: float | None
    status: str          # ok | warn | breach
    detail: str
    run_at: date


KINDS = frozenset({"row_count", "cardinality", "distribution_shift",
                   "null_rate", "duplicate_rate"})


def evaluate(metric: float, baseline: list[float], params: dict) -> tuple[str, str]:
    """Judge a metric against its own history. Returns ``(status, detail)``."""
    kind = params.get("kind", "row_count")

    # Checked first so an unrecognised kind cannot slip through on a
    # monitor's first run and become a permanently green check.
    if kind not in KINDS:
        raise ValueError(f"unknown monitor kind: {kind!r} (expected one of "
                         f"{', '.join(sorted(KINDS))})")

    absolute = params.get("max_absolute")
    if absolute is not None and metric > absolute:
        return "breach", f"{metric:g} exceeds absolute limit {absolute:g}"

    if not baseline:
        # A monitor that alarms on its own first execution never gets trusted.
        return "ok", "no baseline yet — recorded for future comparison"

    median = statistics.median(baseline)

    if kind in {"row_count", "cardinality"}:
        if median <= 0:
            return "ok", "baseline median is zero"
        ratio = metric / median
        if ratio < params.get("breach_ratio", 0.5):
            return "breach", f"{metric:g} is {ratio:.0%} of trailing median {median:g}"
        if ratio < params.get("warn_ratio", 0.8):
            return "warn", f"{metric:g} is {ratio:.0%} of trailing median {median:g}"
        return "ok", f"{metric:g} vs median {median:g}"

    if kind in {"distribution_shift", "null_rate"}:
        # Median absolute deviation, not stddev: one prior outlier widens a
        # stddev band enough to hide the next one.
        deviations = [abs(v - median) for v in baseline]
        mad = statistics.median(deviations)
        if mad == 0:
            mad = statistics.mean(deviations)
        if mad == 0:
            # Genuinely constant history: judge by near-equality, because a
            # metric recomputed from unchanged data still carries float noise.
            if math.isclose(metric, median, rel_tol=1e-9, abs_tol=1e-9):
                return "ok", f"{metric:g} matches constant baseline {median:g}"
            return "breach", f"{metric:g} differs from constant baseline {median:g}"
        robust_z = abs(metric - median) / (1.4826 * mad)
        if robust_z > params.get("breach_z", 4.0):
            return "breach", f"{metric:g} is {robust_z:.1f} robust-z from median {median:g}"
        if robust_z > params.get("warn_z", 3.0):
            return "warn", f"{metric:g} is {robust_z:.1f} robust-z from median {median:g}"
        return "ok", f"{metric:g} within {robust_z:.1f} robust-z"

    if kind == "duplicate_rate":
        return ("ok", f"{metric:g} duplicates") if metric == 0 else (
            "breach", f"{metric:g} duplicate key(s)")

    raise ValueError(f"monitor kind {kind!r} has no evaluation branch")


def latest_incremental_added_rows(engine, ident: str) -> float:
    """Rows added by the most recent non-rebuild snapshot.

    Cumulative ``count(*)`` is the right volume signal for a table rebuilt on
    every run and the wrong one for an append-only table: five batches of
    1,000 rows followed by a batch of 1 still reports ~5,001 against a
    baseline of ~3,000, and the collapse is invisible.
    """
    incremental = [d for d in engine.snapshot_details(ident) if not d["is_full_rebuild"]]
    if not incremental:
        return 0.0
    return float(incremental[-1]["added_records"])


# Iceberg declared type -> the Arrow types that legitimately represent it.
_TYPE_MAP: dict[str, tuple[str, ...]] = {
    "long": ("int64",),
    "int": ("int32",),
    "double": ("double",),
    "float": ("float",),
    "string": ("string", "large_string"),
    "boolean": ("bool",),
    "date": ("date32[day]",),
    "timestamptz": ("timestamp[us]", "timestamp[us, tz=UTC]", "timestamp[us, tz=+00:00]"),
    "struct": ("struct",),
}


def check_column_types(engine, table_def, contract, as_of: date) -> list[MonitorResult]:
    """Physical types vs the contract's declared types. Metadata only.

    Catches silent coercion -- a column that arrives as string where the
    contract declares int passes every value-level expectation while breaking
    every arithmetic consumer downstream.
    """
    actual = {f.name: str(f.type) for f in engine.arrow_schema(table_def.name)}
    results: list[MonitorResult] = []
    for field in contract.schema_fields:
        observed = actual.get(field.name)
        name = f"{table_def.name}_type_{field.name}"
        if observed is None:
            results.append(MonitorResult(name, table_def.name, field.name, "column_type",
                                         None, None, "breach",
                                         f"declared column '{field.name}' is absent", as_of))
            continue
        allowed = _TYPE_MAP.get(field.type, (field.type,))
        ok = any(observed == a or observed.startswith(a) for a in allowed)
        results.append(MonitorResult(name, table_def.name, field.name, "column_type",
                                     None, None, "ok" if ok else "breach",
                                     f"declared {field.type}, observed {observed}", as_of))
    return results
