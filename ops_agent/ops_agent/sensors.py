"""Read Iceberg metadata and diff it against declared contracts.

Columns, field IDs, row counts and snapshot counts all come from table
metadata, not a data scan. The agent is meant to run cheaply and often, and a
sensor that scans the lake on every run is one that gets switched off. The
one thing metadata does not carry is a column's newest *value*, so the
freshness column and any watched enum columns are read through a single
column-projected scan.

Volume is compared per snapshot, not on the cumulative row count: an
append-only table's total only ever grows, so a total-vs-median check can
never fire. An incident shows up as a dip in the batch that just landed.

Field IDs are the only evidence that distinguishes a rename from a drop plus
an unrelated add. Iceberg keeps the ID stable across a rename -- that is why
no data file is rewritten -- so an agent that reads names only cannot see
the difference the storage layer is built around.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import date

from ops_agent import dates

VOLUME_COLLAPSE_RATIO = 0.5  # below half the trailing median is an incident

# Above this many distinct values a column is not an enum, and an enum watch
# pointed at it is a misconfiguration worth reporting.
ENUM_CARDINALITY_LIMIT = 200


@dataclass(frozen=True)
class ObservedState:
    table: str
    columns: dict[str, str]
    row_count: int
    rows_in_latest_snapshot: int
    snapshot_count: int
    newest_date: date | None
    unparseable_date_count: int = 0
    field_ids: dict[str, int] = field(default_factory=dict)
    # Every historical name of a still-present column -> (current name, id).
    renames: dict[str, tuple[str, int]] = field(default_factory=dict)
    # Keyed by the *contract's* column name, so a watch survives a rename.
    enum_values: dict[str, list] = field(default_factory=dict)
    # Why a watched column produced no comparable values. Separate from
    # ``enum_values`` so "could not evaluate" can never read as "no drift".
    enum_errors: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Finding:
    kind: str   # schema_drift | enum_drift | volume_anomaly | staleness | data_corruption | monitor_breach
    table: str
    detail: str
    evidence: dict = field(default_factory=dict)


def finding_key(finding: Finding) -> str:
    """The stable identity of a finding, across runs and processes.

    One implementation, two consumers: the incident filename and the
    ``ops.finding_log`` row. They must agree, or the report's "first seen"
    and the incident file on disk describe different things.
    """
    return "|".join((
        finding.table, finding.kind,
        str(finding.evidence.get("column") or ""),
        str(finding.evidence.get("change") or ""),
        str(finding.evidence.get("monitor") or ""),
    ))


def build_rename_map(schema_versions: list[dict]) -> dict[str, tuple[str, int]]:
    """Every past name of a still-present column -> (current name, field id).

    Grouping names by field ID across the whole schema history makes
    transitive renames (a -> b -> c) fall out for free. A field ID absent from
    the latest schema is a genuine drop and is deliberately not in the map.
    """
    if not schema_versions:
        return {}
    current_by_id = {fid: name
                     for name, fid in schema_versions[-1].get("field_ids", {}).items()}
    names_by_id: dict[int, set[str]] = {}
    for version in schema_versions:
        for name, fid in version.get("field_ids", {}).items():
            names_by_id.setdefault(fid, set()).add(name)
    return {old: (current_by_id[fid], fid)
            for fid, names in names_by_id.items() if fid in current_by_id
            for old in names if old != current_by_id[fid]}


def observe(engine, table_def, date_column: str | None = None,
            enum_columns: tuple[str, ...] = ()) -> ObservedState:
    """Build an ``ObservedState`` for ``table_def``.

    ``date_column`` and ``enum_columns`` are contract names. Each is resolved
    through the rename map before it is read, so a watch on a column that
    was renamed keeps watching the same field ID rather than going blind.
    """
    ident = table_def.name
    schema_versions = engine.schema_history(ident)
    latest = schema_versions[-1] if schema_versions else {}
    columns = latest.get("columns", {})
    field_ids = latest.get("field_ids", {})
    renames = build_rename_map(schema_versions)

    details = engine.snapshot_details(ident)
    per_snapshot = engine.snapshot_row_counts(ident)
    row_count = details[-1]["total_records"] if details else 0

    def live_name(contract_name: str | None) -> str | None:
        if not contract_name:
            return None
        if contract_name in columns:
            return contract_name
        target = renames.get(contract_name)
        return target[0] if target and target[0] in columns else None

    live_date = live_name(date_column)
    live_enums = {c: live_name(c) for c in enum_columns}
    projection = sorted({c for c in (live_date, *live_enums.values()) if c})
    arrow = engine.scan_arrow(ident, columns=projection) if projection else None

    newest, unparseable = None, 0
    if arrow is not None and live_date:
        newest, unparseable = dates.max_parseable_date(arrow.column(live_date).to_pylist())

    enum_values: dict[str, list] = {}
    enum_errors: dict[str, str] = {}
    for contract_name, live in live_enums.items():
        if arrow is None or live is None:
            continue  # ``detect()`` reports a watch that read nothing
        try:
            distinct = sorted({v for v in arrow.column(live).to_pylist() if v is not None})
        except TypeError:
            enum_errors[contract_name] = "not_comparable"
            continue
        enum_values[contract_name] = distinct[:ENUM_CARDINALITY_LIMIT + 1]

    return ObservedState(
        table=ident, columns=columns, row_count=row_count,
        rows_in_latest_snapshot=per_snapshot[-1] if per_snapshot else 0,
        snapshot_count=len(per_snapshot), newest_date=newest,
        unparseable_date_count=unparseable, field_ids=field_ids, renames=renames,
        enum_values=enum_values, enum_errors=enum_errors)


def detect(state: ObservedState, contract, as_of: date, history: list[int]) -> list[Finding]:
    findings: list[Finding] = []
    declared = {f.name: f.type for f in contract.schema_fields}
    observed = state.columns

    # New names reached by a rename: reported once from the old name's side
    # and suppressed on the added-columns pass, otherwise one event surfaces
    # as an unrelated breaking drop plus a benign add.
    renamed_to: set[str] = set()

    for name, declared_type in declared.items():
        if name not in observed:
            target = state.renames.get(name)
            if target and target[0] in observed:
                new_name, field_id = target
                observed_type = observed[new_name]
                compatible = _types_compatible(declared_type, observed_type)
                renamed_to.add(new_name)
                findings.append(Finding(
                    "schema_drift", state.table,
                    f"column '{name}' was renamed to '{new_name}' (field id {field_id}); "
                    f"the contract still declares the old name",
                    {"change": "renamed", "column": name, "renamed_to": new_name,
                     "field_id": field_id, "declared_type": declared_type,
                     "observed_type": observed_type, "type_compatible": compatible}))
                continue
            findings.append(Finding(
                "schema_drift", state.table,
                f"column '{name}' declared in contract but absent from table",
                {"change": "dropped", "column": name, "declared_type": declared_type}))
        elif not _types_compatible(declared_type, observed[name]):
            findings.append(Finding(
                "schema_drift", state.table,
                f"column '{name}' type changed: {declared_type} -> {observed[name]}",
                {"change": "type_changed", "column": name,
                 "declared_type": declared_type, "observed_type": observed[name]}))

    for name in observed:
        if name not in declared and not name.startswith("_") and name not in renamed_to:
            findings.append(Finding(
                "schema_drift", state.table,
                f"column '{name}' present in table but not declared in contract",
                {"change": "added", "column": name, "observed_type": observed[name]}))

    if history:
        median = statistics.median(history)
        latest = state.rows_in_latest_snapshot
        if median > 0 and latest < median * VOLUME_COLLAPSE_RATIO:
            findings.append(Finding(
                "volume_anomaly", state.table,
                f"latest snapshot added {latest:,} rows, below "
                f"{VOLUME_COLLAPSE_RATIO:.0%} of trailing median {median:,.0f}",
                {"rows_in_latest_snapshot": latest, "median": median}))

    freshness = next((e for e in contract.expectations if e.get("type") == "freshness"), None)
    if freshness and state.newest_date is not None:
        lag = (as_of - state.newest_date).days
        if lag > freshness["max_lag_days"]:
            findings.append(Finding(
                "staleness", state.table,
                f"newest data is {lag}d behind AS_OF {as_of} (limit {freshness['max_lag_days']}d)",
                {"lag_days": lag, "as_of": as_of.isoformat()}))

    if state.unparseable_date_count > 0:
        column = freshness["column"] if freshness else None
        note = f" in '{column}'" if column else ""
        findings.append(Finding(
            "data_corruption", state.table,
            f"{state.unparseable_date_count} value(s){note} did not parse as a date",
            {"column": column, "unparseable_count": state.unparseable_date_count}))

    findings += _detect_enum_drift(state, contract)
    return findings


def _detect_enum_drift(state: ObservedState, contract) -> list[Finding]:
    """New values in a watched categorical column. Reports, never blocks.

    Deliberately separate from the contract's ``accepted_values`` gate, which
    stops the build and is right for a column the pipeline branches on. Most
    categorical columns are not like that: upstream adds a category, nothing
    breaks, but somebody needs to know before a mart quietly under-counts.
    """
    findings: list[Finding] = []
    for watch in getattr(contract, "enum_watch", ()):
        column = watch["column"]
        known = set(watch.get("known_values", ()))
        observed = state.enum_values.get(column)
        reason = state.enum_errors.get(column)

        if reason is None and observed is not None:
            try:
                set(observed)
            except TypeError:
                reason = "not_comparable"

        if reason == "not_comparable":
            findings.append(Finding(
                "enum_drift", state.table,
                f"enum watch on '{column}' cannot be evaluated: its values are not "
                f"comparable (a struct or list column); this column is NOT being monitored",
                {"column": column, "error": "not_comparable"}))
            continue
        if observed is not None and not observed:
            findings.append(Finding(
                "enum_drift", state.table,
                f"'{column}' is watched for enum drift but holds no values at all "
                f"({len(known)} registered value(s) and none present)",
                {"column": column, "error": "no_values", "known_count": len(known)}))
            continue
        if observed is None:
            findings.append(Finding(
                "enum_drift", state.table,
                f"enum watch declared on '{column}', but no values were read from that "
                f"column -- it is absent from the table, so it is NOT being monitored",
                {"column": column, "error": "column_absent"}))
            continue

        new_values = sorted(set(observed) - known, key=str)
        if len(observed) > ENUM_CARDINALITY_LIMIT:
            findings.append(Finding(
                "enum_drift", state.table,
                f"'{column}' holds more than {ENUM_CARDINALITY_LIMIT} distinct values, so "
                f"it is not an enum; the watch is misconfigured",
                {"column": column, "error": "cardinality_exceeded",
                 "new_values": new_values[:ENUM_CARDINALITY_LIMIT],
                 "distinct_at_least": len(observed)}))
            continue
        if new_values:
            findings.append(Finding(
                "enum_drift", state.table,
                f"{len(new_values)} new value(s) in '{column}': "
                f"{', '.join(str(v) for v in new_values)}",
                {"column": column, "new_values": new_values, "known_count": len(known),
                 "distinct_observed": len(observed)}))
    return findings


# ``observe()`` reads columns in Iceberg vocabulary, the same one contracts
# use, so declared == observed covers the normal path. This is a bridge for
# states built from an Arrow schema instead.
_COMPATIBLE = {("long", "int64"), ("int", "int32"), ("string", "string"),
               ("string", "large_string"), ("struct", "struct"), ("double", "double"),
               ("date", "date32[day]"), ("timestamptz", "timestamp[us]"),
               ("timestamptz", "timestamp[us, tz=UTC]"), ("boolean", "bool")}


def _types_compatible(declared: str, observed: str) -> bool:
    if declared == observed or (declared, observed) in _COMPATIBLE:
        return True
    return observed.startswith(declared)
