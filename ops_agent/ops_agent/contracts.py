"""Declared contracts, evaluated against Arrow tables. Fails closed.

The same YAML is read by the build (to gate the aggregated table) and by the
ops agent (to diff against live Iceberg schemas). One source of truth, two
consumers -- otherwise the agent alerts on rules the build never enforced.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import yaml

from ops_agent import config, dates

CONTRACTS_DIR = config.CONTRACT_DIR


class ContractViolation(Exception):
    pass


@dataclass(frozen=True)
class SchemaField:
    name: str
    type: str
    nullable: bool = True


@dataclass(frozen=True)
class Contract:
    table: str
    version: int
    owner: str
    schema_fields: tuple[SchemaField, ...]
    expectations: tuple[dict, ...]
    # Kept out of ``expectations`` on purpose. Everything there is a
    # fail-closed gate; an enum watch records a change and must never block a
    # build. The agent reads this key; the build does not.
    enum_watch: tuple[dict, ...] = ()


@dataclass(frozen=True)
class ExpectationResult:
    kind: str
    column: str | None
    passed: bool
    observed: object = None
    detail: str = ""


@dataclass
class ValidationReport:
    table: str
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def failures(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.passed]

    @property
    def passed(self) -> bool:
        return not self.failures


def load_contract(path: Path) -> Contract:
    raw = yaml.safe_load(Path(path).read_text())
    return Contract(
        table=raw["table"],
        version=int(raw.get("version", 1)),
        owner=raw.get("owner", "unknown"),
        schema_fields=tuple(
            SchemaField(f["name"], f["type"], f.get("nullable", True))
            for f in raw.get("schema", [])),
        expectations=tuple(raw.get("expectations", [])),
        enum_watch=tuple(raw.get("enum_watch", [])),
    )


def contract_for(filename: str) -> Contract:
    return load_contract(CONTRACTS_DIR / filename)


def validate(table: pa.Table, contract: Contract, as_of: date) -> ValidationReport:
    report = ValidationReport(table=contract.table)

    for sf in contract.schema_fields:
        present = sf.name in table.column_names
        report.results.append(ExpectationResult(
            "schema_present", sf.name, present, observed=present,
            detail="" if present else "column missing"))
        # ``nullable: false`` is a declaration, so it has to be a check.
        if present and not sf.nullable:
            nulls = table.column(sf.name).null_count
            report.results.append(ExpectationResult(
                "not_null", sf.name, nulls == 0, nulls,
                f"{nulls} null(s) in a column declared nullable: false"))

    for exp in contract.expectations:
        report.results.append(_evaluate(table, exp, as_of))
    return report


def _evaluate(table: pa.Table, exp: dict, as_of: date) -> ExpectationResult:
    kind = exp["type"]
    column = exp.get("column")

    if kind == "row_count":
        n = table.num_rows
        return ExpectationResult(kind, None, n >= exp.get("min", 0), n)

    if column not in table.column_names:
        return ExpectationResult(kind, column, False, None, "column missing")

    col = table.column(column)

    if kind == "not_null":
        nulls = col.null_count
        return ExpectationResult(kind, column, nulls == 0, nulls, f"{nulls} null(s)")

    if kind == "unique":
        distinct = pc.count_distinct(col).as_py()
        dupes = (table.num_rows - col.null_count) - distinct
        return ExpectationResult(kind, column, dupes == 0, dupes, f"{dupes} duplicate(s)")

    if kind == "accepted_values":
        allowed = set(exp["values"])
        seen = {v for v in col.to_pylist() if v is not None}
        bad = seen - allowed
        return ExpectationResult(kind, column, not bad, sorted(bad),
                                 f"unexpected: {sorted(bad)}")

    if kind == "range":
        values = [v for v in col.to_pylist() if v is not None]
        if not values:
            # A column with nothing in it is not a column that is in range.
            return ExpectationResult(
                kind, column, False, None,
                "no values to range-check (column is entirely NULL or empty)")
        lo, hi = min(values), max(values)
        ok = lo >= exp.get("min", float("-inf")) and hi <= exp.get("max", float("inf"))
        return ExpectationResult(kind, column, ok, (lo, hi))

    if kind == "freshness":
        newest, unparseable = dates.max_parseable_date(col.to_pylist())
        if newest is None:
            return ExpectationResult(kind, column, False, None, "no values")
        lag = (as_of - newest).days
        ok = lag <= exp["max_lag_days"] and unparseable == 0
        return ExpectationResult(kind, column, ok, lag,
                                 f"{lag}d behind AS_OF {as_of}"
                                 + (f", {unparseable} unparseable" if unparseable else ""))

    raise ValueError(f"unknown expectation type: {kind}")


def assert_valid(table: pa.Table, contract: Contract, as_of: date) -> None:
    report = validate(table, contract, as_of)
    if not report.passed:
        lines = [f"  - {f.kind} on {f.column}: {f.detail} (observed={f.observed})"
                 for f in report.failures]
        raise ContractViolation(
            f"{contract.table} violated {len(report.failures)} expectation(s):\n"
            + "\n".join(lines))
