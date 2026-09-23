"""The daily platform report: what the agent saw, and what changed since.

This module **reads**. It does not measure. Every number in the report was
computed by ``runner`` or ``graph`` and persisted to ``ops.*`` before the
report existed; the report's job is to select, diff and render. A report that
recomputes a metric can disagree with the monitor that raised the alert, and
when those two disagree the report is the one people read.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from ops_agent import config, tables

REPORT_DIR = config.REPORT_DIR

_SCHEMA_KINDS = frozenset({"schema_drift", "enum_drift"})
_ANOMALY_KINDS = frozenset({"volume_anomaly"})
_QUALITY_MONITOR_KINDS = frozenset({"null_rate", "duplicate_rate", "column_type"})
_ANOMALY_MONITOR_KINDS = frozenset({"row_count", "cardinality", "distribution_shift"})
ACTIONABLE_SEVERITIES = frozenset({"breaking", "renaming"})


@dataclass(frozen=True)
class ReportSnapshot:
    as_of: date
    previous_run: date | None
    ran: bool
    findings: list[dict] = field(default_factory=list)
    previous_findings: list[dict] = field(default_factory=list)
    first_seen: dict[str, date] = field(default_factory=dict)
    monitors: list[dict] = field(default_factory=list)
    previous_monitors: dict[str, float | None] = field(default_factory=dict)
    alerts: list[dict] = field(default_factory=list)


def _rows(engine, table_def, query: str) -> list[dict]:
    if not engine.table_exists(table_def.name):
        return []
    return engine.sql(query, tables={"t": table_def.name}).to_pylist()


def _as_date(value) -> date | None:
    return value.date() if hasattr(value, "date") and not isinstance(value, date) else value


def _latest_per_key(rows: list[dict]) -> list[dict]:
    """One row per ``finding_key``, last write wins: running the agent twice
    on one date must not report "2 still open" for one finding."""
    latest: dict[str, dict] = {}
    for row in rows:
        latest[row["finding_key"]] = row
    return list(latest.values())


def load_snapshot(engine, as_of: date) -> ReportSnapshot:
    """Select everything the report needs for ``as_of``. The only engine call."""
    runs = [_as_date(r["run_at"]) for r in _rows(
        engine, tables.OPS_AGENT_RUNS, "SELECT DISTINCT run_at FROM t ORDER BY run_at")]
    ran = as_of in runs
    earlier = [r for r in runs if r < as_of]
    previous = earlier[-1] if earlier else None

    findings = _rows(engine, tables.OPS_FINDING_LOG, "SELECT * FROM t")
    for row in findings:
        row["run_at"] = _as_date(row["run_at"])
    today = _latest_per_key([f for f in findings if f["run_at"] == as_of])
    prior = _latest_per_key([f for f in findings if f["run_at"] == previous]) if previous else []

    first_seen: dict[str, date] = {}
    for row in findings:
        key = row["finding_key"]
        if key not in first_seen or row["run_at"] < first_seen[key]:
            first_seen[key] = row["run_at"]

    monitor_rows = _rows(engine, tables.OPS_MONITOR_RESULTS, "SELECT * FROM t")
    for row in monitor_rows:
        row["run_at"] = _as_date(row["run_at"])
    monitors_today = [m for m in monitor_rows if m["run_at"] == as_of]
    if not monitors_today and monitor_rows:
        newest = max((m["run_at"] for m in monitor_rows if m["run_at"] <= as_of), default=None)
        monitors_today = [m for m in monitor_rows if m["run_at"] == newest]
    monitors_today = list({m["monitor"]: m for m in monitors_today}.values())

    monitor_runs = sorted({m["run_at"] for m in monitor_rows})
    current_run = monitors_today[0]["run_at"] if monitors_today else None
    prior_runs = [r for r in monitor_runs if current_run and r < current_run]
    previous_monitors = {m["monitor"]: m["metric"] for m in monitor_rows
                         if prior_runs and m["run_at"] == prior_runs[-1]}

    alerts = _rows(engine, tables.OPS_ALERT_LOG, "SELECT * FROM t")
    for row in alerts:
        row["run_at"] = _as_date(row["run_at"])
    alerts = [a for a in alerts if a["run_at"] == as_of]

    return ReportSnapshot(as_of=as_of, previous_run=previous, ran=ran, findings=today,
                          previous_findings=prior, first_seen=first_seen,
                          monitors=monitors_today, previous_monitors=previous_monitors,
                          alerts=alerts)


def _section_of(finding: dict) -> str:
    kind = finding["kind"]
    if kind in _SCHEMA_KINDS:
        return "schema"
    if kind in _ANOMALY_KINDS:
        return "anomaly"
    if kind == "monitor_breach":
        monitor_kind = _evidence(finding).get("kind")
        if monitor_kind in _ANOMALY_MONITOR_KINDS:
            return "anomaly"
        if monitor_kind in _QUALITY_MONITOR_KINDS:
            return "quality"
    return "error"


def _num(value) -> str:
    if value is None:
        return "—"
    magnitude = abs(value)
    if magnitude >= 1000 and float(value).is_integer():
        return f"{value:,.0f}"
    if 0 < magnitude < 0.001:
        return f"{value:.2e}"
    return f"{value:,.4g}"


def _delta(current, previous) -> str:
    """Signed change, or an em dash when there is nothing to compare against.
    Never ``0`` for "no previous value": unchanged and never-measured differ."""
    if current is None or previous is None:
        return "—"
    change = current - previous
    if current and abs(change) < abs(current) * 1e-9:
        return "0"
    if change == 0:
        return "0"
    return f"{'+' if change > 0 else ''}{_num(change)}"


def _evidence(finding: dict) -> dict:
    try:
        return json.loads(finding.get("evidence") or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}


def _age(snapshot: ReportSnapshot, key: str) -> str:
    seen = snapshot.first_seen.get(key)
    if seen is None or seen == snapshot.as_of:
        return "new today"
    return f"open {(snapshot.as_of - seen).days}d (since {seen})"


def _verdict(snapshot: ReportSnapshot) -> list[str]:
    if not snapshot.ran:
        return ["## Verdict — NO RUN RECORDED", "",
                (f"No agent run is recorded for {snapshot.as_of}. This is **not** a clean "
                 "bill of health: it means the report found no evidence that anything was "
                 "checked. Run `./deploy.sh agent` for this date.")]
    blocking = sorted((f for f in snapshot.findings if f["severity"] in ACTIONABLE_SEVERITIES),
                      key=lambda x: (x["severity"], x["finding_key"]))
    failed = [m for m in snapshot.monitors if m["status"] == "breach"]
    if not blocking and not failed:
        return ["## Verdict — CLEAN", "",
                (f"The agent ran on {snapshot.as_of} and raised {len(snapshot.findings)} "
                 f"finding(s), none of them actionable. {len(snapshot.monitors)} monitor(s) "
                 "reported, none breaching.")]
    reasons = [f"{len(blocking)} actionable finding(s)"] if blocking else []
    if failed:
        reasons.append(f"{len(failed)} monitor breach(es)")
    return ["## Verdict — ATTENTION REQUIRED", "", f"{' and '.join(reasons)} on {snapshot.as_of}.",
            "", *[f"- **[{f['severity']}]** {f['detail']}" for f in blocking]]


def _findings_table(snapshot: ReportSnapshot, findings: list[dict]) -> list[str]:
    if not findings:
        return ["_Nothing in this section for this run._"]
    lines = ["| Severity | Table | What | Age |", "|---|---|---|---|"]
    for f in sorted(findings, key=lambda x: (x["severity"], x["finding_key"])):
        detail = (f.get("detail") or "").replace("|", "\\|")
        lines.append(f"| `{f['severity']}` | `{f['table_name']}` | {detail} "
                     f"| {_age(snapshot, f['finding_key'])} |")
    return lines


def _schema_section(snapshot: ReportSnapshot) -> list[str]:
    findings = [f for f in snapshot.findings if _section_of(f) == "schema"]
    lines = ["## Schema and enum evolution", ""]
    if not findings:
        lines.append("No schema or enum change observed against the registered contracts "
                     "this run.")
        return lines
    for f in sorted(findings, key=lambda x: (x["kind"], x["finding_key"])):
        evidence = _evidence(f)
        change = evidence.get("change") or evidence.get("error") or f["kind"]
        lines += [f"### `{change}` — {f['table_name']}", "", f"**{f['detail']}**", ""]
        if evidence.get("field_id") is not None:
            lines.append(f"- Iceberg field id: `{evidence['field_id']}` (unchanged — no data "
                         "file was rewritten)")
        if evidence.get("new_values"):
            lines.append("- New enum value(s): "
                         + ", ".join(f"`{v}`" for v in evidence["new_values"]))
        declared, observed = evidence.get("declared_type"), evidence.get("observed_type")
        if declared and observed and declared != observed:
            lines.append(f"- Contract declares `{declared}`, table has `{observed}`")
        lines.append(f"- Severity `{f['severity']}` — {f.get('reasoning', '')}")
        lines.append(f"- {_age(snapshot, f['finding_key'])}")
        lines.append("")
    return lines


def _quality_section(snapshot: ReportSnapshot) -> list[str]:
    lines = ["## Data quality", ""]
    if not snapshot.monitors:
        lines.append("**No monitor results recorded for this date.** That is an absence of "
                     "evidence, not a pass — run `./deploy.sh monitor`.")
        return lines
    lines += ["| Monitor | Table | Observed | Baseline | Δ vs prev | Status |",
              "|---|---|---|---|---|---|"]
    for m in sorted(snapshot.monitors, key=lambda x: (x["status"] != "breach", x["monitor"])):
        metric, baseline = m.get("metric"), m.get("baseline_median")
        prev = snapshot.previous_monitors.get(m["monitor"])
        lines.append(f"| `{m['monitor']}` | `{m['table_name']}` | {_num(metric)} "
                     f"| {_num(baseline)} | {_delta(metric, prev)} | `{m['status']}` |")
    breaches = [m for m in snapshot.monitors if m["status"] == "breach"]
    lines += ["", f"{len(snapshot.monitors)} monitor(s), {len(breaches)} breaching."]
    return lines


def _anomaly_section(snapshot: ReportSnapshot) -> list[str]:
    findings = [f for f in snapshot.findings if _section_of(f) == "anomaly"]
    lines = ["## Anomalies", ""]
    if not findings:
        lines.append("No volume or distribution anomaly raised this run.")
        return lines
    for f in sorted(findings, key=lambda x: x["finding_key"]):
        evidence = _evidence(f)
        lines.append(f"- **{f['detail']}**")
        if evidence.get("median") is not None:
            lines.append(f"  - baseline median: `{_num(evidence['median'])}`, observed: "
                         f"`{_num(evidence.get('rows_in_latest_snapshot'))}`")
        lines.append(f"  - {_age(snapshot, f['finding_key'])}")
    return lines


def _error_section(snapshot: ReportSnapshot) -> list[str]:
    findings = [f for f in snapshot.findings if _section_of(f) in ("error", "quality")]
    blocked = [f for f in findings if f["severity"] in ACTIONABLE_SEVERITIES]
    return ["## Errors and blocked promotions", "",
            f"**{len(blocked)}** finding(s) would block promotion.", "",
            *_findings_table(snapshot, findings)]


def _diff_section(snapshot: ReportSnapshot) -> list[str]:
    lines = ["## What changed since the previous run", ""]
    if snapshot.previous_run is None:
        lines.append("No earlier agent run is recorded, so there is nothing to diff against. "
                     "Every finding above is being seen for the first time by definition, "
                     "not because it is new.")
        return lines
    today = {f["finding_key"]: f for f in snapshot.findings}
    prior = {f["finding_key"]: f for f in snapshot.previous_findings}

    def ordered(keys, source):
        return [source[k] for k in sorted(keys)]

    new = ordered(today.keys() - prior.keys(), today)
    cleared = ordered(prior.keys() - today.keys(), prior)
    still = ordered(today.keys() & prior.keys(), today)
    lines += [f"Compared with `{snapshot.previous_run}`: **{len(new)} new**, "
              f"**{len(cleared)} cleared**, **{len(still)} still open**.", ""]
    if new:
        lines += ["### New", *[f"- `[{f['severity']}]` {f['detail']}" for f in new], ""]
    if cleared:
        lines += ["### Cleared", *[f"- ~~`[{f['severity']}]` {f['detail']}~~" for f in cleared], ""]
    if still:
        lines += ["### Still open",
                  *[f"- `[{f['severity']}]` {f['detail']} ({_age(snapshot, f['finding_key'])})"
                    for f in still], ""]
    if not (new or cleared or still):
        lines.append("Nothing open on either date — the platform was clean before and is "
                     "clean now.")
    return lines


def _incidents_section(snapshot: ReportSnapshot) -> list[str]:
    open_incidents = [f for f in snapshot.findings if f["severity"] in ACTIONABLE_SEVERITIES]
    lines = ["## Open incidents", ""]
    if not open_incidents:
        lines.append("None open.")
        return lines
    lines += ["| Severity | Table | Detail | Age |", "|---|---|---|---|"]
    for f in sorted(open_incidents, key=lambda x: (
            snapshot.first_seen.get(x["finding_key"], snapshot.as_of), x["finding_key"])):
        detail = (f.get("detail") or "").replace("|", "\\|")
        lines.append(f"| `{f['severity']}` | `{f['table_name']}` | {detail} "
                     f"| {_age(snapshot, f['finding_key'])} |")
    return lines


def build_report(snapshot: ReportSnapshot) -> str:
    """Render ``snapshot``. Takes no engine and computes no metric of its own.
    Every list is sorted on a stable key so the output depends only on what
    is in the snapshot, never on the order it arrived in."""
    parts = [
        [f"# Daily platform report — {snapshot.as_of}", "",
         ("_Assembled from `ops.agent_runs`, `ops.finding_log`, `ops.monitor_results` and "
          "`ops.alert_log`. Every number here was measured by the monitors or the agent "
          "before this report ran; nothing on this page is recomputed._"), ""],
        _verdict(snapshot), _schema_section(snapshot), _quality_section(snapshot),
        _anomaly_section(snapshot), _error_section(snapshot), _diff_section(snapshot),
        _incidents_section(snapshot),
    ]
    rendered = []
    for part in parts:
        while part and not part[-1].strip():
            part = part[:-1]
        rendered.append("\n".join(part))
    return "\n\n".join(rendered) + "\n"


def write_report(engine, as_of: date, directory: Path | None = None) -> tuple[Path, ReportSnapshot]:
    snapshot = load_snapshot(engine, as_of)
    out_dir = Path(directory or REPORT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"daily-{as_of}.md"
    path.write_text(build_report(snapshot))
    return path, snapshot


def main() -> int:
    from ops_agent import lakehouse
    from ops_agent.engine import SparkEngine

    engine = SparkEngine()
    as_of = config.resolve_as_of_date(lakehouse.max_event_date(engine))
    path, snapshot = write_report(engine, as_of)
    print(f"report: {as_of} — {len(snapshot.findings)} finding(s), "
          f"{len(snapshot.monitors)} monitor result(s) -> {path}")
    if not snapshot.ran:
        print("report: WARNING no agent run recorded for this date; the report says so "
              "rather than reporting clean")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
