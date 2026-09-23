"""Severity classification: rules first, LLM only for genuine ambiguity.

CI must never depend on a model call, so ``USE_LLM`` is False unless a key is
present, and every rule path is deterministic.

The severity vocabulary exists because each name implies a different
response:

- ``breaking``   data may be lost or a cast will fail. A human is needed now.
- ``renaming``   no data moved (the field ID is unchanged), but the published
  contract names a column that no longer exists. Actionable, not an emergency.
- ``widening``   int -> long and friends. Existing values still fit.
- ``additive``   a new column. Worth recording, not worth paging.
- ``enum_drift`` a new value in a watched categorical column. Recorded, never
  blocking.
- ``benign``     nothing to do.
"""
from __future__ import annotations

import os

from ops_agent.sensors import Finding

USE_LLM = bool(os.environ.get("ANTHROPIC_API_KEY"))

# (declared, observed) pairs where the table's type is a safe widening.
_WIDENING = {("int", "long"), ("int32", "int64"), ("float", "double")}


def classify(finding: Finding) -> tuple[str, str]:
    if finding.kind == "monitor_breach":
        return "breaking", (
            f"Monitor '{finding.evidence.get('monitor')}' breached on {finding.table}: "
            f"{finding.detail} A monitor breach is already a judged verdict; there is "
            "no additive reading of a check that has failed.")

    if finding.kind == "volume_anomaly":
        return "breaking", (
            f"Latest batch added {finding.evidence.get('rows_in_latest_snapshot'):,} rows "
            f"against a trailing median of {finding.evidence.get('median'):,.0f}. "
            "Downstream aggregates will be silently wrong rather than obviously missing.")

    if finding.kind == "staleness":
        return "breaking", (
            f"Data is {finding.evidence.get('lag_days')}d behind the pipeline's logical "
            "clock; every rollup built from it reports a day that has not arrived.")

    if finding.kind == "data_corruption":
        column = finding.evidence.get("column")
        where = f" in '{column}'" if column else ""
        return "breaking", (
            f"{finding.evidence.get('unparseable_count')} value(s){where} do not parse as "
            "dates. Every time-based join or freshness check on this column is unreliable "
            "until the source is fixed.")

    if finding.kind == "enum_drift":
        column = finding.evidence.get("column")
        error = finding.evidence.get("error")
        if error == "column_absent":
            return "breaking", (
                f"The enum watch on '{column}' is not monitoring anything: the column "
                "produced no values. New values in it will go unnoticed while the check "
                "reports clean.")
        if error == "not_comparable":
            return "benign", (
                f"The enum watch on '{column}' points at a struct or list column, whose "
                "values cannot be diffed as an enum. Remove the watch.")
        if error == "no_values":
            return "breaking", (
                f"'{column}' is watched as a categorical column but holds no values at "
                f"all, against {finding.evidence.get('known_count')} registered. A column "
                "that has gone entirely NULL reads clean at exactly the moment it stopped "
                "working.")
        if error == "cardinality_exceeded":
            return "benign", (
                f"'{column}' is not a categorical column "
                f"({finding.evidence.get('distinct_at_least')}+ distinct values); the watch "
                "is misconfigured. Remove it.")
        values = finding.evidence.get("new_values", [])
        return "enum_drift", (
            f"New value(s) {', '.join(repr(v) for v in values)} appeared in '{column}', "
            "which the contract does not register. Nothing breaks today, but any funnel "
            "or mart that groups on this column now has a bucket nobody declared.")

    if finding.kind == "schema_drift":
        change = finding.evidence.get("change")
        column = finding.evidence.get("column")
        if change == "added":
            return "additive", (
                f"New column '{column}' does not affect existing readers. Worth adding to "
                "the contract, not worth paging anyone.")
        if change == "dropped":
            return "breaking", (
                f"Column '{column}' is declared in the contract but is absent from the "
                "table, so the published contract is violated. Any consumer that selects "
                "it by name breaks; which consumers those are is not visible from this "
                "table's metadata and needs a human to confirm.")
        if change == "renamed":
            new_name = finding.evidence.get("renamed_to")
            field_id = finding.evidence.get("field_id")
            declared = finding.evidence.get("declared_type", "")
            observed = finding.evidence.get("observed_type", "")
            if finding.evidence.get("type_compatible") is False:
                return "breaking", (
                    f"'{column}' was renamed to '{new_name}' (both are field id {field_id}, "
                    f"so no data file moved) but its type also went {declared} -> "
                    f"{observed}. The rename is safe; the type change is not.")
            return "renaming", (
                f"'{column}' was renamed to '{new_name}'. Both are field id {field_id}: "
                "Iceberg resolves columns by ID, not by name, so every file written under "
                "the old name still reads back correctly and no data was rewritten or "
                f"lost. What is wrong is the published contract, which still declares "
                f"'{column}'. That needs a recorded decision, not a rollback.")
        if change == "type_changed":
            declared = finding.evidence.get("declared_type", "")
            observed = finding.evidence.get("observed_type", "")
            if (declared, observed) in _WIDENING:
                return "widening", (
                    f"'{column}' widened {declared} -> {observed}; every existing value "
                    "still fits and old readers keep working. Worth recording against the "
                    "contract, not worth paging anyone.")
            return "breaking", (
                f"'{column}' narrowed or changed kind {declared} -> {observed}; existing "
                "values may not fit and downstream casts will fail.")

    if USE_LLM:
        return _classify_with_llm(finding)
    return "benign", "No rule matched and LLM classification is disabled."


def _classify_with_llm(finding: Finding) -> tuple[str, str]:
    """Optional judgment call for findings no rule matches.

    Only reached with ``ANTHROPIC_API_KEY`` set -- never in CI or the tests.
    Falls back to benign on any error so an absent model never blocks the
    agent. A server-side fallback model is declared so a declined request is
    re-run rather than dropped.
    """
    try:
        import anthropic

        client = anthropic.Anthropic()
        message = client.beta.messages.create(
            model="claude-opus-5",
            max_tokens=300,
            betas=["server-side-fallback-2026-06-01"],
            fallbacks=[{"model": "claude-opus-4-8"}],
            messages=[{"role": "user", "content": (
                "Classify this data-platform finding as exactly one of breaking, "
                "additive, or benign. Reply as '<severity>: <one sentence>'.\n\n"
                f"Finding: {finding.kind} on {finding.table}\nDetail: {finding.detail}\n"
                f"Evidence: {finding.evidence}")}],
        )
        if message.stop_reason == "refusal":
            return "benign", "LLM declined to classify this finding."
        text = next((b.text for b in message.content if b.type == "text"), "").strip()
        severity, _, reasoning = text.partition(":")
        severity = severity.strip().lower()
        if severity not in {"breaking", "additive", "benign"}:
            return "benign", f"Unparseable LLM response: {text[:120]}"
        return severity, reasoning.strip()
    except Exception as exc:  # noqa: BLE001 -- best-effort call; any failure
        # (network, auth, SDK, parsing) degrades to benign, never a crash.
        return "benign", f"LLM classification unavailable ({type(exc).__name__})."
