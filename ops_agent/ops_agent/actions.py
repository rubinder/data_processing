"""What the agent does about a finding. Dry-run by default.

An ops agent that files GitHub issues the moment someone runs it locally is
one people stop running. ``--execute`` is opt-in.
"""
from __future__ import annotations

import hashlib
import subprocess
from datetime import date
from pathlib import Path

from ops_agent import config, sensors
from ops_agent.sensors import Finding

INCIDENTS_DIR = config.INCIDENTS_DIR


def incident_slug(finding: Finding) -> str:
    """Stable across runs so a recurring finding updates one file, not many."""
    digest = hashlib.sha256(sensors.finding_key(finding).encode()).hexdigest()[:8]
    column = (finding.evidence.get("column") or finding.evidence.get("monitor")
              or finding.kind)
    return f"{finding.table.replace('.', '-')}-{column}-{digest}"


def write_incident(finding: Finding, severity: str, reasoning: str, as_of: date) -> Path:
    INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)
    path = INCIDENTS_DIR / f"{incident_slug(finding)}.md"
    path.write_text(
        f"# {finding.kind.replace('_', ' ').title()} — {finding.table}\n\n"
        f"**Severity:** {severity}\n\n"
        f"**Detected as of:** {as_of.isoformat()}\n\n"
        f"## What was observed\n\n{finding.detail}\n\n"
        f"## Why this severity\n\n{reasoning}\n\n"
        f"## Evidence\n\n```\n{finding.evidence}\n```\n")
    return path


def file_github_issue(finding: Finding, severity: str, reasoning: str,
                      execute: bool = False) -> str:
    title = f"[{severity}] {finding.kind} on {finding.table}"
    body = f"{finding.detail}\n\n{reasoning}\n\nEvidence: {finding.evidence}"
    command = ["gh", "issue", "create", "--title", title, "--body", body,
               "--label", f"data-{severity}"]
    if not execute:
        return f"DRY-RUN: would run `{' '.join(command[:3])} ...` -> {title}"
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except Exception as exc:  # noqa: BLE001 -- ``gh`` may be missing or
        # unauthenticated; a failed side effect is reported, not raised.
        return f"FAILED to file issue: {exc}"
