"""Paths, table names and the logical clock. Nothing here touches Spark."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parent.parent

FEED_DIR = Path(os.environ.get("OPS_AGENT_FEEDS", MODULE_ROOT / "feeds"))
CONTRACT_DIR = Path(os.environ.get("OPS_AGENT_CONTRACTS", MODULE_ROOT / "contracts"))
INCIDENTS_DIR = Path(os.environ.get("OPS_AGENT_INCIDENTS", MODULE_ROOT / "incidents"))
REPORT_DIR = Path(os.environ.get("OPS_AGENT_REPORTS", MODULE_ROOT / "reports"))

#: The append-only event table iceberg_deployment creates and seeds.
RAW_TABLE = "db.impressions"
#: Rebuilt from RAW_TABLE on every run; one row per impression.
AGGREGATED_TABLE = "db.impressions_aggregated"


def resolve_as_of_date(data_max: date | None) -> date:
    """The pipeline's logical 'now'.

    Never wall-clock. The impression data is generated for fixed dates, so a
    wall-clock freshness check would fail permanently and get muted -- which
    is how freshness monitoring dies in real systems. ``AS_OF_DATE`` overrides
    for reproducing a stale feed; otherwise the newest event date is 'now'.
    """
    override = os.environ.get("AS_OF_DATE")
    if override:
        return date.fromisoformat(override)
    if data_max is None:
        raise RuntimeError(
            "no impression data to derive the logical date from; seed the "
            "raw table or set AS_OF_DATE=YYYY-MM-DD")
    return data_max
