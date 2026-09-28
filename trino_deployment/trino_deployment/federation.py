"""Cross-catalog reconciliation: Trino over Iceberg against dbt's gold in PostgreSQL.

The same events sit in two engines. dbt computed ``page_type_summary`` from
``raw.impressions`` and it was promoted into ``gold``; Trino computes the same
rollup from the Iceberg copy. One statement joins the two on ``page_type``
and reports every metric side by side with its delta. A nonzero delta is a
finding: either the engines disagree on semantics (rounding, distinct
counting, the malformed-date row) or the copies have drifted.
"""
from __future__ import annotations

import sys

from trino_deployment import client
from trino_deployment.analyses import PAGE_TYPE_SUMMARY

GOLD = "postgres_gold.gold.page_type_summary"

METRICS = ("total_impressions", "unique_users", "avg_funnel_depth", "max_funnel_depth",
           "avg_duration_seconds", "impressions_reaching_d", "impressions_reaching_e",
           "impressions_reaching_f", "pct_reaching_d", "pct_reaching_e", "pct_reaching_f")


def reconciliation_sql() -> str:
    cols = ",\n       ".join(
        f"i.{m} AS iceberg_{m}, g.{m} AS gold_{m}, "
        f"CAST(i.{m} AS double) - CAST(g.{m} AS double) AS delta_{m}" for m in METRICS)
    return f"""
WITH iceberg_summary AS (
{PAGE_TYPE_SUMMARY.replace("ORDER BY page_type", "")}
)
SELECT coalesce(i.page_type, g.page_type) AS page_type,
       {cols}
FROM iceberg_summary i
FULL OUTER JOIN {GOLD} g ON g.page_type = i.page_type
ORDER BY page_type
"""


def reconcile(conn=None) -> list[dict]:
    """One row per page_type; each metric as iceberg_, gold_, delta_."""
    return client.query(reconciliation_sql(), conn).dicts()


def mismatches(rows: list[dict], tolerance: float = 0.0) -> list[tuple[int, str, float]]:
    out = []
    for row in rows:
        for m in METRICS:
            delta = row.get(f"delta_{m}")
            if delta is None or abs(float(delta)) > tolerance:
                out.append((row["page_type"], m, None if delta is None else float(delta)))
    return out


def main() -> int:
    rows = reconcile()
    print(f"reconciliation: {len(rows)} page types, {len(METRICS)} metrics each")
    for row in rows:
        print(f"  page_type {row['page_type']}:")
        for m in METRICS:
            print(f"    {m:<24} iceberg={row[f'iceberg_{m}']!s:<10} gold={row[f'gold_{m}']!s:<10} "
                  f"delta={row[f'delta_{m}']}")
    bad = mismatches(rows)
    print(f"mismatches: {len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
