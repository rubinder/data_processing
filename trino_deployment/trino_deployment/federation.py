"""Cross-catalog reconciliation: Trino over Iceberg and ClickHouse against
dbt's gold in PostgreSQL.

The same events sit in three engines. dbt computed ``page_type_summary``
from ``raw.impressions`` and it was promoted into ``gold``; Trino computes
the same rollup from the Iceberg copy and from the ClickHouse cluster. One
statement joins them on ``page_type`` and reports every metric side by side
with each engine's delta from gold. A nonzero delta is a finding: either the
engines disagree on semantics (rounding, distinct counting, the malformed-
date row) or a copy has drifted.
"""
from __future__ import annotations

import sys

from trino_deployment import client
from trino_deployment.analyses import sql_for

GOLD = "postgres_gold.gold.page_type_summary"

METRICS = ("total_impressions", "unique_users", "avg_funnel_depth", "max_funnel_depth",
           "avg_duration_seconds", "impressions_reaching_d", "impressions_reaching_e",
           "impressions_reaching_f", "pct_reaching_d", "pct_reaching_e", "pct_reaching_f")


def reconciliation_sql(engines: tuple[str, ...] = ("iceberg",)) -> str:
    """Gold on the right, one CTE per engine on the left, deltas per engine."""
    ctes = ",\n".join(
        f"{e}_summary AS (\n{sql_for('page_type_summary', e).replace('ORDER BY page_type', '')}\n)"
        for e in engines)
    cols = []
    for m in METRICS:
        cols.append(f"g.{m} AS gold_{m}")
        for e in engines:
            cols.append(f"{e[0]}.{m} AS {e}_{m}")
            cols.append(f"CAST({e[0]}.{m} AS double) - CAST(g.{m} AS double) AS delta_{e}_{m}")
    joins = "\n".join(f"FULL OUTER JOIN {e}_summary {e[0]} ON {e[0]}.page_type = g.page_type"
                       for e in engines)
    select_list = ",\n       ".join(cols)
    return f"""
WITH {ctes}
SELECT g.page_type,
       {select_list}
FROM {GOLD} g
{joins}
ORDER BY g.page_type
"""


def reconcile(conn=None, engines: tuple[str, ...] = ("iceberg",)) -> list[dict]:
    """One row per page_type; each metric as gold_, <engine>_, delta_<engine>_."""
    return client.query(reconciliation_sql(engines), conn).dicts()


def mismatches(rows: list[dict], engines: tuple[str, ...] = ("iceberg",),
               tolerance: float = 0.0) -> list[tuple[int, str, str, float | None]]:
    out = []
    for row in rows:
        for e in engines:
            for m in METRICS:
                delta = row.get(f"delta_{e}_{m}")
                if delta is None or abs(float(delta)) > tolerance:
                    out.append((row["page_type"], e, m, None if delta is None else float(delta)))
    return out


def main() -> int:
    engines = ("iceberg", "clickhouse") if "--clickhouse" in sys.argv else ("iceberg",)
    rows = reconcile(engines=engines)
    print(f"reconciliation: {len(rows)} page types x {len(METRICS)} metrics x {len(engines)} engines")
    for row in rows:
        print(f"  page_type {row['page_type']}:")
        for m in METRICS:
            cells = "  ".join(f"{e}={row[f'{e}_{m}']!s:<8} delta={row[f'delta_{e}_{m}']}" for e in engines)
            print(f"    {m:<24} gold={row[f'gold_{m}']!s:<8} {cells}")
    bad = mismatches(rows, engines)
    print(f"mismatches: {len(bad)}")
    for page_type, e, m, delta in bad:
        print(f"  page_type {page_type} {e} {m}: delta {delta}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
