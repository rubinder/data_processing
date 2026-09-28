"""What Trino pushes into PostgreSQL, and what it does not.

Two queries that return the same number. The first filters on a column
the JDBC connector can express in the remote SQL, so the plan is a bare
``TableScan``: PostgreSQL does the filtering and the count. The second
filters on ``CAST(page_type AS varchar)``, which the connector cannot
push, so a ``ScanFilterProject`` appears above the scan and every row
crosses the wire to be filtered in Trino. Same result, different plan,
and on a real table a very different cost.
"""
from __future__ import annotations

import sys
import time

from trino_deployment import client

GOLD_HOURLY = "postgres_gold.gold.hourly_traffic"

PUSHED = f"SELECT count(*) FROM {GOLD_HOURLY} WHERE page_type = 3"
NOT_PUSHED = f"SELECT count(*) FROM {GOLD_HOURLY} WHERE CAST(page_type AS varchar) = '3'"
ICEBERG_PRUNED = ("SELECT count(*) FROM iceberg.db.impressions "
                  "WHERE event_ts >= TIMESTAMP '2026-01-01 00:00:00' "
                  "AND event_ts < TIMESTAMP '2026-01-02 00:00:00' AND page_type = 3")


def plan_is_pushed(plan: str) -> bool:
    """True when no filter or aggregation runs in Trino above the scan.

    A fully pushed JDBC query plans as ``TableScan`` (the connector rewrote
    the whole thing) with no ``ScanFilter``, ``Filter`` or ``Aggregate``
    node above it.
    """
    lines = plan.splitlines()
    return (any("TableScan" in ln for ln in lines)
            and not any(("ScanFilter" in ln) or ("Filter[" in ln) or ("Aggregate[" in ln)
                        for ln in lines))


def timed(sql: str, runs: int = 5, conn=None) -> tuple[float, object]:
    conn = conn or client.connect()
    best = None
    value = None
    for _ in range(runs):
        started = time.perf_counter()
        value = client.query(sql, conn).rows[0][0]
        elapsed = (time.perf_counter() - started) * 1000
        best = elapsed if best is None else min(best, elapsed)
    return best, value


def main() -> int:
    conn = client.connect()
    for label, sql in (("pushed", PUSHED), ("not pushed", NOT_PUSHED)):
        plan = client.explain(sql, conn)
        ms, value = timed(sql, conn=conn)
        print(f"== {label}: {sql}")
        print(f"   result={value}  best of 5: {ms:.1f} ms  pushed={plan_is_pushed(plan)}")
        for ln in plan.splitlines():
            if any(k in ln for k in ("TableScan", "ScanFilter", "Filter[", "Aggregate[")):
                print("   " + ln.strip()[:140])
    plan = client.explain(ICEBERG_PRUNED, conn)
    ms, value = timed(ICEBERG_PRUNED, conn=conn)
    print(f"== iceberg partition filter: result={value}  best of 5: {ms:.1f} ms")
    for ln in plan.splitlines():
        if "ScanFilter" in ln or "TableScan" in ln or "constraint" in ln.lower():
            print("   " + ln.strip()[:160])
    return 0


if __name__ == "__main__":
    sys.exit(main())
