"""The module end to end, printed: catalogs, seed from PostgreSQL raw, the
four analyses over Iceberg, the reconciliation against gold, least privilege
through federation, time travel, pushdown."""
from __future__ import annotations

import sys

from trino_deployment import analyses, client, federation, pushdown, seed, timetravel


def main() -> int:
    conn = client.connect()
    print("=== 1. catalogs")
    for (name,) in client.query("SHOW CATALOGS", conn).rows:
        print(f"  {name}")

    print("\n=== 2. seed iceberg.db.impressions from postgres_raw.raw.impressions")
    seed.create(conn)
    seed.truncate(conn)
    n = seed.from_postgres(conn)
    print(f"  inserted {n} rows; malformed-date rows skipped by try_cast")
    print(f"  table now holds {seed.row_count(conn)} rows")

    print("\n=== 3. the four analyses over Iceberg, through Trino")
    for name in analyses.ANALYSES:
        result = analyses.run(name, conn)
        print(f"  {name}: {len(result.rows)} rows, columns {result.columns[:4]} ...")
    summary = analyses.run("page_type_summary", conn)
    for row in summary.rows:
        print(f"    {row[:6]}")

    print("\n=== 4. reconcile Trino-over-Iceberg against dbt gold in PostgreSQL, one query")
    rows = federation.reconcile(conn)
    bad = federation.mismatches(rows)
    for row in rows:
        print(f"  page_type {row['page_type']}: total_impressions iceberg={row['iceberg_total_impressions']} "
              f"gold={row['gold_total_impressions']}, pct_reaching_d iceberg={row['iceberg_pct_reaching_d']} "
              f"gold={row['gold_pct_reaching_d']}")
    print(f"  {len(rows)} page types x {len(federation.METRICS)} metrics: {len(bad)} mismatches")
    for page_type, metric, delta in bad:
        print(f"    page_type {page_type} {metric}: delta {delta}")

    print("\n=== 5. least privilege survives federation")
    for label, sql in (("gold role reading raw", "SELECT count(*) FROM postgres_gold.raw.impressions"),
                       ("raw role reading gold", "SELECT count(*) FROM postgres_raw.gold.page_type_summary")):
        try:
            client.query(sql, conn)
            print(f"  {label}: ALLOWED  <-- unexpected")
        except Exception as exc:  # noqa: BLE001 -- the point is the message
            msg = str(exc).split("\n")[0]
            print(f"  {label}: denied ({msg[:90]})")

    print("\n=== 6. time travel")
    before = seed.row_count(conn)
    seed.synthetic(20, seed_value=99, conn=conn)
    snaps = timetravel.snapshots(conn=conn)
    print(f"  snapshots: {len(snaps)}; rows now {seed.row_count(conn)}, "
          f"rows at previous snapshot {timetravel.count_at(snaps[-2]['snapshot_id'], conn=conn)} "
          f"(was {before})")
    client.query(f"DELETE FROM {seed.TABLE} WHERE impression_id LIKE 'b99_%'", conn)
    print(f"  after DELETE (merge-on-read, v2): rows {seed.row_count(conn)}, "
          f"snapshots {len(timetravel.snapshots(conn=conn))}")

    print("\n=== 7. pushdown into PostgreSQL")
    for label, sql in (("pushed", pushdown.PUSHED), ("not pushed", pushdown.NOT_PUSHED)):
        plan = client.explain(sql, conn)
        ms, value = pushdown.timed(sql, conn=conn)
        print(f"  {label:<11} result={value} best-of-5 {ms:.1f} ms  "
              f"plan pushed={pushdown.plan_is_pushed(plan)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
