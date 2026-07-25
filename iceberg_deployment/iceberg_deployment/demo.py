"""End-to-end walkthrough: the four capabilities, printed as they happen.

Run with ``./deploy.sh demo``. Uses the local filesystem catalog by default, so
it needs no services -- set ``ICEBERG_CATALOG_TYPE=rest`` to run the same thing
against the REST catalog on S3.

Each step prints before/after state, so the output is the evidence.
"""
import argparse
import shutil
import tempfile
from datetime import datetime, timedelta

from iceberg_deployment import (
    impressions,
    maintenance,
    schema_evolution as evo,
    time_travel as tt,
    upserts,
)
from iceberg_deployment.session import get_spark_session

TABLE = "db.impressions_demo"


def banner(text: str) -> None:
    print(f"\n{'=' * 72}\n{text}\n{'=' * 72}", flush=True)


def run(warehouse: str | None = None) -> None:
    spark = get_spark_session("iceberg-demo", warehouse=warehouse)
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    impressions.create_table(spark, TABLE)

    banner("1. Seed the table")
    written = impressions.seed(spark, TABLE, count=120)
    print(f"wrote {written} rows")
    print(f"schema: {[c for c, _ in evo.current_schema(spark, TABLE)]}")
    print(f"files:  {maintenance.file_stats(spark, TABLE)}")
    baseline = tt.current_snapshot_id(spark, TABLE)
    baseline_rows = spark.table(TABLE).count()

    banner("2. Schema evolution — metadata-only, safe on existing data")
    ids_before = evo.field_ids(spark, TABLE)
    files_before = spark.sql(f"SELECT file_path FROM {TABLE}.files").count()

    evo.rename_column(spark, TABLE, "page_type", "page_category")
    evo.add_column(spark, TABLE, "intent", "STRING", comment="added later")

    ids_after = evo.field_ids(spark, TABLE)
    files_after = spark.sql(f"SELECT file_path FROM {TABLE}.files").count()
    print(f"renamed page_type -> page_category; field ID "
          f"{ids_before['page_type']} -> {ids_after['page_category']} (unchanged)")
    print(f"data files before/after: {files_before}/{files_after} "
          f"(no rewrite)")
    rows = spark.sql(
        f"SELECT page_category, count(*) AS n FROM {TABLE} "
        f"GROUP BY page_category ORDER BY page_category"
    ).collect()
    print(f"old rows still readable under the new name: "
          f"{[(r['page_category'], r['n']) for r in rows]}")

    banner("3. Partition spec evolution — days() -> hours(), no rewrite")
    evo.evolve_partition_spec(spark, TABLE, add="hours(event_ts)",
                              drop="days(event_ts)")
    impressions.seed(spark, TABLE, count=40, seed_value=99)
    specs = sorted({r["spec_id"] for r in
                    spark.sql(f"SELECT spec_id FROM {TABLE}.files").collect()})
    print(f"data files now span partition specs {specs}; "
          f"total rows {spark.table(TABLE).count()}")

    banner("4. MERGE INTO — replay is idempotent, corrections apply in place")
    batch = [("user_0001", "imp_000000", 9,
              datetime(2026, 6, 1, 10, 0, 0), "a")]
    upserts.upsert_rows(spark, TABLE, batch)
    once = spark.table(TABLE).count()
    upserts.upsert_rows(spark, TABLE, batch)          # replay
    twice = spark.table(TABLE).count()
    print(f"rows after first apply: {once}; after replaying the same batch: "
          f"{twice}  -> {'idempotent' if once == twice else 'DUPLICATED'}")

    banner("5. Time travel and rollback")
    for snap in tt.list_snapshots(spark, TABLE)[-4:]:
        print(f"  {snap['committed_at']}  {snap['operation']:12s} "
              f"id={snap['snapshot_id']}")
    print(f"current rows: {spark.table(TABLE).count()}")
    print(f"rows at the first snapshot: "
          f"{tt.read_at_snapshot(spark, TABLE, baseline).count()}")
    tt.rollback_to_snapshot(spark, TABLE, baseline)
    print(f"after rollback to the first snapshot: "
          f"{spark.table(TABLE).count()} (expected {baseline_rows})")

    banner("6. Maintenance — compaction and snapshot expiry")
    for i in range(5):
        impressions.seed(spark, TABLE, count=8, seed_value=200 + i)
    before = maintenance.file_stats(spark, TABLE)
    result = maintenance.compact(spark, TABLE)
    after = maintenance.file_stats(spark, TABLE)
    print(f"data files {before['data_files']} -> {after['data_files']}, "
          f"records preserved: {before['records']} -> {after['records']}")
    print(f"rewrite summary: {result}")

    cutoff = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    expired = maintenance.expire_snapshots(spark, TABLE, cutoff, retain_last=3)
    print(f"snapshots retained: {len(tt.list_snapshots(spark, TABLE))} "
          f"(expiry removed {expired})")

    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    spark.stop()
    print("\nDone.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", default=None,
                        help="warehouse dir (default: a temp dir)")
    parser.add_argument("--keep", action="store_true",
                        help="keep the warehouse directory afterwards")
    args = parser.parse_args()

    warehouse = args.warehouse or tempfile.mkdtemp(prefix="iceberg-demo-")
    try:
        run(warehouse)
    finally:
        if not args.keep and not args.warehouse:
            shutil.rmtree(warehouse, ignore_errors=True)


if __name__ == "__main__":
    main()
