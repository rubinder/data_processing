"""Compaction and snapshot expiry against a real table."""
from datetime import datetime, timedelta

from iceberg_deployment import impressions, maintenance, time_travel as tt


def test_compaction_reduces_file_count(spark, empty_table):
    """Many small writes make many small files; compaction merges them.

    This is the small-files problem the repo's Spark jobs already work around
    with a repartition-before-write; Iceberg makes it a maintenance procedure
    that can run after the fact.
    """
    for i in range(6):
        impressions.seed(spark, empty_table, count=10, seed_value=i)
    before = maintenance.file_stats(spark, empty_table)
    assert before["data_files"] > 1

    result = maintenance.compact(spark, empty_table)
    after = maintenance.file_stats(spark, empty_table)

    assert after["data_files"] < before["data_files"], result
    assert after["records"] == before["records"], "compaction changed row count"


def test_compaction_preserves_query_results(spark, empty_table):
    for i in range(4):
        impressions.seed(spark, empty_table, count=10, seed_value=i)
    before = spark.sql(
        f"SELECT page_type, count(*) AS n FROM {empty_table} "
        f"GROUP BY page_type ORDER BY page_type"
    ).collect()

    maintenance.compact(spark, empty_table)

    after = spark.sql(
        f"SELECT page_type, count(*) AS n FROM {empty_table} "
        f"GROUP BY page_type ORDER BY page_type"
    ).collect()
    assert [r.asDict() for r in after] == [r.asDict() for r in before]


def test_expire_snapshots_keeps_a_floor(spark, table):
    """Expiry must not be able to leave a table with no history at all."""
    for i in range(4):
        impressions.seed(spark, table, count=5, seed_value=100 + i)
    assert len(tt.list_snapshots(spark, table)) >= 5

    future = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    maintenance.expire_snapshots(spark, table, older_than=future, retain_last=3)

    remaining = tt.list_snapshots(spark, table)
    assert len(remaining) == 3, "retain_last floor was not honoured"
    assert spark.table(table).count() > 0


def test_file_stats_reports_averages(spark, table):
    stats = maintenance.file_stats(spark, table)
    assert stats["data_files"] > 0
    assert stats["records"] > 0
    assert stats["avg_file_bytes"] > 0
