"""MERGE INTO: replay must converge, not duplicate."""
from datetime import datetime

from iceberg_deployment import upserts


def _rows(page_type: int = 9):
    return [
        ("user_0001", "imp_000000", page_type,
         datetime(2026, 6, 1, 10, 0, 0), "a"),
        ("user_0002", "imp_000001", page_type,
         datetime(2026, 6, 1, 10, 5, 0), "b"),
    ]


def test_merge_replay_is_idempotent(spark, empty_table):
    """The property that makes at-least-once delivery survivable.

    Applying the same batch twice must leave the table identical -- this is
    exactly the crash-between-process-and-commit case the repo's CDC path
    produces.
    """
    upserts.upsert_rows(spark, empty_table, _rows())
    first = spark.table(empty_table).count()

    upserts.upsert_rows(spark, empty_table, _rows())
    assert spark.table(empty_table).count() == first, "replay inserted duplicates"


def test_merge_updates_matched_rows_in_place(spark, empty_table):
    upserts.upsert_rows(spark, empty_table, _rows(page_type=1))
    upserts.upsert_rows(spark, empty_table, _rows(page_type=7))

    values = {r["page_type"] for r in
              spark.sql(f"SELECT page_type FROM {empty_table}").collect()}
    assert values == {7}, "correction did not overwrite the earlier value"
    assert spark.table(empty_table).count() == 2


def test_merge_inserts_unmatched_rows(spark, empty_table):
    upserts.upsert_rows(spark, empty_table, _rows())
    new = [("user_0003", "imp_000002", 2,
            datetime(2026, 6, 1, 11, 0, 0), "c")]
    upserts.upsert_rows(spark, empty_table, new)
    assert spark.table(empty_table).count() == 3


def test_row_level_delete(spark, table):
    """Deleting by predicate, not by partition."""
    total = spark.table(table).count()
    ones = spark.sql(
        f"SELECT count(*) AS n FROM {table} WHERE page_type = 1"
    ).collect()[0]["n"]
    assert ones > 0

    upserts.delete_where(spark, table, "page_type = 1")

    assert spark.table(table).count() == total - ones
    assert spark.sql(
        f"SELECT count(*) AS n FROM {table} WHERE page_type = 1"
    ).collect()[0]["n"] == 0
