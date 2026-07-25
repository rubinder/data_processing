"""Schema evolution must be safe on data written before the change.

These are the assertions that matter: not "the ALTER succeeded" but "data
written under the old schema still reads correctly under the new one". That is
the property a Hive-style table does not have, and the reason for adopting a
table format.
"""
import pytest

from iceberg_deployment import schema_evolution as evo


def test_rename_preserves_existing_data(spark, table):
    """The operation that silently returns NULLs on a Hive table.

    Rows are written, the column is renamed, and the *old* rows must still
    return their values under the new name -- because Iceberg resolves by field
    ID, and the ID did not change.
    """
    before = {r["page_type"] for r in
              spark.sql(f"SELECT DISTINCT page_type FROM {table}").collect()}
    assert before == {1, 2, 3}

    evo.rename_column(spark, table, "page_type", "page_category")

    after = {r["page_category"] for r in
             spark.sql(f"SELECT DISTINCT page_category FROM {table}").collect()}
    assert after == before, "renaming lost data written before the rename"
    assert "page_type" not in dict(evo.current_schema(spark, table))


def test_rename_keeps_the_field_id(spark, table):
    """Field ID identity is the mechanism; assert it directly."""
    before = evo.field_ids(spark, table)
    evo.rename_column(spark, table, "page_type", "page_category")
    after = evo.field_ids(spark, table)
    assert after["page_category"] == before["page_type"]


def test_added_column_is_null_for_old_rows(spark, table):
    """A new column reads NULL on old data -- correct, not a gap."""
    evo.add_column(spark, table, "intent", "STRING", comment="added later")
    rows = spark.sql(
        f"SELECT count(*) AS n FROM {table} WHERE intent IS NOT NULL"
    ).collect()
    assert rows[0]["n"] == 0
    assert "intent" in dict(evo.current_schema(spark, table))


def test_drop_then_readd_does_not_resurrect_old_values(spark, empty_table):
    """The subtlest Hive failure: old files still hold the dropped column.

    Iceberg retires the field ID on drop, so re-adding the same *name* gets a
    new ID and cannot read the old values.
    """
    from datetime import datetime

    evo.add_column(spark, empty_table, "intent", "STRING")
    spark.sql(
        f"INSERT INTO {empty_table} VALUES "
        f"('u1','i1',1,TIMESTAMP '2026-06-01 10:00:00','a','refund')"
    )
    assert spark.sql(
        f"SELECT count(*) AS n FROM {empty_table} WHERE intent = 'refund'"
    ).collect()[0]["n"] == 1

    evo.drop_column(spark, empty_table, "intent")
    evo.add_column(spark, empty_table, "intent", "STRING")

    resurrected = spark.sql(
        f"SELECT count(*) AS n FROM {empty_table} WHERE intent IS NOT NULL"
    ).collect()[0]["n"]
    assert resurrected == 0, "dropped column's values came back after re-add"
    assert isinstance(datetime.now(), datetime)


def test_widening_a_type_is_allowed(spark, table):
    """int -> bigint keeps every existing value representable."""
    evo.widen_column(spark, table, "page_type", "bigint")
    assert dict(evo.current_schema(spark, table))["page_type"] == "bigint"
    assert spark.table(table).count() > 0


def test_narrowing_a_type_is_rejected(spark, table):
    """bigint -> int could invalidate stored values, so it must fail loudly.

    A format that allowed this would be trading a crash for silent truncation.
    """
    evo.widen_column(spark, table, "page_type", "bigint")
    with pytest.raises(Exception):
        evo.widen_column(spark, table, "page_type", "int")


def test_schema_change_is_metadata_only(spark, table):
    """Evolution must not rewrite data files -- that is what makes it cheap."""
    files_before = spark.sql(f"SELECT file_path FROM {table}.files").count()
    evo.add_column(spark, table, "intent", "STRING")
    evo.rename_column(spark, table, "event_type", "event_step")
    files_after = spark.sql(f"SELECT file_path FROM {table}.files").count()
    assert files_after == files_before


def test_partition_spec_evolves_without_rewriting_data(spark, table):
    """Old files keep their spec; new files use the new one; reads span both.

    No Hive-layout equivalent exists: there, the partitioning *is* the
    directory structure, so changing it means rewriting the table.
    """
    from iceberg_deployment import impressions

    original = spark.table(table).count()

    evo.evolve_partition_spec(spark, table, add="hours(event_ts)",
                              drop="days(event_ts)")
    added = impressions.seed(spark, table, count=20,
                             start=None, seed_value=99)

    specs = {r["spec_id"] for r in
             spark.sql(f"SELECT spec_id FROM {table}.files").collect()}
    assert len(specs) >= 2, "expected data files under two partition specs"
    assert spark.table(table).count() == original + added


def test_reads_are_correct_across_a_rename_and_an_add(spark, table):
    """End to end: evolve twice, then verify every row still reconciles."""
    total = spark.table(table).count()
    evo.rename_column(spark, table, "page_type", "page_category")
    evo.add_column(spark, table, "intent", "STRING")
    evo.rename_column(spark, table, "event_type", "event_step")

    rows = spark.sql(
        f"SELECT page_category, count(*) AS n FROM {table} "
        f"GROUP BY page_category ORDER BY page_category"
    ).collect()
    assert sum(r["n"] for r in rows) == total
    assert [r["page_category"] for r in rows] == [1, 2, 3]
