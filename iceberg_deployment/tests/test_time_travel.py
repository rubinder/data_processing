"""Snapshots, time travel, and rollback against a real table."""
from iceberg_deployment import impressions, time_travel as tt


def test_every_write_creates_a_snapshot(spark, table):
    before = len(tt.list_snapshots(spark, table))
    impressions.seed(spark, table, count=10, seed_value=1)
    after = tt.list_snapshots(spark, table)
    assert len(after) == before + 1
    assert after[-1]["operation"] == "append"
    assert int(after[-1]["added_records"]) > 0


def test_time_travel_returns_the_old_row_count(spark, table):
    """The core guarantee: an old snapshot is still fully readable."""
    original = spark.table(table).count()
    snapshot = tt.current_snapshot_id(spark, table)

    impressions.seed(spark, table, count=25, seed_value=2)
    assert spark.table(table).count() > original

    historical = tt.read_at_snapshot(spark, table, snapshot).count()
    assert historical == original


def test_rollback_restores_the_previous_state(spark, table):
    """A bad write is undone by moving the pointer, not by restoring data."""
    good = tt.current_snapshot_id(spark, table)
    good_count = spark.table(table).count()

    impressions.seed(spark, table, count=40, seed_value=3)   # the "bad" write
    assert spark.table(table).count() != good_count

    tt.rollback_to_snapshot(spark, table, good)
    assert spark.table(table).count() == good_count


def test_rollback_is_itself_auditable(spark, table):
    """Rollback appends a snapshot rather than erasing history."""
    good = tt.current_snapshot_id(spark, table)
    impressions.seed(spark, table, count=10, seed_value=4)
    before = len(tt.list_snapshots(spark, table))

    tt.rollback_to_snapshot(spark, table, good)

    assert len(tt.list_snapshots(spark, table)) >= before
    ids = {s["snapshot_id"] for s in tt.list_snapshots(spark, table)}
    assert good in ids, "the snapshot rolled back to must remain in history"


def test_deleted_rows_are_still_visible_in_the_prior_snapshot(spark, table):
    """Time travel covers deletes, which is what makes it useful in incidents."""
    from iceberg_deployment import upserts

    before_delete = tt.current_snapshot_id(spark, table)
    total = spark.table(table).count()

    upserts.delete_where(spark, table, "page_type = 1")
    assert spark.table(table).count() < total

    assert tt.read_at_snapshot(spark, table, before_delete).count() == total
