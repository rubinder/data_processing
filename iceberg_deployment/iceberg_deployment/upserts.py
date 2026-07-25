"""Idempotent upserts with MERGE INTO.

The repository's CDC path (Debezium -> Kafka -> Flink) is at-least-once: a
crash between processing and offset commit replays the tail of the stream. Two
ways to make that harmless:

* **Dedupe at the storage layer.** ClickHouse's ReplacingMergeTree collapses
  duplicate rows on merge -- eventually, and only for rows identical on the
  whole sorting key.
* **Dedupe at write time.** ``MERGE INTO`` matches on a key and updates in
  place, so replaying a batch converges to the same table state rather than
  appending duplicates that must be cleaned up later.

The second is what a lakehouse table gives you and a plain append-only Parquet
directory does not. It is also the only one of the two that lets a *correction*
(a late-arriving fix to an already-written row) be expressed directly.

## Why format-version 2 matters here

With v2 merge-on-read, an update writes a small **delete file** marking the old
row dead plus a new data file with the new value -- it does not rewrite the
data files that contained the old row. That makes a per-batch MERGE cheap
enough to run continuously. The cost moves to read time (readers must apply
delete files) and is reclaimed by compaction; see ``maintenance.py``.

Copy-on-write is the alternative: rewrite whole data files on every update.
Cheaper reads, far more expensive writes. Choose per table by write frequency
-- frequent small updates want merge-on-read, rare bulk rewrites want
copy-on-write.
"""

#: The grain at which an impression event is unique in this repo's data model.
#: Getting this wrong is the classic MERGE bug: too coarse and updates clobber
#: unrelated rows, too fine and every replay inserts instead of updating.
DEFAULT_MERGE_KEYS = ("impression_id", "user_id", "event_type")


def build_merge_sql(spark, target: str, source: str,
                    keys: tuple[str, ...] = DEFAULT_MERGE_KEYS) -> str:
    """Compose MERGE INTO from the table's current columns.

    Derived rather than hardcoded so the statement survives a renamed column,
    which is the whole point of the schema-evolution work next door.
    """
    columns = [f.name for f in spark.table(target).schema.fields]
    key_cols = [k for k in keys if k in columns]
    if not key_cols:
        raise ValueError(
            f"none of the merge keys {keys} exist on {target}: {columns}"
        )
    on = "\n  AND ".join(f"t.{k} = s.{k}" for k in key_cols)
    updates = ",\n    ".join(
        f"t.{c} = s.{c}" for c in columns if c not in key_cols
    )
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source} AS s\n"
        f"ON  {on}\n"
        f"WHEN MATCHED THEN UPDATE SET\n    {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def upsert(spark, target: str, source_df,
           source_view: str = "_upsert_source",
           keys: tuple[str, ...] = DEFAULT_MERGE_KEYS) -> None:
    """Merge ``source_df`` into ``target`` on the natural key."""
    source_df.createOrReplaceTempView(source_view)
    spark.sql(build_merge_sql(spark, target, source_view, keys))


def upsert_rows(spark, target: str, rows: list[tuple],
                keys: tuple[str, ...] = DEFAULT_MERGE_KEYS) -> None:
    """Convenience wrapper for a literal list of rows."""
    from iceberg_deployment.impressions import rows_to_df

    upsert(spark, target, rows_to_df(spark, target, rows), keys=keys)


DELETE_SQL = "DELETE FROM {target} WHERE {predicate}"


def delete_where(spark, target: str, predicate: str) -> None:
    """Row-level delete.

    Worth noting because it is genuinely hard without a table format: on plain
    partitioned Parquet the only unit you can delete is a whole partition, so
    "erase this user" becomes "rewrite every partition they appear in". Here it
    is a predicate, and with merge-on-read it writes delete files rather than
    rewriting data.
    """
    spark.sql(DELETE_SQL.format(target=target, predicate=predicate))
