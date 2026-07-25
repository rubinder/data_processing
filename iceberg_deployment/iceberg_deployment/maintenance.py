"""Table maintenance: compaction, snapshot expiry, orphan cleanup.

An Iceberg table left unmaintained degrades in three separate ways, and each
has its own procedure. This is the part of "adopting a table format" that gets
skipped and then becomes an incident six months later.

**Small files.** Streaming or per-batch writes produce many small data files.
Every one costs a manifest entry, a file open, and a footer read at plan time.
This repo already fights the same problem in ``spark_applications`` (see
``_compact`` in ``utils/storage.py``); Iceberg gives it a first-class procedure
instead of a repartition-before-write trick.

**Delete files.** With merge-on-read, every ``MERGE``/``DELETE`` writes delete
files that readers must apply. They accumulate, and read cost grows with them.
Compaction is what converts them back into rewritten data files.

**Snapshot history.** Every commit retains its data files so time travel works.
Retained forever, that is unbounded storage growth. ``expire_snapshots`` is the
retention policy -- and it is the operation that makes rolled-back data
genuinely unrecoverable, so the expiry window *is* the recovery window. Setting
it to 7 days means you have 7 days to notice a bad write.
"""

#: Sensible defaults. The retention window is a deliberate trade between
#: storage cost and how long you have to notice a bad write.
DEFAULT_SNAPSHOT_RETENTION_DAYS = 7
DEFAULT_MIN_SNAPSHOTS_TO_KEEP = 5


def compact(spark, table: str, catalog: str = "local",
            target_file_size_bytes: int = 134_217_728) -> dict:
    """Rewrite small data files into larger ones, applying pending deletes.

    Returns the procedure's own summary, which reports how many files were
    rewritten -- worth logging, because a compaction that rewrites nothing is
    a scheduled job burning cluster time for no reason.
    """
    rows = spark.sql(
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{table}', "
        f"options => map("
        f"'target-file-size-bytes','{target_file_size_bytes}',"
        f"'min-input-files','2'))"
    ).collect()
    if not rows:
        return {}
    row = rows[0].asDict()
    return {
        "rewritten_data_files": row.get("rewritten_data_files_count"),
        "added_data_files": row.get("added_data_files_count"),
        "rewritten_bytes": row.get("rewritten_bytes_count"),
    }


def rewrite_manifests(spark, table: str, catalog: str = "local") -> None:
    """Reorganize manifest files so planning prunes better.

    Manifests accumulate in write order, which over time stops matching the
    partition layout. Rewriting them clusters entries so the planner can skip
    whole manifests instead of reading them to discover irrelevance.
    """
    spark.sql(f"CALL {catalog}.system.rewrite_manifests('{table}')")


def expire_snapshots(spark, table: str, older_than: str,
                     catalog: str = "local",
                     retain_last: int = DEFAULT_MIN_SNAPSHOTS_TO_KEEP) -> dict:
    """Drop snapshots older than ``older_than``, keeping at least ``retain_last``.

    ``retain_last`` is a floor that protects against the obvious footgun: an
    expiry window shorter than the gap between writes would otherwise leave a
    table with no history at all.

    This deletes the underlying data files that no live snapshot references, so
    it is the point of no return for anything rolled back.
    """
    rows = spark.sql(
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"retain_last => {retain_last})"
    ).collect()
    if not rows:
        return {}
    row = rows[0].asDict()
    return {
        "deleted_data_files": row.get("deleted_data_files_count"),
        "deleted_manifest_files": row.get("deleted_manifest_files_count"),
    }


def remove_orphan_files(spark, table: str, older_than: str,
                        catalog: str = "local") -> int:
    """Delete files in the table's directory that no metadata references.

    Orphans come from failed or killed writes: the data landed, the commit
    never happened, so nothing points at it and nothing will ever clean it up.

    The ``older_than`` guard is not optional in practice -- run this with a
    recent cutoff while a write is in flight and it will delete that write's
    files out from under it. A cutoff of at least a few days is the norm.
    """
    rows = spark.sql(
        f"CALL {catalog}.system.remove_orphan_files("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}')"
    ).collect()
    return len(rows)


def file_stats(spark, table: str) -> dict:
    """Current file counts and sizes -- the input to deciding on compaction."""
    data = spark.sql(
        f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes, "
        f"sum(record_count) AS records FROM {table}.files"
    ).collect()[0]
    return {
        "data_files": data["files"],
        "bytes": data["bytes"],
        "records": data["records"],
        "avg_file_bytes": (
            int(data["bytes"] / data["files"]) if data["files"] else 0
        ),
    }
