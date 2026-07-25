"""Snapshots, time travel, and rollback.

Every Iceberg write produces a **snapshot**: an immutable, atomically committed
list of the data files that constitute the table at that instant. Readers pin a
snapshot for the duration of a query, so a long-running read never sees a
half-finished write, and a failed write leaves nothing behind to clean up.

Two capabilities fall out of that, and both are answers to problems the other
storage engines in this repository cannot solve well:

**Time travel.** Query the table as of a snapshot or a timestamp. This is what
makes an incident answerable after the fact -- "what did the dashboard show
before the backfill?" -- and what makes a rollup reproducible: pin the snapshot
ID in the job and the aggregate is deterministic no matter what has landed
since.

**Rollback.** A bad write is undone by moving the table pointer back to a
previous snapshot. It is a metadata operation, not a restore: no data is
copied, and it takes the same time on a 10 GB table as on a 10 TB one.

The ClickHouse module in this repo has neither. When an aggregate there drifts,
the remedy is to rebuild the affected partitions from raw data. That is a
reasonable trade for a serving layer optimized for read latency, but it is the
reason a lakehouse table sits underneath one in most architectures.
"""


def list_snapshots(spark, table: str) -> list[dict]:
    """Snapshot history, oldest first: id, parent, timestamp, operation."""
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, committed_at, operation, summary "
        f"FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [
        {
            "snapshot_id": r["snapshot_id"],
            "parent_id": r["parent_id"],
            "committed_at": r["committed_at"],
            "operation": r["operation"],
            "added_records": (r["summary"] or {}).get("added-records"),
            "deleted_records": (r["summary"] or {}).get("deleted-records"),
        }
        for r in rows
    ]


def current_snapshot_id(spark, table: str) -> int:
    """The snapshot the table currently points at."""
    return spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]["snapshot_id"]


def read_at_snapshot(spark, table: str, snapshot_id: int):
    """Read the table exactly as it was at ``snapshot_id``.

    ``VERSION AS OF`` is the SQL form; the DataFrame form takes a
    ``snapshot-id`` read option. Either way the planner resolves the file list
    from that snapshot's manifest rather than the current one.
    """
    return spark.sql(f"SELECT * FROM {table} VERSION AS OF {snapshot_id}")


def read_at_timestamp(spark, table: str, timestamp: str):
    """Read the table as of a wall-clock time.

    Resolves to the most recent snapshot committed at or before the timestamp,
    which is the form you actually want during an incident -- you know when
    something looked wrong, not which snapshot ID was current.
    """
    return spark.sql(f"SELECT * FROM {table} TIMESTAMP AS OF '{timestamp}'")


def rollback_to_snapshot(spark, table: str, snapshot_id: int,
                         catalog: str = "local") -> None:
    """Move the table pointer back to ``snapshot_id``.

    Note this *adds* a snapshot rather than deleting the intervening ones: the
    history remains auditable, and the rollback itself can be rolled back. The
    bad data becomes unreachable, not unrecoverable, until ``expire_snapshots``
    removes it (see maintenance.py).
    """
    spark.sql(
        f"CALL {catalog}.system.rollback_to_snapshot('{table}', {snapshot_id})"
    )


def cherrypick(spark, table: str, snapshot_id: int,
               catalog: str = "local") -> None:
    """Apply a single staged snapshot onto the current table state.

    Used with write-audit-publish: stage a write, validate it while it is
    invisible to readers, then publish exactly that snapshot. It is how you get
    a quality gate *between* writing and exposing data, rather than after.
    """
    spark.sql(
        f"CALL {catalog}.system.cherrypick_snapshot('{table}', {snapshot_id})"
    )
