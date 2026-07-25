"""Schema and partition evolution -- the reason this module exists.

`Tasks.md` carried an open item: *"Adopt Delta/Iceberg with column mapping and a
deliberate schema-evolution policy for evolving tables."* This is that item.

## Why schema evolution is dangerous without a table format

A Hive-style table is a directory of Parquet files plus a metastore schema.
Parquet stores column *names* and *positions*. Nothing binds a file written
last year to the schema registered today, so evolution goes wrong in ways that
do not raise an error:

* **Rename** — the metastore says ``page_category``, old files say
  ``page_type``. Reads of old files return NULL for the column. Silently.
* **Drop then re-add** with the same name — old files still contain the old
  column, and their values reappear under the new column's meaning.
* **Reorder** — anything reading positionally now maps the wrong values to the
  wrong columns.
* **Add in the middle** — same problem, plus every downstream ``SELECT *``
  consumer shifts.

Each of these produces plausible-looking wrong data, which is worse than a
crash: nobody notices until a number is questioned in a meeting.

## What Iceberg does instead

Every column gets a permanent, immutable **field ID** on creation. Data files
record field IDs, not names. The schema maps IDs to current names. So:

* a rename changes the name for ID 3 and nothing else -- old files still
  resolve, because they were never storing the name to begin with;
* a drop retires the ID permanently, so re-adding the same *name* allocates a
  *new* ID and cannot resurrect old values;
* reordering is a metadata change; positions were never load-bearing.

All of it is a metadata-only commit: no data is rewritten, and the operation is
atomic and instantly reversible via the snapshot it creates.

## The compatibility policy this module enforces

Iceberg permits these safely, and they are what a well-behaved producer should
be limited to:

    ADD COLUMN (nullable, at any position)   RENAME COLUMN
    DROP COLUMN                              REORDER COLUMNS
    WIDEN TYPE (int->bigint, float->double, decimal precision increase)

Deliberately *not* permitted, because they cannot be applied to already-written
files and so are not metadata-only:

    narrowing a type (bigint->int)           making a nullable column required
    changing an unrelated type (string->int)

The rule of thumb: a change that could invalidate an existing value is a new
column plus a backfill, never an in-place alter.
"""

#: The set of alterations that are metadata-only and always safe.
SAFE_OPERATIONS = (
    "add_column",
    "rename_column",
    "drop_column",
    "reorder_column",
    "widen_type",
)

#: Widenings Iceberg accepts. Anything not listed here needs a new column.
SAFE_TYPE_PROMOTIONS = {
    ("int", "bigint"),
    ("float", "double"),
    ("decimal", "decimal"),  # precision increase only, scale must match
}


def add_column(spark, table: str, name: str, sql_type: str,
               comment: str | None = None, after: str | None = None) -> None:
    """Add a nullable column. Metadata-only; existing files are untouched.

    New columns must be nullable: existing rows have no value for them, and
    Iceberg will not let you claim otherwise. Reads of old data files return
    NULL for the new field ID, which is correct rather than a gap.
    """
    statement = f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}"
    if comment:
        statement += f" COMMENT '{comment}'"
    if after:
        statement += f" AFTER {after}"
    spark.sql(statement)


def rename_column(spark, table: str, old: str, new: str) -> None:
    """Rename a column. The field ID is unchanged, so old files still resolve.

    This is the operation that silently corrupts a Hive table and is a no-op
    risk here.
    """
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")


def drop_column(spark, table: str, name: str) -> None:
    """Drop a column. The field ID is retired and never reused."""
    spark.sql(f"ALTER TABLE {table} DROP COLUMN {name}")


def widen_column(spark, table: str, name: str, new_type: str) -> None:
    """Widen a column's type (e.g. int -> bigint).

    Safe because every existing value remains representable. The reverse is
    not, and Iceberg refuses it.
    """
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN {name} TYPE {new_type}")


def evolve_partition_spec(spark, table: str, add: str | None = None,
                          drop: str | None = None) -> None:
    """Change how new data is partitioned, without rewriting old data.

    This is the capability with no equivalent in a Hive layout, where the
    partitioning is the directory structure: changing it means rewriting the
    table.

    Iceberg versions the partition spec and stamps each data file with the spec
    it was written under. Files written under ``days(event_ts)`` and files
    written under ``hours(event_ts)`` coexist, and the planner produces a split
    plan that prunes each group according to its own spec.

    The usual reason to do this is growth: daily partitions that were right at
    a million events a day are too coarse at a hundred million.
    """
    if drop:
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD {drop}")
    if add:
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD {add}")


def current_schema(spark, table: str) -> list[tuple[str, str]]:
    """Return [(column, type)] as the table currently defines it."""
    rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
    schema = []
    for row in rows:
        name = (row["col_name"] or "").strip()
        if not name or name.startswith("#") or name == "":
            break
        schema.append((name, (row["data_type"] or "").strip()))
    return schema


def field_ids(spark, table: str) -> dict[str, int]:
    """Map column name -> Iceberg field ID.

    Exposed because the field ID is the thing that makes evolution safe, and
    asserting on it is how the tests prove a rename kept the identity and a
    drop-then-re-add did not.
    """
    import json

    rows = spark.sql(
        f"SELECT * FROM {table}.metadata_log_entries "
        f"ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not rows:
        return {}
    with open(rows[0]["file"].replace("file:", "")) as handle:
        metadata = json.load(handle)
    current = metadata["current-schema-id"]
    for schema in metadata["schemas"]:
        if schema["schema-id"] == current:
            return {f["name"]: f["id"] for f in schema["fields"]}
    return {}
