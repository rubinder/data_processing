"""The engine protocol, and its Spark implementation over the Iceberg catalog.

Everything above this module talks to a ``LakehouseEngine``: metadata reads
(schema history with field IDs, per-snapshot row counts), Arrow scans, SQL
over aliased tables, and appends/overwrites. The tests run the same rules
through an in-memory fake; the real thing is a SparkSession from
``iceberg_deployment.session``, so the agent reads exactly the tables that
module creates, through exactly the catalog config it uses.

Two environment quirks shaped this file:

1. Py4J converts Iceberg TIMESTAMP values to Python ``datetime`` using the
   JVM's default timezone, not ``spark.sql.session.timeZone``. The JVM
   timezone is fixed at process start, so ``TZ`` is pinned to UTC before the
   session is built -- inside ``SparkEngine.__init__``, not at import time.
2. ``DataFrame.toPandas()`` needs pandas and goes through a conversion path
   that has broken across Python versions; ``collect()`` plus
   ``Row.asDict()`` is dependency-free and handles nested types.
"""
from __future__ import annotations

import os
import time
from typing import Protocol, runtime_checkable

import pyarrow as pa

from ops_agent.tables import TableDef


@runtime_checkable
class LakehouseEngine(Protocol):
    def create_table(self, table_def: TableDef) -> None: ...
    def table_exists(self, ident: str) -> bool: ...
    def arrow_schema(self, ident: str) -> pa.Schema: ...
    def append(self, ident: str, data: pa.Table) -> None: ...
    def overwrite(self, ident: str, data: pa.Table) -> None: ...
    def scan_arrow(self, ident: str, snapshot_id: int | None = None,
                   columns: list[str] | None = None) -> pa.Table: ...
    def snapshots(self, ident: str) -> list[int]: ...
    def snapshot_row_counts(self, ident: str) -> list[int]: ...
    def snapshot_details(self, ident: str) -> list[dict]: ...
    def schema_history(self, ident: str) -> list[dict]: ...
    def sql(self, query: str, tables: dict[str, str]) -> pa.Table: ...


NAMESPACES = ("db", "ops")


class SparkEngine:
    """``LakehouseEngine`` over the catalog ``iceberg_deployment`` configures.

    Pass an existing ``spark`` to share a session (the tests do); otherwise
    one is built from the same environment variables the Iceberg module
    reads (``ICEBERG_CATALOG_TYPE``, ``ICEBERG_WAREHOUSE``).
    """

    def __init__(self, spark=None, catalog: str | None = None):
        os.environ.setdefault("TZ", "UTC")
        if hasattr(time, "tzset"):
            time.tzset()
        from iceberg_deployment import session as iceberg_session

        if spark is None:
            spark = iceberg_session.get_spark_session("ops-agent")
        self.spark = spark
        self.catalog = catalog or iceberg_session.CATALOG
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        for namespace in NAMESPACES:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{namespace}")

    def qualified(self, ident: str) -> str:
        return f"{self.catalog}.{ident}"

    def _jtable(self, ident: str):
        """The underlying ``org.apache.iceberg.Table``.

        Spark's metadata tables (``snapshots``, ``history``) do not expose
        per-schema-version field IDs; the Java table does, without another
        SQL dialect in between.
        """
        return self.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            self.spark._jsparkSession, self.qualified(ident))

    # -- lifecycle -----------------------------------------------------------

    def create_table(self, table_def: TableDef) -> None:
        self.spark.sql(table_def.create_ddl(self.qualified(table_def.name)))

    def drop_table(self, ident: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {self.qualified(ident)} PURGE")

    def table_exists(self, ident: str) -> bool:
        return self.spark.catalog.tableExists(self.qualified(ident))

    def arrow_schema(self, ident: str) -> pa.Schema:
        """The table's live schema. Metadata only, no data scan."""
        return _spark_schema_to_arrow(self.spark.table(self.qualified(ident)).schema)

    def schema_history(self, ident: str) -> list[dict]:
        """One entry per schema version, oldest first, with Iceberg field IDs."""
        jtable = self._jtable(ident)
        schemas = jtable.schemas()
        history = []
        for schema_id in sorted(schemas.keySet()):
            jschema = schemas.get(schema_id)
            columns, field_ids = {}, {}
            for jfield in jschema.columns():
                columns[jfield.name()] = str(jfield.type().toString())
                field_ids[jfield.name()] = int(jfield.fieldId())
            history.append({"schema_id": int(schema_id), "columns": columns,
                            "field_ids": field_ids})
        return history

    # -- writes --------------------------------------------------------------

    def append(self, ident: str, data: pa.Table) -> None:
        target = self.arrow_schema(ident)
        self._to_spark(conform(data, target), target).writeTo(self.qualified(ident)).append()

    def overwrite(self, ident: str, data: pa.Table) -> None:
        from pyspark.sql.functions import lit

        target = self.arrow_schema(ident)
        # Row-level overwrite of every existing row: one snapshot whose
        # operation is genuinely "overwrite". Not ``overwritePartitions()``,
        # which would leave partitions absent from ``data`` in place.
        self._to_spark(conform(data, target), target).writeTo(
            self.qualified(ident)).overwrite(lit(True))

    # -- reads ---------------------------------------------------------------

    def scan_arrow(self, ident: str, snapshot_id: int | None = None,
                   columns: list[str] | None = None) -> pa.Table:
        projection = ", ".join(columns) if columns else "*"
        query = f"SELECT {projection} FROM {self.qualified(ident)}"
        if snapshot_id is not None:
            query += f" VERSION AS OF {snapshot_id}"
        return df_to_arrow(self.spark.sql(query))

    def sql(self, query: str, tables: dict[str, str]) -> pa.Table:
        for alias, ident in tables.items():
            self.spark.table(self.qualified(ident)).createOrReplaceTempView(alias)
        try:
            return df_to_arrow(self.spark.sql(query))
        finally:
            for alias in tables:
                self.spark.catalog.dropTempView(alias)

    # -- snapshot metadata ---------------------------------------------------

    def snapshots(self, ident: str) -> list[int]:
        return [d["snapshot_id"] for d in self.snapshot_details(ident)]

    def snapshot_row_counts(self, ident: str) -> list[int]:
        """Rows added per snapshot, oldest first. Includes overwrites, so
        filter on ``is_full_rebuild`` from ``snapshot_details`` when the
        distinction matters."""
        return [d["added_records"] for d in self.snapshot_details(ident)]

    def snapshot_details(self, ident: str) -> list[dict]:
        """Per-snapshot metadata from manifest summaries, oldest first."""
        jsnaps = sorted(self._jtable(ident).snapshots(),
                        key=lambda s: s.timestampMillis())
        raw = []
        for snap in jsnaps:
            summary = dict(snap.summary()) if snap.summary() is not None else {}
            parent = snap.parentId()
            raw.append({
                "snapshot_id": int(snap.snapshotId()),
                "parent_snapshot_id": int(parent) if parent is not None else None,
                "operation": str(snap.operation()),
                "added_records": _summary_int(summary, "added-records"),
                "deleted_records": _summary_int(summary, "deleted-records"),
                "total_records": _summary_int(summary, "total-records"),
            })
        by_id = {d["snapshot_id"]: d for d in raw}
        for detail in raw:
            detail["is_full_rebuild"] = is_full_rebuild(detail, by_id)
        return raw

    # -- internal ------------------------------------------------------------

    def _to_spark(self, data: pa.Table, target: pa.Schema):
        return self.spark.createDataFrame(data.to_pylist(),
                                          schema=_arrow_schema_to_spark(target))


# -- shared helpers (used by the Spark engine and by the tests' fake) --------

def is_full_rebuild(detail: dict, by_id: dict) -> bool:
    """True for a snapshot that replaced the table rather than adding to it.

    Spark commits an overwrite as one snapshot whose operation is
    "overwrite"; other writers commit a delete-that-clears-the-table followed
    by an append. Both shapes are recognised so a volume monitor never reads
    a rebuild's entire row count as "rows added today".
    """
    if detail["operation"] in ("overwrite", "replace"):
        return True
    if _clears_table(detail):
        return True
    if detail["operation"] == "append":
        return _clears_table(by_id.get(detail["parent_snapshot_id"]))
    return False


def _clears_table(detail: dict | None) -> bool:
    return bool(detail and detail["operation"] == "delete"
                and detail["deleted_records"] > 0 and detail["total_records"] == 0)


def _summary_int(summary, key: str) -> int:
    raw = summary.get(key) or summary.get(key.replace("-", "_")) or 0
    return int(raw)


def conform(data: pa.Table, target: pa.Schema) -> pa.Table:
    """Reorder ``data``'s columns by name to match ``target``, then cast.

    Column sets must match exactly: a stale or typo'd column in a writer must
    fail here rather than be silently dropped.
    """
    missing = [n for n in target.names if n not in data.column_names]
    extra = [n for n in data.column_names if n not in target.names]
    if missing or extra:
        raise KeyError(f"column set does not match target schema: "
                       f"missing={missing}, extra={extra}")
    return data.select(list(target.names)).cast(target)


def df_to_arrow(df) -> pa.Table:
    schema = _spark_schema_to_arrow(df.schema)
    rows = [r.asDict(recursive=True) for r in df.collect()]
    if not rows:
        return schema.empty_table()
    return pa.Table.from_pylist(rows, schema=schema)


def _spark_type_to_arrow(dt) -> pa.DataType:
    import pyspark.sql.types as T

    if isinstance(dt, T.StructType):
        return pa.struct([pa.field(f.name, _spark_type_to_arrow(f.dataType),
                                   nullable=f.nullable) for f in dt.fields])
    mapping = [
        (T.StringType, pa.string()), (T.LongType, pa.int64()),
        (T.IntegerType, pa.int32()), (T.DoubleType, pa.float64()),
        (T.FloatType, pa.float32()), (T.BooleanType, pa.bool_()),
        (T.DateType, pa.date32()), (T.TimestampType, pa.timestamp("us")),
    ]
    for spark_type, arrow_type in mapping:
        if isinstance(dt, spark_type):
            return arrow_type
    if isinstance(dt, T.DecimalType):
        # Spark widens `sum(...) * 1.0 / count(*)` to DECIMAL(38,16); DuckDB
        # returns DOUBLE for the same text. Kept exact here and turned into a
        # float by the consumer, so a monitor's metric is the same number on
        # both engines.
        return pa.decimal128(dt.precision, dt.scale)
    raise TypeError(f"unmapped Spark type for Arrow conversion: {dt}")


def _spark_schema_to_arrow(struct) -> pa.Schema:
    return pa.schema([pa.field(f.name, _spark_type_to_arrow(f.dataType),
                               nullable=f.nullable) for f in struct.fields])


def _arrow_type_to_spark(t: pa.DataType):
    import pyspark.sql.types as T

    if pa.types.is_struct(t):
        return T.StructType([T.StructField(f.name, _arrow_type_to_spark(f.type),
                                           nullable=f.nullable) for f in t])
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return T.StringType()
    if pa.types.is_int64(t):
        return T.LongType()
    if pa.types.is_int32(t):
        return T.IntegerType()
    if pa.types.is_float64(t):
        return T.DoubleType()
    if pa.types.is_float32(t):
        return T.FloatType()
    if pa.types.is_boolean(t):
        return T.BooleanType()
    if pa.types.is_date32(t):
        return T.DateType()
    if pa.types.is_timestamp(t):
        return T.TimestampType()
    if pa.types.is_decimal(t):
        return T.DecimalType(t.precision, t.scale)
    raise TypeError(f"unmapped Arrow type for Spark conversion: {t}")


def _arrow_schema_to_spark(schema: pa.Schema):
    import pyspark.sql.types as T

    return T.StructType([T.StructField(f.name, _arrow_type_to_spark(f.type),
                                       nullable=f.nullable) for f in schema])
