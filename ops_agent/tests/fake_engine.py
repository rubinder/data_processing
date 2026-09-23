"""An in-memory ``LakehouseEngine`` for the rules tests.

Tables are Arrow tables; SQL runs through DuckDB, so every monitor query in
``feeds/*.yaml`` is executed for real by the fast suite -- against a second
engine than the one production uses, which is how a dialect-specific query
gets caught before it reaches Spark. Snapshot metadata and schema history
are recorded the way Iceberg would record them, and can be overridden to
stage a rename or a widening without a JVM.
"""
from __future__ import annotations

import duckdb
import pyarrow as pa

from ops_agent.engine import conform, is_full_rebuild
from ops_agent.tables import TableDef

_ICEBERG_TYPES = {
    "int32": "int", "int64": "long", "double": "double", "float": "float",
    "string": "string", "large_string": "string", "bool": "boolean",
    "date32[day]": "date", "timestamp[us]": "timestamptz",
}


class FakeEngine:
    def __init__(self):
        self.tables: dict[str, pa.Table] = {}
        self.details: dict[str, list[dict]] = {}
        self.histories: dict[str, list[dict]] = {}
        self.created: list[str] = []
        self._next_id = 1

    # -- lifecycle -----------------------------------------------------------

    def create_table(self, table_def: TableDef) -> None:
        self.created.append(table_def.name)
        if table_def.name not in self.tables:
            self.tables[table_def.name] = table_def.arrow_schema().empty_table()
            self.details[table_def.name] = []

    def table_exists(self, ident: str) -> bool:
        return ident in self.tables

    def drop_table(self, ident: str) -> None:
        self.tables.pop(ident, None)
        self.details.pop(ident, None)
        self.histories.pop(ident, None)

    def arrow_schema(self, ident: str) -> pa.Schema:
        return self.tables[ident].schema

    def schema_history(self, ident: str) -> list[dict]:
        if ident in self.histories:
            return self.histories[ident]
        schema = self.tables[ident].schema
        return [{"schema_id": 0,
                 "columns": {f.name: _ICEBERG_TYPES.get(str(f.type), str(f.type))
                             for f in schema},
                 "field_ids": {f.name: i + 1 for i, f in enumerate(schema)}}]

    def set_schema_history(self, ident: str, versions: list[dict]) -> None:
        self.histories[ident] = versions

    # -- writes --------------------------------------------------------------

    def _commit(self, ident: str, operation: str, added: int, deleted: int) -> None:
        total = self.tables[ident].num_rows
        parent = self.details[ident][-1]["snapshot_id"] if self.details[ident] else None
        detail = {"snapshot_id": self._next_id, "parent_snapshot_id": parent,
                  "operation": operation, "added_records": added,
                  "deleted_records": deleted, "total_records": total}
        self._next_id += 1
        by_id = {d["snapshot_id"]: d for d in self.details[ident]}
        by_id[detail["snapshot_id"]] = detail
        detail["is_full_rebuild"] = is_full_rebuild(detail, by_id)
        self.details[ident].append(detail)

    def append(self, ident: str, data: pa.Table) -> None:
        existing = self.tables[ident]
        self.tables[ident] = pa.concat_tables([existing, conform(data, existing.schema)])
        self._commit(ident, "append", data.num_rows, 0)

    def overwrite(self, ident: str, data: pa.Table) -> None:
        existing = self.tables[ident]
        deleted = existing.num_rows
        self.tables[ident] = conform(data, existing.schema)
        self._commit(ident, "overwrite", data.num_rows, deleted)

    def put(self, ident: str, data: pa.Table) -> None:
        """Test helper: create-or-replace a table straight from an Arrow table."""
        self.tables[ident] = data
        self.details.setdefault(ident, [])
        self._commit(ident, "append", data.num_rows, 0)

    # -- reads ---------------------------------------------------------------

    def scan_arrow(self, ident: str, snapshot_id=None, columns=None) -> pa.Table:
        table = self.tables[ident]
        return table.select(columns) if columns else table

    def sql(self, query: str, tables: dict[str, str]) -> pa.Table:
        con = duckdb.connect()
        try:
            for alias, ident in tables.items():
                con.register(alias, self.tables[ident])
            result = con.execute(query)
            # `.arrow()` returns a RecordBatchReader on duckdb >= 1.5.
            return (result.to_arrow_table() if hasattr(result, "to_arrow_table")
                    else result.fetch_arrow_table())
        finally:
            con.close()

    # -- snapshot metadata ---------------------------------------------------

    def snapshots(self, ident: str) -> list[int]:
        return [d["snapshot_id"] for d in self.details.get(ident, [])]

    def snapshot_row_counts(self, ident: str) -> list[int]:
        return [d["added_records"] for d in self.details.get(ident, [])]

    def snapshot_details(self, ident: str) -> list[dict]:
        return list(self.details.get(ident, []))
