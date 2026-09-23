"""Every Iceberg table the agent knows about, by name.

Feed configs name tables; this registry is what makes a typo in a YAML file a
loud failure at load time rather than a "table does not exist" swallowed into
a breach at run time. Columns are declared in Iceberg's own type vocabulary
(``int``, ``long``, ``string``, ``timestamptz`` ...) because that is what the
engine's ``schema_history`` reports and what contracts are written in.
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from iceberg_deployment.impressions import IMPRESSIONS_DDL

from ops_agent import config

_SPARK_TYPES = {
    "int": "INT", "long": "BIGINT", "float": "FLOAT", "double": "DOUBLE",
    "string": "STRING", "boolean": "BOOLEAN", "date": "DATE",
    "timestamptz": "TIMESTAMP",
}
_ARROW_TYPES = {
    "int": pa.int32(), "long": pa.int64(), "float": pa.float32(),
    "double": pa.float64(), "string": pa.string(), "boolean": pa.bool_(),
    "date": pa.date32(), "timestamptz": pa.timestamp("us"),
}


@dataclass(frozen=True)
class TableDef:
    name: str
    columns: tuple[tuple[str, str], ...]
    layer: str                  # raw | derived | ops
    partitioned_by: str = ""
    # An explicit DDL template (with a ``{table}`` placeholder) for tables
    # whose properties matter; generated from ``columns`` otherwise.
    ddl: str | None = None

    def create_ddl(self, qualified_name: str) -> str:
        if self.ddl:
            return self.ddl.format(table=qualified_name)
        cols = ", ".join(f"{name} {_SPARK_TYPES[kind]}" for name, kind in self.columns)
        part = f" PARTITIONED BY ({self.partitioned_by})" if self.partitioned_by else ""
        return f"CREATE TABLE IF NOT EXISTS {qualified_name} ({cols}) USING iceberg{part}"

    def arrow_schema(self) -> pa.Schema:
        return pa.schema([pa.field(name, _ARROW_TYPES[kind]) for name, kind in self.columns])


# The event table iceberg_deployment owns. Its DDL is imported, not copied,
# so hidden partitioning and the merge-on-read properties stay in one place.
RAW_IMPRESSIONS = TableDef(
    config.RAW_TABLE,
    (("user_id", "string"), ("impression_id", "string"), ("page_type", "int"),
     ("event_ts", "timestamptz"), ("event_type", "string")),
    layer="raw", ddl=IMPRESSIONS_DDL,
)

# One row per impression, rebuilt from RAW_IMPRESSIONS on every run. Exists so
# the module has both shapes of table: an append-only one where volume must be
# judged per snapshot, and a fully rebuilt one where ``count(*)`` is right.
AGGREGATED_IMPRESSIONS = TableDef(
    config.AGGREGATED_TABLE,
    (("impression_id", "string"), ("user_id", "string"), ("page_type", "int"),
     ("event_date", "date"), ("funnel_depth", "int"),
     ("max_event_type", "string"), ("first_event_ts", "timestamptz"),
     ("last_event_ts", "timestamptz")),
    layer="derived", partitioned_by="event_date, page_type",
)

OPS_MONITOR_RESULTS = TableDef(
    "ops.monitor_results",
    (("run_at", "date"), ("monitor", "string"), ("table_name", "string"),
     ("column_name", "string"), ("kind", "string"), ("metric", "double"),
     ("baseline_median", "double"), ("status", "string"), ("detail", "string")),
    layer="ops", partitioned_by="months(run_at)",
)

OPS_ALERT_LOG = TableDef(
    "ops.alert_log",
    (("run_at", "date"), ("alert_key", "string"), ("severity", "string"),
     ("title", "string"), ("body", "string"), ("source", "string"),
     ("delivered", "boolean")),
    layer="ops", partitioned_by="months(run_at)",
)

# Every finding the agent has ever raised, one row per finding per run.
# Append-only and keyed on ``finding_key``, which is what makes "first seen",
# "still open" and "cleared" answerable. The incident files cannot answer
# them: their slugs are deliberately stable so a recurring finding rewrites
# one file, which means yesterday's state is gone.
OPS_FINDING_LOG = TableDef(
    "ops.finding_log",
    (("run_at", "date"), ("finding_key", "string"), ("table_name", "string"),
     ("kind", "string"), ("severity", "string"), ("column_name", "string"),
     ("detail", "string"), ("reasoning", "string"), ("evidence", "string"),
     ("actioned", "boolean")),
    layer="ops", partitioned_by="months(run_at)",
)

# One row per agent run, findings or not. Without it "the agent ran and found
# nothing" and "the agent never ran" are the same absence of rows.
OPS_AGENT_RUNS = TableDef(
    "ops.agent_runs",
    (("run_at", "date"), ("findings_count", "long"), ("actioned_count", "long"),
     ("dry_run", "boolean")),
    layer="ops", partitioned_by="months(run_at)",
)

ALL_TABLES: tuple[TableDef, ...] = (
    RAW_IMPRESSIONS, AGGREGATED_IMPRESSIONS,
    OPS_MONITOR_RESULTS, OPS_ALERT_LOG, OPS_FINDING_LOG, OPS_AGENT_RUNS,
)


def by_name(name: str) -> TableDef:
    for table in ALL_TABLES:
        if table.name == name:
            return table
    raise KeyError(name)
