"""Connection settings and paths. Development defaults, environment overrides."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = Path(os.environ.get("GOLD_TEMPLATES", MODULE_ROOT / "templates"))
SQL_DIR = MODULE_ROOT / "sql"
DBT_TARGET_DIR = Path(os.environ.get(
    "DBT_TARGET_DIR", MODULE_ROOT.parent / "dbt_deployment" / "dbt_project" / "target"))

GOLD_SCHEMA = "gold"
#: Where dbt materializes the analysis models (profile schema `public` plus
#: the model config `+schema: analytics`).
SOURCE_SCHEMA = os.environ.get("GOLD_SOURCE_SCHEMA", "public_analytics")
EMBEDDING_DIMENSION = 384

STAGE_ROLES = ("ingestion", "transform", "promotion", "mcp_reader")


@dataclass(frozen=True)
class Database:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (f"host={self.host} port={self.port} dbname={self.dbname} "
                f"user={self.user} password={self.password}")


def _base() -> tuple[str, int, str]:
    # The dbt deployment publishes PostgreSQL on 5433; that is the default.
    return (os.environ.get("GOLD_DB_HOST", "localhost"),
            int(os.environ.get("GOLD_DB_PORT", "5433")),
            os.environ.get("GOLD_DB_NAME", "data_processing"))


def admin_db() -> Database:
    """The superuser that applies sql/ and owns the promotion functions."""
    host, port, name = _base()
    return Database(host, port, name, os.environ.get("GOLD_DB_ADMIN_USER", "dbt"),
                    os.environ.get("GOLD_DB_ADMIN_PASSWORD", "dbt"))


def role_db(role: str) -> Database:
    """A stage role's connection. Password from ``<ROLE>_PASSWORD``, else the
    development default (the role name)."""
    if role not in STAGE_ROLES:
        raise ValueError(f"unknown stage role: {role!r} (expected one of {STAGE_ROLES})")
    host, port, name = _base()
    return Database(host, port, name, role, os.environ.get(f"{role.upper()}_PASSWORD", role))
