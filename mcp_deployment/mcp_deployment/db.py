"""Thin psycopg helpers. Every SQL statement in this module is repo-owned."""
from __future__ import annotations

from pathlib import Path

import psycopg

from mcp_deployment import config


def connect(database: config.Database, **kwargs) -> psycopg.Connection:
    return psycopg.connect(database.dsn, **kwargs)


def apply_sql_files(conn: psycopg.Connection, directory: Path = config.SQL_DIR) -> list[str]:
    """Apply every ``sql/*.sql`` in name order. Each file is idempotent."""
    applied = []
    for path in sorted(Path(directory).glob("*.sql")):
        with conn.transaction():
            conn.execute(path.read_text())
        applied.append(path.name)
    return applied


def rotate_passwords(conn: psycopg.Connection, passwords: dict[str, str]) -> list[str]:
    """``ALTER ROLE ... PASSWORD`` for each role given. Role names are checked
    against the fixed stage list; passwords are bound, never interpolated."""
    rotated = []
    for role, password in passwords.items():
        if role not in config.STAGE_ROLES:
            raise ValueError(f"unknown stage role: {role!r}")
        with conn.transaction():
            conn.execute(psycopg.sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                psycopg.sql.Identifier(role), psycopg.sql.Literal(password)))
        rotated.append(role)
    return rotated


def rows(conn: psycopg.Connection, query: str, params=None) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description is None:
            return []
        names = [d.name for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
