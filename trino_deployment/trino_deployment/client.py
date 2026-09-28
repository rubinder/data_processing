"""A thin wrapper over the Trino DB-API client."""
from __future__ import annotations

import os
from dataclasses import dataclass

import trino

HOST = os.environ.get("TRINO_HOST", "localhost")
PORT = int(os.environ.get("TRINO_PORT", "8085"))
USER = os.environ.get("TRINO_USER", "analyst")


@dataclass(frozen=True)
class Result:
    columns: list[str]
    rows: list[list]

    def dicts(self) -> list[dict]:
        return [dict(zip(self.columns, r)) for r in self.rows]


def connect(catalog: str | None = None, schema: str | None = None):
    return trino.dbapi.connect(host=HOST, port=PORT, user=USER, catalog=catalog, schema=schema)


def query(sql: str, conn=None) -> Result:
    own = conn is None
    conn = conn or connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []
        return Result(columns, [list(r) for r in rows])
    finally:
        if own:
            conn.close()


def explain(sql: str, conn=None) -> str:
    return "\n".join(r[0] for r in query(f"EXPLAIN {sql}", conn).rows)


def reachable() -> bool:
    try:
        query("SELECT 1")
        return True
    except Exception:  # noqa: BLE001 -- any failure means "no Trino here"
        return False
