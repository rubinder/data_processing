"""Promote, roll back, inspect. Thin wrappers over the SECURITY DEFINER
functions in sql/002_gold_promotion.sql, run as the promotion role."""
from __future__ import annotations

import sys

from mcp_deployment import config, db


def promote(conn, source_schema: str = config.SOURCE_SCHEMA, note: str | None = None) -> int:
    with conn.transaction():
        rid = conn.execute("SELECT gold_ops.promote(%s, %s)", (source_schema, note)).fetchone()[0]
    conn.commit()
    return rid


def rollback(conn) -> int:
    with conn.transaction():
        rid = conn.execute("SELECT gold_ops.rollback()").fetchone()[0]
    conn.commit()
    return rid


def releases(conn, limit: int = 20) -> list[dict]:
    return db.rows(conn,
                   "SELECT release_id, promoted_at, promoted_by, source_schema, tables, "
                   "row_counts, note, status, status_changed, schema_retained "
                   "FROM gold_ops.releases ORDER BY release_id DESC LIMIT %s", (limit,))


def active_release(conn) -> dict | None:
    found = db.rows(conn, "SELECT release_id, promoted_at, promoted_by, source_schema, tables, "
                          "row_counts, note FROM gold_ops.releases WHERE status = 'active'")
    return found[0] if found else None


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else "releases"
    with db.connect(config.role_db("promotion")) as conn:
        if command == "promote":
            note = " ".join(sys.argv[2:]) or None
            rid = promote(conn, note=note)
            print(f"promoted {config.SOURCE_SCHEMA} -> gold as release {rid}")
        elif command == "rollback":
            rid = rollback(conn)
            print(f"rolled back; release {rid} is active again")
        elif command == "releases":
            for r in releases(conn):
                print(f"  #{r['release_id']:<3} {r['status']:<11} {r['promoted_at']:%Y-%m-%d %H:%M:%S} "
                      f"{r['source_schema']} tables={len(r['tables'])} "
                      f"retained={r['schema_retained']} {r['note'] or ''}")
        else:
            print("usage: python -m mcp_deployment.promotion [promote [note] | rollback | releases]")
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
