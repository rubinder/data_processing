"""The layer end to end, printed: apply the SQL, promote, sync the catalog,
search it, run a template as the reader, prove the reader cannot see
anything else, roll back, promote again."""
from __future__ import annotations

import json
import sys

import psycopg

from mcp_deployment import catalog, config, db, promotion, templates
from mcp_deployment.embeddings import get_embedder
from mcp_deployment.service import postgres_service


def apply() -> None:
    with db.connect(config.admin_db()) as conn:
        applied = db.apply_sql_files(conn)
        print(f"applied {', '.join(applied)} as {config.admin_db().user}")
        overrides = {role: pw for role in config.STAGE_ROLES
                     if (pw := __import__("os").environ.get(f"{role.upper()}_PASSWORD"))}
        if overrides:
            print(f"rotated passwords for {', '.join(db.rotate_passwords(conn, overrides))}")


def sync() -> catalog.SyncResult:
    embedder = get_embedder()
    entries = catalog.entries_from_dbt() + catalog.entries_from_templates(
        templates.load_templates())
    with db.connect(config.role_db("transform")) as conn:
        result = catalog.sync(conn, entries, embedder)
    print(f"catalog: {len(entries)} entries ({result.inserted} new, {result.updated} changed, "
          f"{result.deleted} removed, {result.unchanged} unchanged) with {result.embedder}")
    return result


def ask(question: str) -> None:
    service = postgres_service()
    for hit in service.search_catalog(question, None, 5)["results"]:
        print(f"  {hit['score']:.3f}  {hit['kind']:<8} {hit['name']}"
              f"{'  (' + hit['table_name'] + ')' if hit['kind'] == 'column' else ''}")


def _cannot(role: str, statement: str) -> str:
    try:
        with db.connect(config.role_db(role)) as conn:
            conn.execute(statement)
        return f"  {role}: ALLOWED  {statement}   <-- unexpected"
    except psycopg.errors.InsufficientPrivilege as exc:
        return f"  {role}: denied   {statement}  ({exc.diag.message_primary})"
    except psycopg.errors.UndefinedTable as exc:
        return f"  {role}: denied   {statement}  ({exc.diag.message_primary})"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["apply"]:
        apply()
        return 0
    if argv[:1] == ["sync"]:
        sync()
        return 0
    if argv[:1] == ["ask"]:
        ask(" ".join(argv[1:]))
        return 0

    print("=== 1. apply roles, gold, promotion functions, catalog")
    apply()

    print("\n=== 2. promote public_analytics -> gold as the promotion role")
    with db.connect(config.role_db("promotion")) as conn:
        rid = promotion.promote(conn, note="demo release")
        active = promotion.active_release(conn)
    print(f"  release #{rid}: {', '.join(active['tables'])}")
    print(f"  row counts: {json.dumps(active['row_counts'])}")

    print("\n=== 3. sync the catalog from the dbt manifest and templates")
    sync()

    print("\n=== 4. what the agent sees: search, describe, run")
    service = postgres_service()
    for question in ("conversion rate by page type", "who are the most engaged users",
                     "traffic by hour last week"):
        print(f"  ? {question}")
        for hit in service.search_catalog(question, None, 3)["results"]:
            print(f"      {hit['score']:.3f}  {hit['kind']:<8} {hit['name']}")
    result = service.run_template("funnel_by_page_type", {"page_type": 3})
    print(f"  run funnel_by_page_type(page_type=3): {result['row_count']} rows in "
          f"{result['elapsed_ms']} ms, columns {result['columns']}")
    for row in result["rows"]:
        print(f"      {row}")
    for bad in ({"page_type": 9}, {"page_type": "three"}, {"page_typo": 3}):
        try:
            service.run_template("funnel_by_page_type", bad)
        except templates.TemplateError as exc:
            print(f"  rejected {bad}: {exc}")

    print("\n=== 5. what the roles cannot do")
    print(_cannot("mcp_reader", "SELECT count(*) FROM raw.impressions"))
    print(_cannot("mcp_reader", f"SELECT count(*) FROM {config.SOURCE_SCHEMA}.funnel_analysis"))
    print(_cannot("mcp_reader", "SELECT gold_ops.promote('public_analytics', 'x')"))
    print(_cannot("promotion", "SELECT count(*) FROM gold.funnel_analysis"))
    print(_cannot("ingestion", "SELECT count(*) FROM gold.funnel_analysis"))

    print("\n=== 6. promote again, then roll back")
    with db.connect(config.role_db("promotion")) as conn:
        rid2 = promotion.promote(conn, note="second release")
        print(f"  release #{rid2} active")
        back = promotion.rollback(conn)
        print(f"  rolled back: release #{back} active again")
        for r in promotion.releases(conn):
            print(f"    #{r['release_id']} {r['status']:<11} retained={r['schema_retained']} "
                  f"{r['note']}")
    with db.connect(config.role_db("mcp_reader")) as conn:
        n = conn.execute("SELECT count(*) FROM gold.funnel_analysis").fetchone()[0]
    print(f"  reader still sees gold.funnel_analysis: {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
