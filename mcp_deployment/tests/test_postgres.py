"""Against the real database: the roles, the swap, the rollback, the vector
catalog and a governed execution. Skipped when PostgreSQL is not reachable."""
import psycopg
import pytest

from mcp_deployment import catalog, config, promotion, templates
from mcp_deployment.embeddings import HashEmbedder
from mcp_deployment.service import postgres_service
from mcp_deployment.templates import Param, Template, TemplateError

pytestmark = pytest.mark.postgres


def _count(conn, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def _denied(conn, statement: str) -> bool:
    try:
        with conn.transaction():
            conn.execute(statement)
        return False
    except (psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedTable):
        return True


def test_promotion_copies_only_gold_tables_and_grants_the_reader(admin, candidate, role_conn):
    rid = promotion.promote(role_conn("promotion"), candidate, "test")
    active = promotion.active_release(admin)
    assert active["release_id"] == rid
    assert active["tables"] == ["funnel_analysis", "hourly_traffic", "page_type_summary",
                                "user_engagement"]
    assert active["row_counts"]["funnel_analysis"] == 4
    reader = role_conn("mcp_reader")
    assert _count(reader, "gold.funnel_analysis") == 4
    assert _denied(reader, "SELECT * FROM gold.int_impressions_aggregated")


def test_stage_roles_are_least_privilege(admin, candidate, role_conn):
    promotion.promote(role_conn("promotion"), candidate, "rbac")
    reader = role_conn("mcp_reader")
    assert _denied(reader, "SELECT count(*) FROM raw.impressions")
    assert _denied(reader, f"SELECT count(*) FROM {candidate}.funnel_analysis")
    assert _denied(reader, "SELECT gold_ops.promote('public_analytics', 'x')")
    assert _denied(reader, "CREATE TABLE gold.scratch (x int)")
    assert _denied(reader, "INSERT INTO catalog.entries (entry_id) VALUES ('x')")
    promo = role_conn("promotion")
    assert _denied(promo, "SELECT count(*) FROM gold.funnel_analysis")
    assert _denied(promo, f"SELECT count(*) FROM {candidate}.funnel_analysis")
    with pytest.raises(psycopg.errors.RaiseException, match="not a promotable candidate"):
        promotion.promote(promo, "gold_ops")
    with pytest.raises(psycopg.errors.RaiseException, match="does not exist"):
        promotion.promote(promo, "nope_schema")
    assert _denied(role_conn("ingestion"), "SELECT count(*) FROM gold.funnel_analysis")


def test_rollback_restores_the_previous_release_and_the_log_says_so(admin, candidate, role_conn):
    promo = role_conn("promotion")
    first = promotion.promote(promo, candidate, "first")
    admin.execute(f"DELETE FROM {candidate}.funnel_analysis WHERE page_type = 3")
    second = promotion.promote(promo, candidate, "second")
    reader = role_conn("mcp_reader")
    assert _count(reader, "gold.funnel_analysis") == 2
    back = promotion.rollback(promo)
    assert back == first
    assert _count(reader, "gold.funnel_analysis") == 4
    status = {r["release_id"]: r["status"] for r in promotion.releases(admin)}
    assert status[first] == "active" and status[second] == "rolled_back"
    # Rolling back again needs another retained superseded release.
    with pytest.raises(psycopg.errors.RaiseException, match="no retained previous"):
        for _ in range(10):
            promotion.rollback(promo)


def test_retention_keeps_three_inactive_release_schemas(admin, candidate, role_conn):
    promo = role_conn("promotion")
    ids = [promotion.promote(promo, candidate, f"r{i}") for i in range(6)]
    retained = {r["release_id"]: r["schema_retained"] for r in promotion.releases(admin, 50)}
    assert retained[ids[-1]] is True                      # active
    assert all(retained[i] for i in ids[-4:-1])           # three newest inactive
    assert not any(retained[i] for i in ids[:2])          # older ones dropped
    schemas = {r[0] for r in admin.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name LIKE 'gold_release_%'").fetchall()}
    assert not any(f"gold_release_{i}" in schemas for i in ids[:2])


def test_catalog_syncs_incrementally_and_searches_through_pgvector(admin, role_conn):
    embedder = HashEmbedder()
    entries = catalog.entries_from_dbt() + catalog.entries_from_templates(
        templates.load_templates())
    transform = role_conn("transform")
    admin.execute("DELETE FROM catalog.entries")
    first = catalog.sync(transform, entries, embedder)
    assert first.inserted == len(entries) and first.updated == first.deleted == 0
    again = catalog.sync(transform, entries, embedder)
    assert again.unchanged == len(entries) and again.inserted == 0
    changed = [catalog.Entry(e.entry_id, e.kind, e.name, e.table_name, e.description,
                             e.content + " (edited)", e.ordinal) if e.name == "funnel_by_page_type"
               else e for e in entries if e.kind != "column"]
    third = catalog.sync(transform, changed, embedder)
    assert third.updated == 1 and third.deleted == sum(1 for e in entries if e.kind == "column")

    catalog.sync(transform, entries, embedder)
    reader = role_conn("mcp_reader")
    hits = catalog.search(reader, "funnel conversion rate from the previous stage", embedder,
                          ("template",), 3)
    assert hits[0]["name"] == "funnel_by_page_type" and 0 < hits[0]["score"] <= 1
    in_memory = catalog.rank(entries, "funnel conversion rate from the previous stage",
                             embedder, ("template",), 3)
    assert [h["name"] for h in hits] == [e.name for e, _ in in_memory]
    assert catalog.columns_of(reader, "funnel_analysis")[0]["name"] == "page_type"


def test_governed_execution_binds_caps_and_times_out(admin, candidate, role_conn):
    promotion.promote(role_conn("promotion"), candidate, "exec")
    reader = role_conn("mcp_reader")
    loaded = templates.load_templates()
    result = templates.execute(reader, loaded["funnel_by_page_type"], {"page_type": 3})
    assert result.columns[:2] == ["page_type", "event_type"] and result.row_count == 2
    assert result.params == {"page_type": 3}

    capped = Template("cap", "cap probe", "SELECT * FROM gold.hourly_traffic", max_rows=5)
    out = templates.execute(reader, capped, {})
    assert out.row_count == 5 and out.truncated

    slow = Template("slow", "slow probe", "SELECT pg_sleep(1) FROM gold.funnel_analysis",
                    timeout_ms=100)
    with pytest.raises(TemplateError, match="timeout"):
        templates.execute(reader, slow, {})

    # Not reachable through the loader (the lint rejects it), but if a
    # hand-built template did read outside gold the role stops it.
    leak = Template("leak", "leak probe", f"SELECT * FROM {candidate}.funnel_analysis")
    with pytest.raises(TemplateError, match="cannot see"):
        templates.execute(reader, leak, {})


def test_the_service_end_to_end_as_the_reader(admin, candidate, role_conn):
    promotion.promote(role_conn("promotion"), candidate, "service")
    entries = catalog.entries_from_dbt() + catalog.entries_from_templates(
        templates.load_templates())
    catalog.sync(role_conn("transform"), entries, HashEmbedder())
    svc = postgres_service(embedder=HashEmbedder())
    hit = svc.search_catalog("most engaged users", ["template"], 1)["results"][0]
    assert hit["name"] == "top_engaged_users"
    out = svc.run_template(hit["name"], {"limit": 3})
    assert out["row_count"] == 3 and out["rows"][0][0] == "user_1"
    table = svc.describe_table("user_engagement")
    assert table["release"]["source_schema"] == candidate


def test_every_shipped_template_runs_with_only_its_required_parameters(admin, candidate, role_conn):
    """An optional parameter arrives as NULL; PostgreSQL must still be able to
    type it (the ::int casts in the templates exist because it could not)."""
    promotion.promote(role_conn("promotion"), candidate, "all templates")
    reader = role_conn("mcp_reader")
    sample = {"int": 1, "date": "2026-06-03", "str": "x", "float": 1.0, "bool": True}
    for t in templates.load_templates().values():
        params = {p.name: (p.choices[0] if p.choices else sample[p.type])
                  for p in t.params if p.required}
        result = templates.execute(reader, t, params)
        assert list(result.columns) == list(t.returns), t.name
        assert result.row_count >= 1, t.name
