import json

from mcp_deployment import catalog, config, templates
from mcp_deployment.embeddings import HashEmbedder, tokenize


def test_entries_come_from_the_dbt_manifest_and_catalog():
    entries = catalog.entries_from_dbt()
    tables = [e for e in entries if e.kind == "table"]
    assert sorted(t.name for t in tables) == ["funnel_analysis", "hourly_traffic",
                                              "page_type_summary", "user_engagement"]
    physical = json.loads((config.DBT_TARGET_DIR / "catalog.json").read_text())["nodes"]
    for t in tables:
        expected = physical[f"model.data_processing_analytics.{t.name}"]["columns"]
        assert {e.name for e in entries if e.kind == "column" and e.table_name == t.name} \
            == set(expected)
        assert "Columns:" in t.content and t.description
    assert not any(e.name.startswith(("int_", "stg_")) for e in tables)


def test_content_hash_is_stable_and_changes_with_content():
    a = catalog.Entry("table:x", "table", "x", "x", "d", "content one")
    b = catalog.Entry("table:x", "table", "x", "x", "d", "content one")
    c = catalog.Entry("table:x", "table", "x", "x", "d", "content two")
    assert a.content_hash == b.content_hash != c.content_hash


def test_template_entries_carry_parameters_and_returns():
    entries = catalog.entries_from_templates(templates.load_templates())
    funnel = next(e for e in entries if e.name == "funnel_by_page_type")
    assert funnel.kind == "template" and "page_type (int)" in funnel.content
    assert "Returns: page_type, event_type" in funnel.content


def test_tokenizer_splits_snake_case_identifiers():
    assert "funnel" in tokenize("avg_funnel_depth") and "avg_funnel_depth" in tokenize(
        "avg_funnel_depth")


def test_ranking_puts_the_relevant_template_first():
    embedder = HashEmbedder()
    entries = catalog.entries_from_dbt() + catalog.entries_from_templates(
        templates.load_templates())
    top = catalog.rank(entries, "funnel conversion rate from the previous stage", embedder,
                       kinds=("template",), limit=3)
    assert top[0][0].name == "funnel_by_page_type"
    users = catalog.rank(entries, "most engaged users ranked", embedder, kinds=("template",))
    assert users[0][0].name == "top_engaged_users"
    tables = catalog.rank(entries, "hourly traffic patterns by page type", embedder,
                          kinds=("table",))
    assert tables[0][0].name == "hourly_traffic"
    assert all(e.kind == "table" for e, _ in tables)
