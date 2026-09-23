import asyncio
import json

import pytest

from mcp_deployment import server, templates
from mcp_deployment.service import GoldService
from mcp_deployment.templates import QueryResult, TemplateError


def _service(hits=None):
    loaded = templates.load_templates()
    calls = {}

    def search_fn(query, kinds, limit):
        calls["search"] = (query, kinds, limit)
        return hits or [{"entry_id": "template:funnel_by_page_type", "kind": "template",
                         "name": "funnel_by_page_type", "table_name": None,
                         "description": "d", "score": 0.9}]

    def execute_fn(template, params):
        bound = templates.bind(template, params)
        calls["execute"] = (template.name, bound)
        rows = [[3, "a", 500, 500, 100.0, None]] * 3
        return QueryResult(template.name, ["page_type", "event_type", "impressions_at_stage",
                                           "total_impressions", "pct_of_total",
                                           "pct_from_previous_stage"],
                           rows, len(rows), False, 1.5, bound)

    def columns_fn(table):
        return [{"name": "page_type", "description": ""}] if table == "funnel_analysis" else []

    return GoldService(loaded, search_fn, execute_fn, columns_fn, lambda: {"release_id": 1}), calls


def test_search_validates_and_passes_through():
    svc, calls = _service()
    out = svc.search_catalog("  conversion by page  ", ["template"], 500)
    assert calls["search"] == ("conversion by page", ("template",), 50)
    assert out["count"] == 1 and out["results"][0]["name"] == "funnel_by_page_type"
    with pytest.raises(TemplateError, match="non-empty"):
        svc.search_catalog("   ")
    with pytest.raises(TemplateError, match="unknown kind"):
        svc.search_catalog("x", ["view"])


def test_run_template_binds_before_executing_and_names_unknown_templates():
    svc, calls = _service()
    out = svc.run_template("funnel_by_page_type", {"page_type": "3"})
    assert calls["execute"] == ("funnel_by_page_type", {"page_type": 3})
    assert out["row_count"] == 3 and out["columns"][0] == "page_type"
    with pytest.raises(TemplateError, match="must be one of"):
        svc.run_template("funnel_by_page_type", {"page_type": 7})
    with pytest.raises(TemplateError, match="no template named 'select_star'"):
        svc.run_template("select_star", {})


def test_describe_table_and_template():
    svc, _ = _service()
    table = svc.describe_table("funnel_analysis")
    assert table["templates_reading_it"] == ["funnel_by_page_type"]
    assert table["release"] == {"release_id": 1}
    with pytest.raises(TemplateError, match="no gold table named"):
        svc.describe_table("secrets")
    desc = svc.describe_template("top_engaged_users")
    assert {p["name"] for p in desc["params"]} == {"limit", "min_impressions"}
    assert desc["max_rows"] == 500 and "gold.user_engagement" in desc["sql"]


def test_mcp_server_exposes_exactly_the_governed_tools():
    svc, _ = _service()
    mcp = server.build_server(svc)
    tools = asyncio.run(mcp.list_tools())
    names = sorted(t.name for t in tools)
    assert names == ["gold_describe_table", "gold_describe_template", "gold_list_templates",
                     "gold_run_template", "gold_search_catalog"]
    for tool in tools:
        assert tool.annotations.readOnlyHint is True
        assert "sql" not in tool.inputSchema.get("properties", {})


def test_mcp_tool_calls_return_json_and_errors_are_messages_not_exceptions():
    svc, _ = _service()
    mcp = server.build_server(svc)
    ok = asyncio.run(mcp.call_tool("gold_run_template",
                                   {"name": "funnel_by_page_type", "params": {"page_type": 3}}))
    payload = json.loads(_text(ok))
    assert payload["row_count"] == 3 and payload["params"] == {"page_type": 3}
    bad = asyncio.run(mcp.call_tool("gold_run_template",
                                    {"name": "funnel_by_page_type", "params": {"page_type": 9}}))
    assert "must be one of" in json.loads(_text(bad))["error"]
    listing = json.loads(_text(asyncio.run(mcp.call_tool("gold_list_templates", {}))))
    assert listing["count"] == 5


def _text(result) -> str:
    content = result[0] if isinstance(result, tuple) else result
    return content[0].text
