from datetime import date

import pytest

from mcp_deployment import templates
from mcp_deployment.templates import Param, Template, TemplateError, bind, build_query


def test_the_shipped_templates_load_and_are_all_gold_only():
    loaded = templates.load_templates()
    assert set(loaded) == {"funnel_by_page_type", "page_type_summary", "top_engaged_users",
                           "hourly_traffic", "daily_conversion_trend"}
    for t in loaded.values():
        assert t.description and t.returns
        assert "gold." in t.sql


def _t(sql: str, params=(), **kw) -> Template:
    return Template("probe", "a probe template", sql, tuple(params), **kw)


@pytest.mark.parametrize("sql, params, needle", [
    ("SELECT 1; DROP TABLE gold.x", (), "no ';'"),
    ("DELETE FROM gold.funnel_analysis", (), "must start with SELECT or WITH"),
    ("SELECT * FROM gold.t WHERE a = %(a)s", (), "undeclared placeholder"),
    ("SELECT * FROM gold.t", (Param("a", "int"),), "declared but unused"),
    ("SELECT * FROM raw.impressions", (), "not a gold.<table>"),
    ("SELECT * FROM gold.t JOIN public_analytics.x USING (id)", (), "not a gold.<table>"),
    ("SELECT a % 2 FROM gold.t", (), "bare '%'"),
    ("SELECT * FROM gold.t WHERE a = %(a)s", (Param("a", "money"),), "unknown type"),
    ("SELECT * FROM gold.t WHERE a = %(a)s", (Param("a", "int", "x", required=False),),
     "what NULL means"),
])
def test_unsafe_or_malformed_templates_are_rejected_at_load(sql, params, needle):
    with pytest.raises(TemplateError, match=needle):
        templates.validate_template(_t(sql, params))


def test_ctes_defined_by_the_template_are_allowed_as_sources():
    templates.validate_template(_t(
        "WITH d AS (SELECT * FROM gold.t), r AS (SELECT * FROM d) SELECT * FROM r"))


def test_row_cap_and_timeout_bounds():
    with pytest.raises(TemplateError, match="max_rows"):
        templates.validate_template(_t("SELECT 1 FROM gold.t", max_rows=0))
    with pytest.raises(TemplateError, match="timeout_ms"):
        templates.validate_template(_t("SELECT 1 FROM gold.t", timeout_ms=999_999))


def test_bind_coerces_checks_and_defaults():
    t = _t("SELECT %(n)s, %(d)s, %(p)s, %(f)s FROM gold.t", (
        Param("n", "int", minimum=1, maximum=10, default=5, required=False),
        Param("d", "date"), Param("p", "int", choices=(1, 2, 3)),
        Param("f", "bool", default=False, required=False)))
    bound = bind(t, {"d": "2026-06-04", "p": "3", "f": "true"})
    assert bound == {"n": 5, "d": date(2026, 6, 4), "p": 3, "f": True}
    with pytest.raises(TemplateError, match="'d' is required"):
        bind(t, {"p": 1})
    with pytest.raises(TemplateError, match="must be one of"):
        bind(t, {"d": "2026-06-04", "p": 9})
    with pytest.raises(TemplateError, match="must be <= 10"):
        bind(t, {"d": "2026-06-04", "p": 1, "n": 11})
    with pytest.raises(TemplateError, match="expects int"):
        bind(t, {"d": "2026-06-04", "p": 1, "n": "many"})
    with pytest.raises(TemplateError, match="expects date"):
        bind(t, {"d": "yesterday", "p": 1})
    with pytest.raises(TemplateError, match="unknown parameter"):
        bind(t, {"d": "2026-06-04", "p": 1, "sql": "DROP"})


def test_bool_typed_ints_are_not_ints():
    t = _t("SELECT %(n)s FROM gold.t", (Param("n", "int"),))
    with pytest.raises(TemplateError):
        bind(t, {"n": True})


def test_build_query_wraps_the_template_in_its_row_cap():
    q = build_query(_t("SELECT a FROM gold.t"))
    assert q.startswith("SELECT * FROM (") and q.endswith("LIMIT %(__cap)s")


def test_duplicate_names_and_empty_directories_are_errors(tmp_path):
    body = ("name: dup\ndescription: d\nsql: SELECT 1 FROM gold.t\n")
    (tmp_path / "a.yaml").write_text(body)
    (tmp_path / "b.yaml").write_text(body)
    with pytest.raises(TemplateError, match="duplicate template name"):
        templates.load_templates(tmp_path)
    with pytest.raises(TemplateError, match="no templates found"):
        templates.load_templates(tmp_path / "nothing")
