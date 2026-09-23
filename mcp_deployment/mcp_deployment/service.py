"""What the MCP tools do, without the MCP plumbing.

``GoldService`` owns the loaded templates and two injectable callables: one
that searches the catalog and one that executes a template. The server wires
them to PostgreSQL as the reader role; the tests wire them to fakes. There is
deliberately no method that accepts SQL.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from mcp_deployment import catalog, config, db, templates
from mcp_deployment.embeddings import Embedder, get_embedder
from mcp_deployment.templates import Template, TemplateError

KINDS = ("table", "column", "template")


@dataclass
class GoldService:
    templates: dict[str, Template]
    search_fn: Callable[[str, tuple[str, ...] | None, int], list[dict]]
    execute_fn: Callable[[Template, dict | None], templates.QueryResult]
    columns_fn: Callable[[str], list[dict]] = lambda table: []
    release_fn: Callable[[], dict | None] = lambda: None
    embedder_name: str = "hash"
    extra: dict = field(default_factory=dict)

    # -- catalog ---------------------------------------------------------------

    def search_catalog(self, query: str, kinds: list[str] | None = None,
                       limit: int = 10) -> dict:
        if not query or not query.strip():
            raise TemplateError("search needs a non-empty query")
        wanted = tuple(kinds or ())
        bad = [k for k in wanted if k not in KINDS]
        if bad:
            raise TemplateError(f"unknown kind(s) {bad}; expected any of {list(KINDS)}")
        limit = max(1, min(int(limit), 50))
        hits = self.search_fn(query.strip(), wanted or None, limit)
        return {"query": query.strip(), "kinds": list(wanted) or list(KINDS),
                "results": hits, "count": len(hits),
                "next_step": ("call gold_describe_template on a template hit, or "
                              "gold_describe_table on a table hit")}

    def list_templates(self) -> dict:
        return {"templates": [self._summary(t) for t in sorted(self.templates.values(),
                                                                key=lambda x: x.name)],
                "count": len(self.templates),
                "note": "these are the only queries this server can run"}

    def describe_template(self, name: str) -> dict:
        t = self._template(name)
        return {**self._summary(t), "sql": t.sql,
                "params": [{"name": p.name, "type": p.type, "required": p.required,
                            "default": p.default, "choices": list(p.choices) if p.choices else None,
                            "min": p.minimum, "max": p.maximum, "description": p.description}
                           for p in t.params],
                "returns": list(t.returns), "max_rows": t.max_rows, "timeout_ms": t.timeout_ms}

    def describe_table(self, name: str) -> dict:
        columns = self.columns_fn(name)
        if not columns:
            raise TemplateError(f"no gold table named '{name}' in the catalog; "
                                "gold_search_catalog with kinds=['table'] lists what exists")
        applicable = [t.name for t in self.templates.values()
                      if f"{config.GOLD_SCHEMA}.{name}" in t.sql]
        return {"table": name, "schema": config.GOLD_SCHEMA, "columns": columns,
                "templates_reading_it": sorted(applicable), "release": self.release_fn()}

    # -- execution -------------------------------------------------------------

    def run_template(self, name: str, params: dict | None = None) -> dict:
        t = self._template(name)
        result = self.execute_fn(t, params)
        out = {"template": result.template, "params": result.params,
               "columns": result.columns, "rows": result.rows, "row_count": result.row_count,
               "truncated": result.truncated, "elapsed_ms": result.elapsed_ms}
        if result.truncated:
            out["note"] = (f"only the first {t.max_rows} rows are returned; narrow the "
                           "parameters for the rest")
        return out

    # -- internal --------------------------------------------------------------

    def _template(self, name: str) -> Template:
        t = self.templates.get(name)
        if t is None:
            raise TemplateError(f"no template named '{name}'; gold_list_templates shows the "
                                f"{len(self.templates)} that exist")
        return t

    @staticmethod
    def _summary(t: Template) -> dict:
        return {"name": t.name, "description": t.description, "tags": list(t.tags),
                "params": [f"{p.name}: {p.type}{'' if p.required else ' (optional)'}"
                           for p in t.params]}


def postgres_service(template_dir=None, embedder: Embedder | None = None) -> GoldService:
    """A service bound to the reader role. Every call opens its own short
    connection; the server is stateless between calls."""
    loaded = templates.load_templates(template_dir)
    embedder = embedder or get_embedder()
    reader = config.role_db("mcp_reader")

    def search_fn(query, kinds, limit):
        with db.connect(reader) as conn:
            return catalog.search(conn, query, embedder, kinds, limit)

    def execute_fn(template, params):
        with db.connect(reader) as conn:
            return templates.execute(conn, template, params)

    def columns_fn(table):
        with db.connect(reader) as conn:
            return catalog.columns_of(conn, table)

    def release_fn():
        from mcp_deployment import promotion
        with db.connect(reader) as conn:
            found = promotion.active_release(conn)
        if found:
            found["promoted_at"] = found["promoted_at"].isoformat()
        return found

    return GoldService(loaded, search_fn, execute_fn, columns_fn, release_fn, embedder.model)
