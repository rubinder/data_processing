"""The MCP server: five read-only tools over the gold layer, no SQL tool.

Run with ``python -m mcp_deployment.server`` (stdio) and point an MCP client
at it. The process connects as ``mcp_reader``, which can SELECT gold and the
catalog and nothing else; the tools can call templates and nothing else.
"""
from __future__ import annotations

import json
import sys
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from mcp_deployment.service import GoldService, postgres_service
from mcp_deployment.templates import TemplateError

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=False)


def build_server(service: GoldService | None = None) -> FastMCP:
    mcp = FastMCP("gold_mcp")
    state = {"service": service}

    def svc() -> GoldService:
        if state["service"] is None:
            state["service"] = postgres_service()
        return state["service"]

    def guarded(fn):
        try:
            return json.dumps(fn(), default=str)
        except TemplateError as exc:
            return json.dumps({"error": str(exc)})

    @mcp.tool(name="gold_search_catalog", annotations=READ_ONLY)
    def gold_search_catalog(
        query: Annotated[str, Field(description="What you are looking for, in plain words, "
                                                "e.g. 'conversion rate by page type'")],
        kinds: Annotated[list[str] | None, Field(description="Restrict to 'table', 'column' "
                                                             "and/or 'template'")] = None,
        limit: Annotated[int, Field(description="Max results, 1-50", ge=1, le=50)] = 10,
    ) -> str:
        """Semantic search over the gold catalog: tables, columns and the query
        templates this server can run. Start here. Returns JSON hits with a
        similarity score; follow a template hit with gold_describe_template."""
        return guarded(lambda: svc().search_catalog(query, kinds, limit))

    @mcp.tool(name="gold_list_templates", annotations=READ_ONLY)
    def gold_list_templates() -> str:
        """List every pre-approved query template with its parameters. These are
        the only queries this server can execute; there is no raw-SQL tool."""
        return guarded(lambda: svc().list_templates())

    @mcp.tool(name="gold_describe_template", annotations=READ_ONLY)
    def gold_describe_template(
        name: Annotated[str, Field(description="Template name from gold_list_templates")],
    ) -> str:
        """Full detail of one template: description, typed parameters with
        defaults and allowed values, the columns it returns, its row cap and
        timeout, and the SQL it runs (read-only, for transparency)."""
        return guarded(lambda: svc().describe_template(name))

    @mcp.tool(name="gold_describe_table", annotations=READ_ONLY)
    def gold_describe_table(
        name: Annotated[str, Field(description="Gold table name, e.g. 'funnel_analysis'")],
    ) -> str:
        """Columns of one gold table (from the catalog), which templates read
        it, and the active release it comes from."""
        return guarded(lambda: svc().describe_table(name))

    @mcp.tool(name="gold_run_template", annotations=READ_ONLY)
    def gold_run_template(
        name: Annotated[str, Field(description="Template name")],
        params: Annotated[dict | None, Field(description="Parameter values by name, as "
                                                         "gold_describe_template declares "
                                                         "them")] = None,
    ) -> str:
        """Execute a pre-approved template with validated, server-side-bound
        parameters under a row cap and a statement timeout, as a role that can
        read only gold. Returns JSON: columns, rows, row_count, truncated."""
        return guarded(lambda: svc().run_template(name, params))

    return mcp


def main() -> int:
    build_server().run(transport="stdio")
    return 0


if __name__ == "__main__":
    sys.exit(main())
