"""Templates on Trino: same YAML, same lint, same binding, a second engine.

The template's ``%(name)s`` placeholders become Trino's positional ``?``
markers (a name used twice binds twice), values are sent as prepared
statement parameters, the statement runs under ``query_max_execution_time``
and inside the same ``LIMIT max_rows + 1`` wrapper. The connection asserts
user ``mcp_reader``, which ``../trino_deployment/etc/access-control-rules.json``
makes read-only on every catalog. Trino here has no authentication, so that
user name is asserted rather than proven: the rule is a guardrail on the
server's own connection, and the PostgreSQL role beneath ``postgres_gold`` is
the boundary that does not depend on it.
"""
from __future__ import annotations

import re
import time
from datetime import date

from mcp_deployment import config
from mcp_deployment.templates import (PLACEHOLDER, QueryResult, Template, TemplateError,
                                      _plain, bind)


def positional(sql: str, bound: dict) -> tuple[str, list]:
    """``%(a)s ... %(a)s`` -> ``? ... ?`` with ``[a, a]``."""
    values: list = []

    def swap(match: re.Match) -> str:
        values.append(bound[match.group(1)])
        return "?"

    return PLACEHOLDER.sub(swap, sql).replace("%%", "%"), values


def connect(timeout_ms: int):
    import trino

    return trino.dbapi.connect(
        host=config.TRINO_HOST, port=config.TRINO_PORT, user=config.TRINO_USER,
        session_properties={"query_max_execution_time": f"{int(timeout_ms)}ms"})


def execute(template: Template, params: dict | None, conn=None) -> QueryResult:
    import trino.exceptions

    if template.engine != "trino":
        raise TemplateError(f"template '{template.name}' is a {template.engine} template")
    bound = bind(template, params)
    sql, values = positional(
        f"SELECT * FROM (\n{template.sql}\n) AS template_result LIMIT {int(template.max_rows) + 1}",
        bound)
    own = conn is None
    conn = conn or connect(template.timeout_ms)
    started = time.perf_counter()
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        fetched = cur.fetchall()
        columns = [d[0] for d in cur.description]
    except trino.exceptions.TrinoQueryError as exc:
        # TrinoUserError, TrinoExternalError and the INSUFFICIENT_RESOURCES
        # class the time limit raises all derive from this one.
        name = getattr(exc, "error_name", "") or ""
        if name == "EXCEEDED_TIME_LIMIT":
            raise TemplateError(f"template '{template.name}' exceeded its {template.timeout_ms} ms "
                                "timeout; narrow the parameters") from None
        if name == "PERMISSION_DENIED":
            raise TemplateError(f"template '{template.name}' touched something the reader "
                                f"cannot see: {exc.message}") from None
        if name == "JDBC_ERROR":
            raise TemplateError(f"template '{template.name}' failed in a connector: "
                                f"{exc.message}") from None
        raise TemplateError(f"template '{template.name}' failed: {exc.message}") from None
    finally:
        if own:
            conn.close()
    elapsed = (time.perf_counter() - started) * 1000
    truncated = len(fetched) > template.max_rows
    rows = [[_plain(v) for v in r] for r in fetched[:template.max_rows]]
    return QueryResult(template.name, columns, rows, len(rows), truncated, round(elapsed, 2),
                       {k: (v.isoformat() if isinstance(v, date) else v) for k, v in bound.items()})


def reachable() -> bool:
    try:
        conn = connect(2000)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        conn.close()
        return True
    except Exception:  # noqa: BLE001 -- any failure means "no Trino here"
        return False
