"""Pre-approved, parameterized query templates. The only way to read gold.

A template is a YAML file: a name, a description an agent can search for,
the SQL with ``%(param)s`` placeholders, and a typed declaration of every
parameter. Loading validates the set, so a template that could not be run
safely never becomes runnable:

- one statement, no ``;``, starting with SELECT or WITH;
- every placeholder declared, every declared parameter used;
- every FROM/JOIN target is a ``gold.`` table or a CTE the template defines.

Execution binds parameters server-side through psycopg (no string building),
wraps the query in a row cap, and runs under a statement timeout. The role it
runs as can SELECT nothing but gold, so the lint above is a courtesy to the
author; the database is the boundary.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import yaml

from mcp_deployment import config

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
PLACEHOLDER = re.compile(r"%\((\w+)\)s")
STRAY_PERCENT = re.compile(r"%(?!\()(?!%)")
FROM_TARGET = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.IGNORECASE)
CTE_NAME = re.compile(r"(?:\bwith\b|,)\s*([a-zA-Z_]\w*)\s+as\s*\(", re.IGNORECASE)
COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
PARAM_TYPES = ("int", "float", "str", "date", "bool")


class TemplateError(Exception):
    """A template that cannot be trusted, or a call that does not fit one."""


@dataclass(frozen=True)
class Param:
    name: str
    type: str
    description: str = ""
    required: bool = True
    default: object = None
    choices: tuple | None = None
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class Template:
    name: str
    description: str
    sql: str
    params: tuple[Param, ...] = ()
    returns: tuple[str, ...] = ()
    max_rows: int = 1000
    timeout_ms: int = 5000
    tags: tuple[str, ...] = ()
    source: str = ""

    def param(self, name: str) -> Param | None:
        return next((p for p in self.params if p.name == name), None)


@dataclass(frozen=True)
class QueryResult:
    template: str
    columns: list[str]
    rows: list[list]
    row_count: int
    truncated: bool
    elapsed_ms: float
    params: dict = field(default_factory=dict)


def _strip_comments(sql: str) -> str:
    return COMMENT.sub(" ", sql)


def validate_template(t: Template) -> None:
    where = f"template '{t.name}'"
    if not IDENTIFIER.match(t.name):
        raise TemplateError(f"{where}: name must match {IDENTIFIER.pattern}")
    if not t.description.strip():
        raise TemplateError(f"{where}: needs a description; it is what the catalog searches")
    body = _strip_comments(t.sql).strip()
    if ";" in body:
        raise TemplateError(f"{where}: one statement only, no ';'")
    if not re.match(r"^(select|with)\b", body, re.IGNORECASE):
        raise TemplateError(f"{where}: must start with SELECT or WITH")
    if STRAY_PERCENT.search(body):
        raise TemplateError(f"{where}: a bare '%' outside a %(name)s placeholder; "
                            "write '%%' for a literal percent")
    used = set(PLACEHOLDER.findall(body))
    declared = {p.name for p in t.params}
    if used - declared:
        raise TemplateError(f"{where}: undeclared placeholder(s) {sorted(used - declared)}")
    if declared - used:
        raise TemplateError(f"{where}: declared but unused parameter(s) "
                            f"{sorted(declared - used)}")
    ctes = {m.lower() for m in CTE_NAME.findall(body)}
    for target in FROM_TARGET.findall(body):
        low = target.lower()
        if low in ctes:
            continue
        if not low.startswith(f"{config.GOLD_SCHEMA}."):
            raise TemplateError(f"{where}: reads '{target}', which is not a "
                                f"{config.GOLD_SCHEMA}.<table> or a CTE of this template")
    for p in t.params:
        if p.type not in PARAM_TYPES:
            raise TemplateError(f"{where}: parameter '{p.name}' has unknown type "
                                f"'{p.type}' (expected one of {PARAM_TYPES})")
        if not IDENTIFIER.match(p.name):
            raise TemplateError(f"{where}: parameter name '{p.name}' is not an identifier")
        if not p.required and p.default is None and p.choices is None:
            # Optional with no default means the SQL must handle NULL; allowed,
            # but the description has to say so, or the agent cannot know.
            if "null" not in p.description.lower():
                raise TemplateError(f"{where}: optional parameter '{p.name}' has no default; "
                                    "its description must say what NULL means")
    if t.max_rows <= 0 or t.max_rows > 100_000:
        raise TemplateError(f"{where}: max_rows must be in 1..100000")
    if t.timeout_ms <= 0 or t.timeout_ms > 60_000:
        raise TemplateError(f"{where}: timeout_ms must be in 1..60000")


def _param_from_yaml(raw: dict, where: str) -> Param:
    if "name" not in raw or "type" not in raw:
        raise TemplateError(f"{where}: every parameter needs 'name' and 'type'")
    return Param(
        name=raw["name"], type=raw["type"], description=raw.get("description", ""),
        required=bool(raw.get("required", "default" not in raw)),
        default=raw.get("default"),
        choices=tuple(raw["choices"]) if raw.get("choices") is not None else None,
        minimum=raw.get("min"), maximum=raw.get("max"))


def load_template(path: Path) -> Template:
    raw = yaml.safe_load(Path(path).read_text())
    where = f"templates/{Path(path).name}"
    if not isinstance(raw, dict):
        raise TemplateError(f"{where}: empty or not a mapping")
    for key in ("name", "description", "sql"):
        if key not in raw:
            raise TemplateError(f"{where}: missing required key '{key}'")
    template = Template(
        name=raw["name"], description=raw["description"].strip(), sql=raw["sql"].strip(),
        params=tuple(_param_from_yaml(p, where) for p in raw.get("params") or []),
        returns=tuple(raw.get("returns") or []),
        max_rows=int(raw.get("max_rows", 1000)), timeout_ms=int(raw.get("timeout_ms", 5000)),
        tags=tuple(raw.get("tags") or []), source=Path(path).name)
    validate_template(template)
    return template


def load_templates(directory: Path | None = None) -> dict[str, Template]:
    root = Path(directory or config.TEMPLATE_DIR)
    paths = sorted(root.glob("*.yaml"))
    if not paths:
        raise TemplateError(f"no templates found in {root}/; with no templates the server "
                            "has nothing it is allowed to run")
    out: dict[str, Template] = {}
    for path in paths:
        template = load_template(path)
        if template.name in out:
            raise TemplateError(f"duplicate template name '{template.name}' in "
                                f"{out[template.name].source} and {path.name}")
        out[template.name] = template
    return out


def _coerce(p: Param, value, where: str):
    try:
        if p.type == "int":
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise ValueError
            return int(value)
        if p.type == "float":
            return float(value)
        if p.type == "str":
            if not isinstance(value, str):
                raise ValueError
            return value
        if p.type == "date":
            return value if isinstance(value, date) else date.fromisoformat(str(value))
        if p.type == "bool":
            if isinstance(value, bool):
                return value
            if str(value).lower() in ("true", "false"):
                return str(value).lower() == "true"
            raise ValueError
    except (TypeError, ValueError):
        raise TemplateError(f"{where}: parameter '{p.name}' expects {p.type}, "
                            f"got {value!r}") from None
    raise TemplateError(f"{where}: unsupported type {p.type}")


def bind(template: Template, provided: dict | None) -> dict:
    """Validate and coerce a call's parameters. Unknown, missing, out-of-range
    or wrong-typed values are errors that name the parameter."""
    provided = dict(provided or {})
    where = f"template '{template.name}'"
    unknown = set(provided) - {p.name for p in template.params}
    if unknown:
        raise TemplateError(f"{where}: unknown parameter(s) {sorted(unknown)}; "
                            f"accepts {[p.name for p in template.params]}")
    bound: dict = {}
    for p in template.params:
        if p.name not in provided or provided[p.name] is None:
            if p.required:
                raise TemplateError(f"{where}: parameter '{p.name}' is required")
            bound[p.name] = p.default
            continue
        value = _coerce(p, provided[p.name], where)
        if p.choices is not None and value not in p.choices:
            raise TemplateError(f"{where}: parameter '{p.name}' must be one of "
                                f"{list(p.choices)}, got {value!r}")
        if p.minimum is not None and value < p.minimum:
            raise TemplateError(f"{where}: parameter '{p.name}' must be >= {p.minimum}")
        if p.maximum is not None and value > p.maximum:
            raise TemplateError(f"{where}: parameter '{p.name}' must be <= {p.maximum}")
        bound[p.name] = value
    return bound


def build_query(template: Template) -> str:
    """The template wrapped in its row cap. One extra row is fetched so
    truncation is reported rather than guessed."""
    return f"SELECT * FROM (\n{template.sql}\n) AS template_result LIMIT %(__cap)s"


def _plain(value):
    """JSON-friendly cell values: Decimal -> float, dates -> ISO strings."""
    from datetime import datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def execute(conn, template: Template, params: dict | None) -> QueryResult:
    """Run ``template`` on ``conn`` with bound parameters, a statement timeout
    and the row cap. ``conn`` should be the reader role's connection."""
    import psycopg

    bound = bind(template, params)
    started = time.perf_counter()
    try:
        with conn.transaction():
            # set_config(..., is_local => true) scopes it to this transaction;
            # SET LOCAL cannot take a bound parameter.
            conn.execute("SELECT set_config('statement_timeout', %s, true)",
                         (str(template.timeout_ms),))
            with conn.cursor() as cur:
                cur.execute(build_query(template), {**bound, "__cap": template.max_rows + 1})
                columns = [d.name for d in cur.description]
                fetched = cur.fetchall()
    except psycopg.errors.QueryCanceled:
        raise TemplateError(f"template '{template.name}' exceeded its {template.timeout_ms} ms "
                            "timeout; narrow the parameters") from None
    except psycopg.errors.InsufficientPrivilege as exc:
        raise TemplateError(f"template '{template.name}' touched something the reader role "
                            f"cannot see: {exc.diag.message_primary}") from None
    except psycopg.Error as exc:
        # Anything else the database rejects is a template defect, reported
        # to the caller as a message rather than a stack trace.
        raise TemplateError(f"template '{template.name}' failed: "
                            f"{getattr(exc.diag, 'message_primary', None) or exc}") from None
    elapsed = (time.perf_counter() - started) * 1000
    truncated = len(fetched) > template.max_rows
    rows = [[_plain(v) for v in r] for r in fetched[:template.max_rows]]
    return QueryResult(template.name, columns, rows, len(rows), truncated, round(elapsed, 2),
                       {k: (v.isoformat() if isinstance(v, date) else v) for k, v in bound.items()})
