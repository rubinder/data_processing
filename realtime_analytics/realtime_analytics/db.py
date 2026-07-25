"""ClickHouse access with two interchangeable backends.

``http``  -- clickhouse-connect against a real server or cluster. What the
            docker-compose deployment and production use.
``chdb``  -- ClickHouse embedded in-process. Same engine, same SQL, no server.
            Used by the test suite and by the benchmark so that both run on a
            laptop with no daemon, and so CI can assert on real query results
            rather than mocks.

Both expose the same tiny surface: ``query`` returns rows as dicts plus the
engine-reported statistics (elapsed, rows_read, bytes_read), which is what the
benchmark needs -- wall-clock alone cannot tell you *why* a query got faster.

Queries use ClickHouse's native parameter syntax, ``{name:Type}``. The HTTP
backend passes them to the server, which binds them safely. The embedded
backend has no binding API, so parameters are escaped and substituted here;
the escaping is strict and type-checked rather than string formatting.
"""
import json
import os
import re
import time
from dataclasses import dataclass, field

PARAM_RE = re.compile(r"\{(\w+):(\w+(?:\(\d+\))?)\}")


@dataclass
class QueryResult:
    """Rows plus the engine's own accounting for a single query."""

    rows: list[dict]
    elapsed_s: float
    rows_read: int = 0
    bytes_read: int = 0
    wall_s: float = 0.0
    meta: list[dict] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.rows)


def _escape_string(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _render_param(value, ch_type: str) -> str:
    """Render a single parameter as a literal for the embedded backend."""
    base = ch_type.split("(")[0]
    if base in {"UInt8", "UInt16", "UInt32", "UInt64",
                "Int8", "Int16", "Int32", "Int64"}:
        return str(int(value))
    if base in {"Float32", "Float64"}:
        return repr(float(value))
    if base == "UUID":
        # Validated by construction: anything that is not a UUID raises.
        import uuid as _uuid

        return f"toUUID('{_uuid.UUID(str(value))}')"
    if base in {"Date", "Date32"}:
        return f"toDate({_escape_string(value)})"
    if base in {"DateTime", "DateTime64"}:
        return f"toDateTime64({_escape_string(value)}, 3)"
    if base in {"String", "LowCardinality"}:
        return _escape_string(value)
    raise ValueError(f"Unsupported parameter type for embedded backend: {ch_type}")


def render_query(sql: str, params: dict | None) -> str:
    """Substitute ``{name:Type}`` placeholders with escaped literals."""
    params = params or {}

    def replace(match: re.Match) -> str:
        name, ch_type = match.group(1), match.group(2)
        if name not in params:
            raise KeyError(f"Missing query parameter: {name}")
        return _render_param(params[name], ch_type)

    return PARAM_RE.sub(replace, sql)


class ChdbBackend:
    """Embedded ClickHouse. Real engine, in-process, no server required."""

    name = "chdb"

    def __init__(self, path: str | None = None):
        import chdb.session as chs

        self._session = chs.Session(path) if path else chs.Session()

    def query(self, sql: str, params: dict | None = None) -> QueryResult:
        rendered = render_query(sql, params)
        started = time.perf_counter()
        raw = self._session.query(rendered, "JSON")
        wall = time.perf_counter() - started
        text = str(raw)
        if not text.strip():
            return QueryResult(rows=[], elapsed_s=0.0, wall_s=wall)
        payload = json.loads(text)
        stats = payload.get("statistics", {})
        return QueryResult(
            rows=payload.get("data", []),
            elapsed_s=float(stats.get("elapsed", 0.0)),
            rows_read=int(stats.get("rows_read", 0)),
            bytes_read=int(stats.get("bytes_read", 0)),
            wall_s=wall,
            meta=payload.get("meta", []),
        )

    def command(self, sql: str, params: dict | None = None) -> None:
        self._session.query(render_query(sql, params))

    def apply_settings(self, **settings) -> None:
        for name, value in settings.items():
            try:
                self._session.query(f"SET {name} = {value}")
            except Exception:  # noqa: BLE001 - setting absent on this version
                pass

    def close(self) -> None:
        self._session.close()


class HttpBackend:
    """clickhouse-connect against a ClickHouse server or cluster."""

    name = "http"

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ):
        import clickhouse_connect

        self._client = clickhouse_connect.get_client(
            host=host or os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(port or os.getenv("CLICKHOUSE_PORT", "8123")),
            username=username or os.getenv("CLICKHOUSE_USER", "default"),
            password=password or os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=database or os.getenv("CLICKHOUSE_DATABASE", "default"),
        )

    def query(self, sql: str, params: dict | None = None) -> QueryResult:
        started = time.perf_counter()
        result = self._client.query(sql, parameters=params or {})
        wall = time.perf_counter() - started
        columns = result.column_names
        rows = [dict(zip(columns, row)) for row in result.result_rows]
        summary = result.summary or {}
        return QueryResult(
            rows=rows,
            elapsed_s=float(summary.get("elapsed", 0.0) or 0.0),
            rows_read=int(summary.get("read_rows", 0) or 0),
            bytes_read=int(summary.get("read_bytes", 0) or 0),
            wall_s=wall,
        )

    def command(self, sql: str, params: dict | None = None) -> None:
        self._client.command(sql, parameters=params or {})

    def apply_settings(self, **settings) -> None:
        for name, value in settings.items():
            try:
                self._client.command(f"SET {name} = {value}")
            except Exception:  # noqa: BLE001 - setting absent on this version
                pass

    def close(self) -> None:
        self._client.close()


def get_backend(kind: str | None = None, **kwargs):
    """Build the backend named by ``ANALYTICS_BACKEND`` (``http``/``chdb``)."""
    kind = (kind or os.getenv("ANALYTICS_BACKEND", "http")).lower()
    if kind == "chdb":
        return ChdbBackend(**kwargs)
    if kind == "http":
        return HttpBackend(**kwargs)
    raise ValueError(f"Unknown backend: {kind}")


def split_statements(script: str) -> list[str]:
    """Split a .sql file into statements, ignoring ``--`` comment lines."""
    lines = [
        line for line in script.splitlines()
        if not line.strip().startswith("--")
    ]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
