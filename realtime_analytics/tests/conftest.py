"""Shared fixtures.

Every test in this suite runs against a **real ClickHouse engine** embedded
in-process via ``chdb`` -- the same engine the docker deployment runs, just
without a server. Nothing here is mocked, so a test that passes is evidence
that the SQL is correct, not that a stub was called.

``chdb`` allows exactly one embedded server per process, so a single
session-scoped backend hosts everything:

* the production schema (``clickhouse/10_production.sql``) populated from the
  Python event generator -- used by the API and materialized-view tests;
* the benchmark stage tables (``events_v0_naive`` ... ``events_v6_projected``)
  populated by the in-engine generator -- used to prove every schema variant
  returns identical answers.

The two sets have disjoint table names, so they coexist. ``05_materialized_
views.sql`` is deliberately not applied here: its target tables are the same
ones ``10_production.sql`` creates, sourced from the production events table.
"""
import os
import sys
from datetime import datetime, timedelta

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from realtime_analytics import data_gen  # noqa: E402
from realtime_analytics.db import split_statements  # noqa: E402
from realtime_analytics.events import COLUMNS, generate_events  # noqa: E402

SQL_DIR = os.path.join(ROOT, "clickhouse")

#: Small enough to build in a couple of seconds, large enough that the
#: aggregates have several rows per day and the percentiles are meaningful.
PROD_EVENTS = 20_000
BENCH_ROWS = 120_000

STAGE_FILES = [
    "00_naive.sql",
    "01_types_codecs.sql",
    "02_sorting_key.sql",
    "03_partitioning.sql",
    "04_skipping_indexes.sql",
    "06_projections.sql",
]

STAGE_TABLES = {
    "naive": "events_v0_naive",
    "v1": "events_v1_typed",
    "v2": "events_v2_sorted",
    "v3": "events_v3_partitioned",
    "v4": "events_v4_indexed",
    "v6": "events_v6_projected",
}

try:
    import chdb  # noqa: F401

    CHDB_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the environment
    CHDB_AVAILABLE = False


def _read_sql(name: str) -> str:
    with open(os.path.join(SQL_DIR, name)) as handle:
        return handle.read()


def _escape(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _insert_events(backend, table: str, events: list) -> None:
    """Insert Python-generated events, in chunks, as a VALUES literal."""
    chunk_size = 5000
    columns = ", ".join(COLUMNS)
    for start in range(0, len(events), chunk_size):
        chunk = events[start:start + chunk_size]
        rows = []
        for event in chunk:
            values = ", ".join(_escape(v) for v in event.to_row())
            rows.append(f"({values})")
        backend.command(
            f"INSERT INTO {table} ({columns}) VALUES {', '.join(rows)}"
        )


@pytest.fixture(scope="session")
def ch():
    """One embedded ClickHouse holding both schemas, built once per run."""
    if not CHDB_AVAILABLE:
        pytest.skip("chdb is not installed; install the 'test' extra")

    import tempfile

    from realtime_analytics.db import ChdbBackend

    backend = ChdbBackend(tempfile.mkdtemp(prefix="realtime-analytics-"))
    # See bench_clickhouse.py: the query condition cache remembers which
    # granules matched a predicate, so re-running a query against an
    # unindexed table skips granules and mimics an index. Tests that assert
    # on index pruning must not be at the mercy of execution order.
    backend.apply_settings(use_query_condition_cache=0)

    # --- production schema + Python-generated events ----------------------
    for statement in split_statements(_read_sql("10_production.sql")):
        backend.command(statement)

    # Events must land inside the API's default lookback windows, so they are
    # generated relative to now rather than at a fixed anchor.
    events = list(
        generate_events(
            PROD_EVENTS,
            seed=11,
            accounts=8,
            start_ts=datetime.now() - timedelta(days=3),
            window_hours=70,
        )
    )
    _insert_events(backend, "conversation_events", events)
    for table in ("conversation_events", "conversation_daily",
                  "agent_hourly", "platform_daily"):
        backend.command(f"OPTIMIZE TABLE {table} FINAL")

    # --- benchmark stage tables + in-engine generated events --------------
    for name in STAGE_FILES:
        for statement in split_statements(_read_sql(name)):
            backend.command(statement)
    backend.command(data_gen.insert_naive("events_v0_naive", BENCH_ROWS))
    backend.command(data_gen.insert_typed("events_v1_typed", BENCH_ROWS))
    columns = ", ".join(data_gen.TYPED_COLUMNS)
    for table in ("events_v2_sorted", "events_v3_partitioned",
                  "events_v4_indexed", "events_v6_projected"):
        backend.command(
            f"INSERT INTO {table} ({columns}) "
            f"SELECT {columns} FROM events_v1_typed"
        )
    for table in STAGE_TABLES.values():
        backend.command(f"OPTIMIZE TABLE {table} FINAL")

    yield backend
    backend.close()


@pytest.fixture(scope="session")
def prod_events():
    """The exact event list loaded into ``conversation_events``."""
    return list(
        generate_events(
            PROD_EVENTS,
            seed=11,
            accounts=8,
            start_ts=datetime.now() - timedelta(days=3),
            window_hours=70,
        )
    )


@pytest.fixture(scope="session")
def busiest_account(ch):
    return ch.query(
        "SELECT account_id, count() AS c FROM conversation_events "
        "GROUP BY account_id ORDER BY c DESC LIMIT 1"
    ).rows[0]["account_id"]


@pytest.fixture(scope="session")
def api_client(ch):
    """FastAPI TestClient wired to the shared embedded backend.

    The app's lifespan would otherwise build a second chdb session (which the
    embedded server forbids) and close it on shutdown, so ``get_backend`` is
    patched to hand back a non-closing proxy over the shared one.
    """
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from realtime_analytics import api as api_module

    class NonClosingProxy:
        name = "chdb"

        def query(self, sql, params=None):
            return ch.query(sql, params)

        def command(self, sql, params=None):
            return ch.command(sql, params)

        def close(self):
            """No-op: the session outlives any single app lifespan."""

    api_module.get_backend = lambda *a, **kw: NonClosingProxy()
    with TestClient(api_module.app) as client:
        yield client
