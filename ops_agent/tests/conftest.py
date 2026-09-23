"""Fixtures: a fake engine for the rules, a real Spark session for the rest.

The Spark session is session-scoped (~10 s to start) and backed by a real
local Iceberg catalog in a temp dir; tests that use it drop and recreate the
data tables so they do not share state.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

import pyarrow as pa
import pytest

from ops_agent import actions, config, tables
from tests.fake_engine import FakeEngine

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


@pytest.fixture(autouse=True)
def isolate_side_effects(tmp_path, monkeypatch):
    """Incidents and reports go to a temp dir; the LLM stays off."""
    monkeypatch.setattr(actions, "INCIDENTS_DIR", tmp_path / "incidents")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("AS_OF_DATE", raising=False)


def raw_rows(count: int = 60, start: datetime | None = None, seed: int = 7) -> pa.Table:
    """Impression rows shaped like ``iceberg_deployment.impressions.sample_rows``."""
    from ops_agent.lakehouse import batch_rows

    rows = batch_rows(count, start or datetime(2026, 6, 1), seed)
    return pa.Table.from_pylist(
        [dict(zip(("user_id", "impression_id", "page_type", "event_ts", "event_type"), r))
         for r in rows],
        schema=tables.RAW_IMPRESSIONS.arrow_schema())


def aggregate(raw: pa.Table) -> pa.Table:
    """The same rollup ``lakehouse.AGGREGATE_SQL`` computes, via DuckDB."""
    import duckdb

    con = duckdb.connect()
    con.register("raw", raw)
    out = con.execute("""
        SELECT impression_id, user_id, CAST(page_type AS INTEGER) AS page_type,
               CAST(min(event_ts) AS DATE) AS event_date,
               CAST(count(*) AS INTEGER) AS funnel_depth,
               max(event_type) AS max_event_type,
               min(event_ts) AS first_event_ts, max(event_ts) AS last_event_ts
        FROM raw GROUP BY impression_id, user_id, page_type""")
    out = out.to_arrow_table() if hasattr(out, "to_arrow_table") else out.fetch_arrow_table()
    con.close()
    return out.cast(tables.AGGREGATED_IMPRESSIONS.arrow_schema())


AS_OF = date(2026, 6, 4)  # sample_rows spans 2026-06-01 .. 06-04


@pytest.fixture
def fake() -> FakeEngine:
    """A clean lakehouse: three raw batches, an aggregated rebuild, no ops tables."""
    engine = FakeEngine()
    engine.create_table(tables.RAW_IMPRESSIONS)
    for seed in (7, 11, 13):
        engine.append(config.RAW_TABLE, raw_rows(60, seed=seed))
    engine.create_table(tables.AGGREGATED_IMPRESSIONS)
    engine.overwrite(config.AGGREGATED_TABLE, aggregate(engine.tables[config.RAW_TABLE]))
    return engine


def _java_home() -> str | None:
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for version in ("17", "11"):
        found = os.popen(f"/usr/libexec/java_home -v {version} 2>/dev/null").read().strip()
        if found:
            return found
    return None


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    java_home = _java_home()
    if not java_home:
        pytest.skip("no JDK found; PySpark cannot start")
    os.environ["JAVA_HOME"] = java_home
    os.environ.setdefault("TZ", "UTC")
    time.tzset()
    from iceberg_deployment.session import get_spark_session

    warehouse = tempfile.mkdtemp(prefix="ops-agent-tests-")
    session = get_spark_session("ops-agent-tests", catalog_type="hadoop", warehouse=warehouse)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def engine(spark):
    """A SparkEngine with every known table dropped first."""
    from ops_agent.engine import SparkEngine

    eng = SparkEngine(spark)
    for table in tables.ALL_TABLES:
        eng.drop_table(table.name)
    return eng


def next_day(day: date, n: int = 1) -> date:
    return day + timedelta(days=n)
