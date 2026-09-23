"""Unit tests need nothing. PostgreSQL tests need the dbt deployment's
database (pgvector image) reachable; they skip otherwise."""
from __future__ import annotations

import os
import uuid

import pytest

from mcp_deployment import config, db

os.environ.setdefault("CATALOG_EMBEDDER", "hash")


def _reachable() -> bool:
    try:
        with db.connect(config.admin_db(), connect_timeout=2):
            return True
    except Exception:  # noqa: BLE001 -- any failure means "no database here"
        return False


@pytest.fixture(scope="session")
def admin():
    """Admin connection with sql/*.sql applied. Skips when no database."""
    if not _reachable():
        pytest.skip("PostgreSQL not reachable; start ../dbt_deployment first")
    with db.connect(config.admin_db(), autocommit=True) as conn:
        db.apply_sql_files(conn)
        yield conn
        # Leave gold pointing at the real dbt output if it exists, so a test
        # run does not leave the demo reading synthetic tables.
        has_real = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s "
            "AND table_type = 'BASE TABLE'", (config.SOURCE_SCHEMA,)).fetchone()[0]
        if has_real:
            conn.execute("SELECT gold_ops.promote(%s, %s)",
                         (config.SOURCE_SCHEMA, "restore after tests"))


GOLD_DDL = {
    "funnel_analysis": ("page_type int, event_type varchar(1), impressions_at_stage bigint, "
                        "total_impressions bigint, pct_of_total numeric, "
                        "pct_from_previous_stage numeric"),
    "page_type_summary": ("page_type int, total_impressions bigint, unique_users bigint, "
                          "avg_funnel_depth numeric, max_funnel_depth bigint, "
                          "avg_duration_seconds numeric, impressions_reaching_d bigint, "
                          "impressions_reaching_e bigint, impressions_reaching_f bigint, "
                          "pct_reaching_d numeric, pct_reaching_e numeric, pct_reaching_f numeric"),
    "user_engagement": ("user_id varchar, total_impressions bigint, page_types_visited bigint, "
                        "avg_funnel_depth numeric, max_funnel_depth bigint, total_events numeric, "
                        "first_seen timestamp, last_seen timestamp, most_engaged_page_type int, "
                        "impressions_on_top_page bigint"),
    "hourly_traffic": ("event_date date, hour int, page_type int, total_impressions bigint, "
                       "unique_users bigint, avg_funnel_depth numeric, "
                       "impressions_reaching_d bigint, pct_reaching_d numeric"),
}


@pytest.fixture
def candidate(admin):
    """A throwaway candidate schema shaped like dbt's analysis output, with a
    few rows, plus an int_ table that promotion must skip."""
    schema = f"cand_{uuid.uuid4().hex[:8]}"
    admin.execute(f"CREATE SCHEMA {schema}")
    for name, columns in GOLD_DDL.items():
        admin.execute(f"CREATE TABLE {schema}.{name} ({columns})")
    admin.execute(f"CREATE TABLE {schema}.int_impressions_aggregated (x int)")
    admin.execute(f"""INSERT INTO {schema}.funnel_analysis VALUES
        (1, 'a', 1000, 1000, 100, NULL), (1, 'b', 600, 1000, 60, 60),
        (3, 'a', 500, 500, 100, NULL), (3, 'f', 50, 500, 10, 50)""")
    admin.execute(f"""INSERT INTO {schema}.page_type_summary VALUES
        (1, 1000, 400, 1.6, 4, 12.5, 100, 0, 0, 10, 0, 0),
        (3, 500, 300, 3.1, 6, 40.2, 250, 100, 50, 50, 20, 10)""")
    admin.execute(f"""INSERT INTO {schema}.user_engagement
        SELECT 'user_' || i, 100 - i, 2, 2.5, 5, 250 - i, '2026-06-01', '2026-06-04', 3, 40
        FROM generate_series(1, 30) AS i""")
    admin.execute(f"""INSERT INTO {schema}.hourly_traffic
        SELECT d::date, h, p, 100 + h, 50, 2.0, 30, 30.0
        FROM generate_series('2026-06-01'::date, '2026-06-07'::date, '1 day') AS d,
             generate_series(0, 23) AS h, generate_series(1, 3) AS p""")
    yield schema
    admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def role_conn():
    """Factory for short-lived stage-role connections."""
    opened = []

    def _open(role: str):
        conn = db.connect(config.role_db(role))
        opened.append(conn)
        return conn

    yield _open
    for conn in opened:
        conn.close()
