"""Shared pytest fixtures for the ClickHouse deployment tests.

The query tests run against an EMBEDDED ClickHouse via ``chdb`` (real
ClickHouse SQL semantics, in-process). If ``chdb`` cannot be imported the
query tests are skipped, but the pure loader tests still run.
"""
import json
import os
import sys

import pytest

# Make the top-level load_data module importable without relying on the
# editable install having exposed it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import chdb.session as chs

    CHDB_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the environment
    CHDB_AVAILABLE = False


LOCAL_TABLE = "impressions"

# Deterministic fixture: one tuple per impression as
# (user_id, impression_id, page_type, max_event_reached). Each impression is
# expanded into one row per event a..max_event. All rows share date/hour.
FIXTURE_DATE = "2026-01-01"
FIXTURE_HOUR = 10
EVENTS = "abcdef"

IMPRESSION_SPECS = [
    ("u1", "p1i1", 1, "b"),
    ("u2", "p1i2", 1, "d"),
    ("u1", "p2i1", 2, "c"),
    ("u2", "p2i2", 2, "e"),
    ("u3", "p2i3", 2, "a"),
    ("u1", "p3i1", 3, "f"),
    ("u3", "p3i2", 3, "d"),
    ("u4", "p3i3", 3, "b"),
]


def _fixture_rows():
    rows = []
    for user_id, impression_id, page_type, max_event in IMPRESSION_SPECS:
        depth = EVENTS.index(max_event) + 1
        for i, event_type in enumerate(EVENTS[:depth]):
            rows.append(
                (
                    user_id,
                    impression_id,
                    page_type,
                    FIXTURE_DATE,
                    FIXTURE_HOUR,
                    0,
                    i * 5,
                    event_type,
                )
            )
    return rows


def _insert_values(rows):
    parts = []
    for r in rows:
        parts.append(
            "('{0}','{1}',{2},'{3}',{4},{5},{6},'{7}')".format(*r)
        )
    return ", ".join(parts)


class ChdbRunner:
    """Thin wrapper around a chdb session returning rows as list[dict]."""

    def __init__(self, session, table):
        self.session = session
        self.table = table

    def query(self, sql):
        rendered = sql.replace("{table}", self.table)
        result = self.session.query(rendered, "JSON")
        payload = json.loads(str(result))
        return payload.get("data", [])


@pytest.fixture(scope="session")
def ch():
    if not CHDB_AVAILABLE:
        pytest.skip("chdb is not available; skipping ClickHouse query tests")

    session = chs.Session()
    session.query(
        f"""
        CREATE TABLE {LOCAL_TABLE}
        (
            user_id       String,
            impression_id String,
            page_type     UInt8,
            date          Date,
            hour          UInt8,
            minute        UInt8,
            second        UInt8,
            event_type    String
        )
        ENGINE = MergeTree
        ORDER BY (page_type, date, hour, impression_id)
        """
    )
    session.query(
        f"INSERT INTO {LOCAL_TABLE} "
        f"(user_id, impression_id, page_type, date, hour, minute, second, event_type) "
        f"VALUES {_insert_values(_fixture_rows())}"
    )
    yield ChdbRunner(session, LOCAL_TABLE)
    session.close()
