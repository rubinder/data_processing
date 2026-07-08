"""Execute each queries/*.sql against an embedded ClickHouse (chdb) and assert
the exact numbers implied by the deterministic fixture in conftest.py.

Fixture impressions (user, impression, page_type, deepest event):
    p1i1 u1 pt1 -> b      (depth 2)
    p1i2 u2 pt1 -> d      (depth 4)
    p2i1 u1 pt2 -> c      (depth 3)
    p2i2 u2 pt2 -> e      (depth 5)
    p2i3 u3 pt2 -> a      (depth 1)
    p3i1 u1 pt3 -> f      (depth 6)
    p3i2 u3 pt3 -> d      (depth 4)
    p3i3 u4 pt3 -> b      (depth 2)
"""
import os

import pytest

QUERIES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "queries"
)


def load_query(name):
    with open(os.path.join(QUERIES_DIR, f"{name}.sql")) as fh:
        return fh.read()


def find(rows, **filters):
    for row in rows:
        if all(str(row[k]) == str(v) for k, v in filters.items()):
            return row
    raise AssertionError(f"No row matching {filters} in {rows}")


def num(value):
    return float(value)


def test_funnel_analysis(ch):
    rows = ch.query(load_query("funnel_analysis"))

    p1a = find(rows, page_type=1, event_type="a")
    assert num(p1a["impressions_at_stage"]) == 2
    assert num(p1a["total_impressions"]) == 2
    assert num(p1a["pct_of_total"]) == 100.0
    assert p1a["pct_from_previous_stage"] is None

    p1c = find(rows, page_type=1, event_type="c")
    assert num(p1c["impressions_at_stage"]) == 1
    assert num(p1c["pct_of_total"]) == 50.0
    assert num(p1c["pct_from_previous_stage"]) == 50.0

    p1d = find(rows, page_type=1, event_type="d")
    assert num(p1d["pct_from_previous_stage"]) == 100.0

    p3f = find(rows, page_type=3, event_type="f")
    assert num(p3f["impressions_at_stage"]) == 1
    assert num(p3f["total_impressions"]) == 3
    assert num(p3f["pct_of_total"]) == 33.33
    assert num(p3f["pct_from_previous_stage"]) == 100.0


def test_page_type_summary(ch):
    rows = ch.query(load_query("page_type_summary"))

    pt1 = find(rows, page_type=1)
    assert num(pt1["total_impressions"]) == 2
    assert num(pt1["unique_users"]) == 2
    assert num(pt1["avg_funnel_depth"]) == 3.0
    assert num(pt1["max_funnel_depth"]) == 4
    assert num(pt1["impressions_reaching_d"]) == 1
    assert num(pt1["impressions_reaching_e"]) == 0
    assert num(pt1["pct_reaching_d"]) == 50.0

    pt2 = find(rows, page_type=2)
    assert num(pt2["total_impressions"]) == 3
    assert num(pt2["unique_users"]) == 3
    assert num(pt2["max_funnel_depth"]) == 5
    assert num(pt2["impressions_reaching_d"]) == 1
    assert num(pt2["impressions_reaching_e"]) == 1
    assert num(pt2["pct_reaching_d"]) == 33.33

    pt3 = find(rows, page_type=3)
    assert num(pt3["total_impressions"]) == 3
    assert num(pt3["avg_funnel_depth"]) == 4.0
    assert num(pt3["max_funnel_depth"]) == 6
    assert num(pt3["impressions_reaching_d"]) == 2
    assert num(pt3["impressions_reaching_f"]) == 1
    assert num(pt3["pct_reaching_d"]) == 66.67


def test_user_engagement(ch):
    rows = ch.query(load_query("user_engagement"))

    # Ordered by total_impressions DESC, user_id ASC -> u1 is first.
    assert rows[0]["user_id"] == "u1"

    u1 = find(rows, user_id="u1")
    assert num(u1["total_impressions"]) == 3
    assert num(u1["page_types_visited"]) == 3
    assert num(u1["max_funnel_depth"]) == 6
    assert num(u1["total_events"]) == 11
    assert num(u1["avg_funnel_depth"]) == 3.67

    u2 = find(rows, user_id="u2")
    assert num(u2["total_impressions"]) == 2
    assert num(u2["total_events"]) == 9
    assert num(u2["avg_funnel_depth"]) == 4.5

    u4 = find(rows, user_id="u4")
    assert num(u4["total_impressions"]) == 1
    assert num(u4["page_types_visited"]) == 1
    assert num(u4["most_engaged_page_type"]) == 3


def test_hourly_traffic(ch):
    rows = ch.query(load_query("hourly_traffic"))
    assert len(rows) == 3  # one per page_type, single date/hour

    pt2 = find(rows, page_type=2)
    assert pt2["event_date"] == "2026-01-01"
    assert num(pt2["hour"]) == 10
    assert num(pt2["total_impressions"]) == 3
    assert num(pt2["unique_users"]) == 3
    assert num(pt2["impressions_reaching_d"]) == 1
    assert num(pt2["pct_reaching_d"]) == 33.33

    pt3 = find(rows, page_type=3)
    assert num(pt3["total_impressions"]) == 3
    assert num(pt3["avg_funnel_depth"]) == 4.0
    assert num(pt3["impressions_reaching_d"]) == 2
    assert num(pt3["pct_reaching_d"]) == 66.67
