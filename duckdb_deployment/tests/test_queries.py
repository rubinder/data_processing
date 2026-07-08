"""Assertions on the four analytical queries against the fixture dataset.

Expected numeric values are documented and derived by hand in conftest.py.
"""
from app import queries


def _index(rows, *keys):
    """Index a list of dicts by a tuple of key columns."""
    return {tuple(row[k] for k in keys): row for row in rows}


def test_funnel_analysis(conn):
    rows = queries.funnel_analysis(conn)
    by_stage = _index(rows, "page_type", "event_type")

    # Structure
    expected_cols = {
        "page_type", "event_type", "impressions_at_stage",
        "total_impressions", "pct_of_total", "pct_from_previous_stage",
    }
    assert expected_cols.issubset(rows[0].keys())

    # page_type 1 (total 2)
    assert by_stage[(1, "a")]["impressions_at_stage"] == 2
    assert by_stage[(1, "d")]["impressions_at_stage"] == 1
    assert by_stage[(1, "d")]["pct_of_total"] == 50
    assert by_stage[(1, "a")]["pct_from_previous_stage"] is None
    assert by_stage[(1, "d")]["pct_from_previous_stage"] == 50
    # pt1 never reaches e or f
    assert (1, "e") not in by_stage

    # page_type 2 (total 3)
    assert by_stage[(2, "c")]["impressions_at_stage"] == 2
    assert by_stage[(2, "c")]["pct_of_total"] == 66.67
    assert by_stage[(2, "e")]["impressions_at_stage"] == 1
    assert by_stage[(2, "e")]["pct_of_total"] == 33.33
    assert by_stage[(2, "d")]["pct_from_previous_stage"] == 100
    assert by_stage[(2, "e")]["pct_from_previous_stage"] == 50
    assert (2, "f") not in by_stage

    # page_type 3 (total 3)
    assert by_stage[(3, "f")]["impressions_at_stage"] == 2
    assert by_stage[(3, "f")]["pct_of_total"] == 66.67
    assert by_stage[(3, "e")]["pct_from_previous_stage"] == 66.67
    assert by_stage[(3, "f")]["pct_from_previous_stage"] == 100


def test_page_type_summary(conn):
    rows = queries.page_type_summary(conn)
    by_pt = _index(rows, "page_type")

    assert len(rows) == 3

    pt1 = by_pt[(1,)]
    assert pt1["total_impressions"] == 2
    assert pt1["unique_users"] == 1
    assert pt1["avg_funnel_depth"] == 3.5
    assert pt1["max_funnel_depth"] == 4
    assert pt1["avg_duration_seconds"] == 2.5
    assert pt1["impressions_reaching_d"] == 1
    assert pt1["impressions_reaching_e"] == 0
    assert pt1["pct_reaching_d"] == 50

    pt2 = by_pt[(2,)]
    assert pt2["total_impressions"] == 3
    assert pt2["unique_users"] == 2
    assert pt2["avg_funnel_depth"] == 3.67
    assert pt2["max_funnel_depth"] == 5
    assert pt2["avg_duration_seconds"] == 2.67
    assert pt2["impressions_reaching_d"] == 2
    assert pt2["impressions_reaching_e"] == 1
    assert pt2["impressions_reaching_f"] == 0
    assert pt2["pct_reaching_d"] == 66.67
    assert pt2["pct_reaching_e"] == 33.33

    pt3 = by_pt[(3,)]
    assert pt3["total_impressions"] == 3
    assert pt3["unique_users"] == 3
    assert pt3["avg_funnel_depth"] == 5.33
    assert pt3["max_funnel_depth"] == 6
    assert pt3["avg_duration_seconds"] == 4.33
    assert pt3["impressions_reaching_d"] == 3
    assert pt3["impressions_reaching_e"] == 2
    assert pt3["impressions_reaching_f"] == 2
    assert pt3["pct_reaching_d"] == 100
    assert pt3["pct_reaching_f"] == 66.67


def test_user_engagement(conn):
    rows = queries.user_engagement(conn)
    by_user = _index(rows, "user_id")

    assert len(rows) == 3

    u1 = by_user[("u1",)]
    assert u1["total_impressions"] == 3
    assert u1["page_types_visited"] == 2
    assert u1["avg_funnel_depth"] == 4.33
    assert u1["max_funnel_depth"] == 6
    assert u1["total_events"] == 13
    assert u1["most_engaged_page_type"] == 1
    assert u1["impressions_on_top_page"] == 2

    u2 = by_user[("u2",)]
    assert u2["total_impressions"] == 3
    assert u2["page_types_visited"] == 2
    assert u2["max_funnel_depth"] == 5
    assert u2["total_events"] == 13
    assert u2["most_engaged_page_type"] == 2
    assert u2["impressions_on_top_page"] == 2

    u3 = by_user[("u3",)]
    assert u3["total_impressions"] == 2
    assert u3["page_types_visited"] == 2
    assert u3["avg_funnel_depth"] == 4.0
    assert u3["max_funnel_depth"] == 6
    assert u3["total_events"] == 8
    # tie between pt2 and pt3 (1 impression each) broken to lower page_type
    assert u3["most_engaged_page_type"] == 2
    assert u3["impressions_on_top_page"] == 1


def test_hourly_traffic(conn):
    rows = queries.hourly_traffic(conn)
    by_pt = _index(rows, "page_type")

    # Single date/hour -> one row per page_type
    assert len(rows) == 3
    for row in rows:
        assert row["event_date"] == "2026-01-01"
        assert row["hour"] == 10

    assert by_pt[(1,)]["total_impressions"] == 2
    assert by_pt[(1,)]["unique_users"] == 1
    assert by_pt[(1,)]["avg_funnel_depth"] == 3.5
    assert by_pt[(1,)]["impressions_reaching_d"] == 1
    assert by_pt[(1,)]["pct_reaching_d"] == 50

    assert by_pt[(2,)]["total_impressions"] == 3
    assert by_pt[(2,)]["unique_users"] == 2
    assert by_pt[(2,)]["avg_funnel_depth"] == 3.67
    assert by_pt[(2,)]["pct_reaching_d"] == 66.67

    assert by_pt[(3,)]["total_impressions"] == 3
    assert by_pt[(3,)]["unique_users"] == 3
    assert by_pt[(3,)]["avg_funnel_depth"] == 5.33
    assert by_pt[(3,)]["impressions_reaching_d"] == 3
    assert by_pt[(3,)]["pct_reaching_d"] == 100
