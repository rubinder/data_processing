"""Assertions on the four analyses against the fixture dataset, on both
execution engines. Expected values are derived by hand in conftest.py and
match duckdb_deployment/tests/test_queries.py exactly.
"""
import datetime as dt

import polars as pl
import pytest

from polars_deployment import analyses
from polars_deployment.analyses import ENGINES


def _index(rows, *keys):
    """Index a list of dicts by a tuple of key columns."""
    return {tuple(row[k] for k in keys): row for row in rows}


def _rows(name, events, engine):
    return analyses.run(name, events, engine=engine).to_dicts()


@pytest.mark.parametrize("engine", ENGINES)
def test_funnel_analysis(events, engine):
    rows = _rows("funnel", events, engine)
    by_stage = _index(rows, "page_type", "event_type")

    expected_cols = [
        "page_type", "event_type", "impressions_at_stage",
        "total_impressions", "pct_of_total", "pct_from_previous_stage",
    ]
    assert list(rows[0].keys()) == expected_cols

    # page_type 1 (total 2)
    assert by_stage[(1, "a")]["impressions_at_stage"] == 2
    assert by_stage[(1, "d")]["impressions_at_stage"] == 1
    assert by_stage[(1, "d")]["pct_of_total"] == 50
    assert by_stage[(1, "a")]["pct_from_previous_stage"] is None
    assert by_stage[(1, "d")]["pct_from_previous_stage"] == 50
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

    # Sorted by page_type, event_type
    assert [(r["page_type"], r["event_type"]) for r in rows] == sorted(
        (r["page_type"], r["event_type"]) for r in rows
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_page_type_summary(events, engine):
    rows = _rows("page-type-summary", events, engine)
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
    assert pt1["impressions_reaching_f"] == 0
    assert pt1["pct_reaching_d"] == 50
    assert pt1["pct_reaching_e"] == 0

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
    assert pt3["impressions_reaching_f"] == 2
    assert pt3["pct_reaching_d"] == 100
    assert pt3["pct_reaching_e"] == 66.67
    assert pt3["pct_reaching_f"] == 66.67


@pytest.mark.parametrize("engine", ENGINES)
def test_user_engagement(events, engine):
    rows = _rows("user-engagement", events, engine)
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
    assert u1["first_seen"] == dt.datetime(2026, 1, 1, 10, 0, 1)
    assert u1["last_seen"] == dt.datetime(2026, 1, 1, 10, 0, 6)

    u2 = by_user[("u2",)]
    assert u2["total_impressions"] == 3
    assert u2["max_funnel_depth"] == 5
    assert u2["total_events"] == 13
    assert u2["most_engaged_page_type"] == 2
    assert u2["impressions_on_top_page"] == 2

    # u3 has one impression on each of page 2 and 3: tie broken to the
    # lower page_type, as in the SQL versions.
    u3 = by_user[("u3",)]
    assert u3["total_impressions"] == 2
    assert u3["avg_funnel_depth"] == 4.0
    assert u3["total_events"] == 8
    assert u3["most_engaged_page_type"] == 2
    assert u3["impressions_on_top_page"] == 1

    # Ordered by total_impressions desc, then user_id
    assert [r["user_id"] for r in rows] == ["u1", "u2", "u3"]


@pytest.mark.parametrize("engine", ENGINES)
def test_hourly_traffic(events, engine):
    rows = _rows("hourly-traffic", events, engine)
    by_key = _index(rows, "event_date", "hour", "page_type")
    assert len(rows) == 3
    day = dt.date(2026, 1, 1)

    assert by_key[(day, 10, 1)]["total_impressions"] == 2
    assert by_key[(day, 10, 1)]["unique_users"] == 1
    assert by_key[(day, 10, 1)]["avg_funnel_depth"] == 3.5
    assert by_key[(day, 10, 1)]["impressions_reaching_d"] == 1
    assert by_key[(day, 10, 1)]["pct_reaching_d"] == 50

    assert by_key[(day, 10, 2)]["avg_funnel_depth"] == 3.67
    assert by_key[(day, 10, 2)]["pct_reaching_d"] == 66.67

    assert by_key[(day, 10, 3)]["unique_users"] == 3
    assert by_key[(day, 10, 3)]["impressions_reaching_d"] == 3
    assert by_key[(day, 10, 3)]["pct_reaching_d"] == 100


@pytest.mark.parametrize("name", sorted(analyses.ANALYSES))
def test_engines_agree(events, name):
    """The streaming and in-memory engines return identical frames."""
    in_memory = analyses.run(name, events, engine="in-memory")
    streaming = analyses.run(name, events, engine="streaming")
    assert in_memory.schema == streaming.schema
    assert in_memory.equals(streaming)


def test_run_rejects_unknown_engine(events):
    with pytest.raises(ValueError, match="engine must be one of"):
        analyses.run("funnel", events, engine="gpu")


def test_analyses_are_lazy(events):
    """Building an analysis returns a plan, not data."""
    for build in analyses.ANALYSES.values():
        assert isinstance(build(events), pl.LazyFrame)
