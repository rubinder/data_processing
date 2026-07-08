"""Shared pytest fixtures: an in-memory DuckDB seeded with a deterministic
impression dataset whose expected analytics are computed by hand below.

DATASET (date 2026-01-01, hour 10, min 0; each event at second = its 1-based
stage index, so a=1, b=2, ...):

  page_type 1 (2 impressions):
    imp_1_1  user u1  a,b,c,d          (depth 4, max 'd', duration 3)
    imp_1_2  user u1  a,b,c            (depth 3, max 'c', duration 2)
  page_type 2 (3 impressions):
    imp_2_1  user u2  a,b,c,d,e        (depth 5, max 'e', duration 4)
    imp_2_2  user u2  a,b,c,d          (depth 4, max 'd', duration 3)
    imp_2_3  user u3  a,b              (depth 2, max 'b', duration 1)
  page_type 3 (3 impressions):
    imp_3_1  user u3  a,b,c,d,e,f      (depth 6, max 'f', duration 5)
    imp_3_2  user u1  a,b,c,d,e,f      (depth 6, max 'f', duration 5)
    imp_3_3  user u2  a,b,c,d          (depth 4, max 'd', duration 3)

Expected FUNNEL (distinct impressions reaching each stage):
  pt1 total=2: a=2 b=2 c=2 d=1
    pct_of_total: a=100 b=100 c=100 d=50
    pct_from_previous: a=None b=100 c=100 d=50
  pt2 total=3: a=3 b=3 c=2 d=2 e=1
    pct_of_total: a=100 b=100 c=66.67 d=66.67 e=33.33
    pct_from_previous: a=None b=100 c=66.67 d=100 e=50
  pt3 total=3: a=3 b=3 c=3 d=3 e=2 f=2
    pct_of_total: a=100 b=100 c=100 d=100 e=66.67 f=66.67
    pct_from_previous: a=None b=100 c=100 d=100 e=66.67 f=100

Expected PAGE_TYPE_SUMMARY:
  pt1: total=2 users=1 avg_depth=3.5 max_depth=4 avg_dur=2.5
       reaching d/e/f = 1/0/0  pct d/e/f = 50/0/0
  pt2: total=3 users=2 avg_depth=3.67 max_depth=5 avg_dur=2.67
       reaching d/e/f = 2/1/0  pct d/e/f = 66.67/33.33/0
  pt3: total=3 users=3 avg_depth=5.33 max_depth=6 avg_dur=4.33
       reaching d/e/f = 3/2/2  pct d/e/f = 100/66.67/66.67

Expected USER_ENGAGEMENT:
  u1: total_impr=3 pages_visited=2 avg_depth=4.33 max_depth=6
      total_events=13 most_engaged_page=1 impr_on_top=2
  u2: total_impr=3 pages_visited=2 avg_depth=4.33 max_depth=5
      total_events=13 most_engaged_page=2 impr_on_top=2
  u3: total_impr=2 pages_visited=2 avg_depth=4.0 max_depth=6
      total_events=8  most_engaged_page=2 impr_on_top=1 (tie broken to
      lower page_type)

Expected HOURLY_TRAFFIC (single date/hour, one row per page_type):
  pt1: total=2 users=1 avg_depth=3.5 reaching_d=1 pct_d=50
  pt2: total=3 users=2 avg_depth=3.67 reaching_d=2 pct_d=66.67
  pt3: total=3 users=3 avg_depth=5.33 reaching_d=3 pct_d=100
"""
import duckdb
import pytest

from app.db import init_schema

DATE = "2026-01-01"
HOUR = 10

# (user_id, impression_id, page_type, max_event_reached)
_IMPRESSIONS = [
    ("u1", "imp_1_1", 1, "d"),
    ("u1", "imp_1_2", 1, "c"),
    ("u2", "imp_2_1", 2, "e"),
    ("u2", "imp_2_2", 2, "d"),
    ("u3", "imp_2_3", 2, "b"),
    ("u3", "imp_3_1", 3, "f"),
    ("u1", "imp_3_2", 3, "f"),
    ("u2", "imp_3_3", 3, "d"),
]

_EVENTS = ["a", "b", "c", "d", "e", "f"]


def _build_rows() -> list[tuple]:
    rows = []
    for user_id, impression_id, page_type, max_event in _IMPRESSIONS:
        for idx, event in enumerate(_EVENTS, start=1):
            rows.append(
                (
                    user_id,
                    impression_id,
                    page_type,
                    DATE,
                    HOUR,
                    0,
                    idx,  # second = 1-based stage index
                    event,
                )
            )
            if event == max_event:
                break
    return rows


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB seeded with the deterministic fixture dataset."""
    connection = duckdb.connect(":memory:")
    init_schema(connection)
    connection.executemany(
        "INSERT INTO impressions "
        "(user_id, impression_id, page_type, date, hour, min, second, "
        "event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        _build_rows(),
    )
    yield connection
    connection.close()
