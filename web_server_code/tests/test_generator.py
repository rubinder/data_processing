"""Tests for impression data generator."""

import csv
import gzip
import io
from collections import Counter

from web_server_code.generator import (
    EVENT_TYPES,
    FUNNEL_RATES,
    HEADER,
    _determine_max_event,
    _make_impression_id,
    generate_gzip_csv,
    generate_rows,
)


def test_determine_max_event_always_at_least_a():
    """Every impression reaches at least event 'a'."""
    funnel = FUNNEL_RATES[1]
    for _ in range(100):
        assert _determine_max_event(funnel) >= 0


def test_impression_id_deterministic():
    """Same inputs produce the same impression_id."""
    id1 = _make_impression_id("user1", 1, "2026-01-01", 10, 30)
    id2 = _make_impression_id("user1", 1, "2026-01-01", 10, 30)
    assert id1 == id2


def test_impression_id_varies_with_inputs():
    """Different inputs produce different impression_ids."""
    id1 = _make_impression_id("user1", 1, "2026-01-01", 10, 30)
    id2 = _make_impression_id("user2", 1, "2026-01-01", 10, 30)
    assert id1 != id2


def test_generate_rows_header_fields():
    """Generated rows have the correct number of fields."""
    rows = generate_rows(1, "2026-01-01", 10, num_impressions=100, num_users=50)
    for row in rows:
        assert len(row) == len(HEADER)


def test_generate_rows_min_users():
    """At least 1000 distinct user_ids when using default num_users."""
    rows = generate_rows(
        1, "2026-01-01", 10, num_impressions=5000, num_users=1200
    )
    user_ids = {row[0] for row in rows}
    assert len(user_ids) >= 1000


def test_event_sequentiality():
    """For each impression, events must be sequential from 'a'."""
    rows = generate_rows(1, "2026-01-01", 10, num_impressions=500, num_users=100)

    # Group rows by impression_id
    by_impression: dict[str, list] = {}
    for row in rows:
        imp_id = row[1]
        by_impression.setdefault(imp_id, []).append(row)

    for imp_id, imp_rows in by_impression.items():
        events = sorted(imp_rows, key=lambda r: r[6])  # sort by second
        event_types = [r[7] for r in events]
        # Events should be a prefix of EVENT_TYPES
        expected = EVENT_TYPES[:len(event_types)]
        assert event_types == expected, (
            f"impression {imp_id}: got {event_types}, expected {expected}"
        )


def test_seconds_are_increasing():
    """Within an impression, event seconds must be strictly increasing."""
    rows = generate_rows(1, "2026-01-01", 10, num_impressions=500, num_users=100)

    by_impression: dict[str, list] = {}
    for row in rows:
        imp_id = row[1]
        by_impression.setdefault(imp_id, []).append(row)

    for imp_id, imp_rows in by_impression.items():
        seconds = [r[6] for r in sorted(imp_rows, key=lambda r: r[6])]
        for i in range(1, len(seconds)):
            assert seconds[i] > seconds[i - 1]


def test_page_type_1_funnel_distribution():
    """page_type 1: ~10% reach d, 0% reach e, 0% reach f."""
    rows = generate_rows(1, "2026-01-01", 10, num_impressions=10000, num_users=1200)

    # Count unique impressions reaching each event
    imp_events: dict[str, set] = {}
    for row in rows:
        imp_id = row[1]
        imp_events.setdefault(imp_id, set()).add(row[7])

    total = len(imp_events)
    reached_d = sum(1 for evts in imp_events.values() if "d" in evts)
    reached_e = sum(1 for evts in imp_events.values() if "e" in evts)
    reached_f = sum(1 for evts in imp_events.values() if "f" in evts)

    # Allow +-5% tolerance
    assert 0.05 <= reached_d / total <= 0.15
    assert reached_e == 0
    assert reached_f == 0


def test_page_type_2_funnel_distribution():
    """page_type 2: ~30% reach d, ~10% reach e, 0% reach f."""
    rows = generate_rows(2, "2026-01-01", 10, num_impressions=10000, num_users=1200)

    imp_events: dict[str, set] = {}
    for row in rows:
        imp_id = row[1]
        imp_events.setdefault(imp_id, set()).add(row[7])

    total = len(imp_events)
    reached_d = sum(1 for evts in imp_events.values() if "d" in evts)
    reached_e = sum(1 for evts in imp_events.values() if "e" in evts)
    reached_f = sum(1 for evts in imp_events.values() if "f" in evts)

    assert 0.25 <= reached_d / total <= 0.35
    assert 0.05 <= reached_e / total <= 0.15
    assert reached_f == 0


def test_page_type_3_funnel_distribution():
    """page_type 3: ~50% reach d, ~20% reach e, ~10% reach f."""
    rows = generate_rows(3, "2026-01-01", 10, num_impressions=10000, num_users=1200)

    imp_events: dict[str, set] = {}
    for row in rows:
        imp_id = row[1]
        imp_events.setdefault(imp_id, set()).add(row[7])

    total = len(imp_events)
    reached_d = sum(1 for evts in imp_events.values() if "d" in evts)
    reached_e = sum(1 for evts in imp_events.values() if "e" in evts)
    reached_f = sum(1 for evts in imp_events.values() if "f" in evts)

    assert 0.45 <= reached_d / total <= 0.55
    assert 0.15 <= reached_e / total <= 0.25
    assert 0.05 <= reached_f / total <= 0.15


def test_generate_gzip_csv_decompresses():
    """Generated gzip CSV decompresses to valid CSV with correct header."""
    gz_bytes = generate_gzip_csv(1, "2026-01-01", 10, num_impressions=100, num_users=50)

    csv_text = gzip.decompress(gz_bytes).decode("utf-8")
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    assert header == HEADER

    rows = list(reader)
    assert len(rows) > 0
