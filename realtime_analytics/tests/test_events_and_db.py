"""Event generation invariants and the query-parameter layer."""
import random
from datetime import datetime

import pytest

from realtime_analytics.db import render_query, split_statements
from realtime_analytics.events import (
    COLUMNS,
    EVENT_TYPES,
    generate_conversation,
    generate_events,
)


def test_conversation_starts_and_ends_correctly():
    rng = random.Random(1)
    events = generate_conversation(rng, "acct_0001", datetime(2026, 6, 1, 9))
    assert events[0].event_type == "conversation_started"
    assert events[-1].event_type in ("resolution", "escalation")
    terminal = [e for e in events
                if e.event_type in ("resolution", "escalation")]
    assert len(terminal) == 1, "exactly one terminal event per conversation"


def test_conversation_events_are_time_ordered():
    rng = random.Random(2)
    events = generate_conversation(rng, "acct_0001", datetime(2026, 6, 1, 9))
    timestamps = [e.event_ts for e in events]
    assert timestamps == sorted(timestamps)


def test_all_events_share_conversation_identity():
    rng = random.Random(3)
    events = generate_conversation(rng, "acct_0002", datetime(2026, 6, 1, 9))
    assert len({e.conversation_id for e in events}) == 1
    assert len({e.account_id for e in events}) == 1
    assert len({e.channel for e in events}) == 1


def test_only_agent_responses_carry_latency():
    rng = random.Random(4)
    events = generate_conversation(rng, "acct_0003", datetime(2026, 6, 1, 9))
    for event in events:
        if event.event_type == "agent_response":
            assert event.latency_ms > 0
            assert event.prompt_tokens > 0
        else:
            assert event.latency_ms == 0


def test_ingest_is_never_before_event_time():
    """Negative lag would make watermark reasoning meaningless."""
    rng = random.Random(5)
    for event in generate_conversation(rng, "a", datetime(2026, 6, 1, 9)):
        assert event.ingest_ts >= event.event_ts


def test_escalated_conversations_have_a_reason():
    rng = random.Random(6)
    for _ in range(40):
        for event in generate_conversation(rng, "a", datetime(2026, 6, 1, 9)):
            if event.event_type == "escalation":
                assert event.escalation_reason
            if event.event_type == "resolution":
                assert event.resolved == 1


def test_generate_events_respects_count_and_types():
    events = list(generate_events(500, seed=8, accounts=4))
    assert len(events) == 500
    assert {e.event_type for e in events} <= set(EVENT_TYPES)
    assert len({e.account_id for e in events}) <= 4


def test_row_ordering_matches_columns():
    rng = random.Random(9)
    event = generate_conversation(rng, "a", datetime(2026, 6, 1, 9))[0]
    row = event.to_row()
    assert len(row) == len(COLUMNS)
    assert row[COLUMNS.index("account_id")] == "a"


def test_render_query_escapes_strings():
    sql = render_query("SELECT {a:String}", {"a": "O'Brien"})
    assert sql == "SELECT 'O\\'Brien'"


def test_render_query_rejects_non_numeric_for_int():
    with pytest.raises(ValueError):
        render_query("SELECT {a:UInt32}", {"a": "1; DROP TABLE t"})


def test_render_query_rejects_bad_uuid():
    with pytest.raises(ValueError):
        render_query("SELECT {a:UUID}", {"a": "not-a-uuid'; --"})


def test_render_query_requires_every_parameter():
    with pytest.raises(KeyError):
        render_query("SELECT {a:String}", {})


def test_split_statements_drops_comments():
    script = """
-- a comment with a ; semicolon in it
CREATE TABLE a (x UInt8) ENGINE = Memory;
-- another
CREATE TABLE b (y UInt8) ENGINE = Memory;
"""
    statements = split_statements(script)
    assert len(statements) == 2
    assert all(s.startswith("CREATE TABLE") for s in statements)
