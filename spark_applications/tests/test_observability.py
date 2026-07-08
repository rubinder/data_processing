"""Tests for structured logging / metrics helpers (issue #6)."""

import json

from spark_applications.utils.observability import event_payload, get_logger


def test_event_payload_is_single_line_json():
    payload = event_payload("metrics", job="j1", rows=10, ok=True)
    parsed = json.loads(payload)

    assert parsed == {"event": "metrics", "job": "j1", "rows": 10, "ok": True}
    assert "\n" not in payload


def test_event_payload_serializes_non_json_types():
    # default=str must keep it from blowing up on odd types.
    payload = event_payload("e", value={1, 2})
    assert json.loads(payload)["event"] == "e"


def test_get_logger_does_not_duplicate_handlers():
    a = get_logger("dup_check")
    b = get_logger("dup_check")
    assert a is b
    assert len(a.handlers) == 1
