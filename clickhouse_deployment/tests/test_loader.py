"""Tests for the pure CSV fetch/parse helpers in load_data.py.

These do not touch a live server or ClickHouse and must pass regardless of
whether chdb is installed.
"""
import gzip
import io
from unittest import mock

import load_data

HEADER = "user_id,impression_id,page_type,date,hour,min,second,event_type"


def _gzip_csv(lines):
    body = "\n".join([HEADER, *lines]) + "\n"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(body.encode("utf-8"))
    return buf.getvalue()


def test_parse_csv_gz_types_and_min_mapping():
    raw = _gzip_csv(
        [
            "u1,imp1,1,2026-01-01,10,5,30,a",
            "u1,imp1,1,2026-01-01,10,5,35,b",
        ]
    )

    rows = load_data.parse_csv_gz(raw)

    assert len(rows) == 2
    first = rows[0]
    assert first == {
        "user_id": "u1",
        "impression_id": "imp1",
        "page_type": 1,
        "date": "2026-01-01",
        "hour": 10,
        "minute": 5,  # CSV "min" mapped to "minute"
        "second": 30,
        "event_type": "a",
    }
    assert isinstance(first["page_type"], int)
    assert isinstance(first["hour"], int)
    assert isinstance(first["second"], int)
    assert rows[1]["event_type"] == "b"


def test_fetch_and_parse_calls_api_and_parses():
    raw = _gzip_csv(["u2,imp2,3,2026-02-02,23,0,0,a"])

    fake_response = mock.Mock()
    fake_response.content = raw
    fake_response.raise_for_status = mock.Mock()

    with mock.patch("load_data.requests.get", return_value=fake_response) as get:
        rows = load_data.fetch_and_parse(
            "http://example.test", page_type=3, date="2026-02-02", hour=23
        )

    get.assert_called_once_with(
        "http://example.test/impression",
        params={"page_type": 3, "date": "2026-02-02", "hour": 23},
    )
    fake_response.raise_for_status.assert_called_once()
    assert rows == [
        {
            "user_id": "u2",
            "impression_id": "imp2",
            "page_type": 3,
            "date": "2026-02-02",
            "hour": 23,
            "minute": 0,
            "second": 0,
            "event_type": "a",
        }
    ]
