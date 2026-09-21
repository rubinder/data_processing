"""Loader: gzip CSV parsing without a row loop, and idempotent loads."""
import csv
import datetime as dt
import gzip
import io

import polars as pl
import pytest

from polars_deployment import loader
from polars_deployment.schema import COLUMNS, SCHEMA
from polars_deployment.store import ParquetStore

HEADER = [
    "user_id", "impression_id", "page_type",
    "date", "hour", "min", "second", "event_type",
]


def _csv_gz(rows: list[list]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADER)
    writer.writerows(rows)
    return gzip.compress(buf.getvalue().encode("utf-8"))


ROWS = [
    ["u1", "imp1", 1, "2026-01-01", 10, 5, 1, "a"],
    ["u1", "imp1", 1, "2026-01-01", 10, 5, 2, "b"],
    ["u2", "imp2", 1, "2026-01-01", 10, 7, 30, "a"],
]


class _FakeResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.calls = []

    def raise_for_status(self):
        return None


def test_parse_csv_gz_types():
    df = loader.parse_csv_gz(_csv_gz(ROWS))
    assert df.columns == COLUMNS
    assert dict(df.schema) == SCHEMA
    assert df.height == 3
    assert df["date"].dtype == pl.Date
    assert df["second"].to_list() == [1, 2, 30]


def test_load_impressions_writes_partition(tmp_path, monkeypatch):
    seen = {}

    def fake_get(url, params=None):
        seen["url"] = url
        seen["params"] = params
        return _FakeResponse(_csv_gz(ROWS))

    monkeypatch.setattr(loader.requests, "get", fake_get)
    store = ParquetStore(tmp_path / "impressions")

    n = loader.load_impressions(store, "http://api:8000", 1, "2026-01-01", 10)

    assert n == 3
    assert seen["url"] == "http://api:8000/impression"
    assert seen["params"] == {"page_type": 1, "date": "2026-01-01", "hour": 10}
    assert store.partitions() == [(1, dt.date(2026, 1, 1), 10)]
    scanned = store.scan().collect()
    assert scanned.height == 3
    assert scanned["page_type"].to_list() == [1, 1, 1]
    assert scanned["hour"].to_list() == [10, 10, 10]


def test_load_is_idempotent(tmp_path, monkeypatch):
    payloads = iter([_csv_gz(ROWS), _csv_gz(ROWS[:1])])
    monkeypatch.setattr(
        loader.requests, "get", lambda url, params=None: _FakeResponse(next(payloads))
    )
    store = ParquetStore(tmp_path / "impressions")

    loader.load_impressions(store, "http://api:8000", 1, "2026-01-01", 10)
    loader.load_impressions(store, "http://api:8000", 1, "2026-01-01", 10)

    assert store.scan().collect().height == 1
    assert len(store.files()) == 1


def test_http_error_propagates(tmp_path, monkeypatch):
    class _Failing(_FakeResponse):
        def raise_for_status(self):
            raise RuntimeError("503")

    monkeypatch.setattr(
        loader.requests, "get", lambda url, params=None: _Failing(b"")
    )
    store = ParquetStore(tmp_path / "impressions")
    with pytest.raises(RuntimeError, match="503"):
        loader.load_impressions(store, "http://api:8000", 1, "2026-01-01", 10)
    assert store.files() == []
