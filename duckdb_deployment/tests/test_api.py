"""FastAPI TestClient tests wired to the in-memory fixture DuckDB."""
import gzip

import duckdb
import pytest
from fastapi.testclient import TestClient

from app import loader
from app.db import init_schema
from app.main import app, get_db


@pytest.fixture
def client(conn):
    """TestClient with the DuckDB dependency overridden to the fixture conn."""
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "endpoint",
    ["/funnel", "/page-type-summary", "/user-engagement", "/hourly-traffic"],
)
def test_analytics_endpoints(client, endpoint):
    resp = client.get(endpoint)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    assert len(body) > 0
    assert isinstance(body[0], dict)


def test_funnel_endpoint_values(client):
    resp = client.get("/funnel")
    assert resp.status_code == 200
    body = resp.json()
    pt3_f = next(
        r for r in body if r["page_type"] == 3 and r["event_type"] == "f"
    )
    assert pt3_f["impressions_at_stage"] == 2
    assert pt3_f["pct_of_total"] == 66.67


class _FakeResponse:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        return None


def test_load_endpoint(monkeypatch):
    # Fresh empty in-memory DB for the load test.
    connection = duckdb.connect(":memory:")
    init_schema(connection)

    csv_text = (
        "user_id,impression_id,page_type,date,hour,min,second,event_type\n"
        "u9,imp_9,1,2026-02-02,11,0,1,a\n"
        "u9,imp_9,1,2026-02-02,11,0,2,b\n"
        "u9,imp_9,1,2026-02-02,11,0,3,c\n"
    )
    gz = gzip.compress(csv_text.encode("utf-8"))

    def fake_get(url, params=None):
        assert url.endswith("/impression")
        assert params["page_type"] == 1
        return _FakeResponse(gz)

    monkeypatch.setattr(loader.requests, "get", fake_get)

    app.dependency_overrides[get_db] = lambda: connection
    try:
        with TestClient(app) as test_client:
            resp = test_client.post(
                "/load",
                params={"page_type": 1, "date": "2026-02-02", "hour": 11},
            )
        assert resp.status_code == 200
        assert resp.json()["rows_loaded"] == 3
        # Rows really landed in DuckDB.
        count = connection.execute(
            "SELECT count(*) FROM impressions"
        ).fetchone()[0]
        assert count == 3
    finally:
        app.dependency_overrides.clear()
        connection.close()
