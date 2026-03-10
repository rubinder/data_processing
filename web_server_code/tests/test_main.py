"""Tests for the FastAPI endpoint."""

import csv
import gzip
import io

from fastapi.testclient import TestClient

from web_server_code.generator import HEADER
from web_server_code.main import app

client = TestClient(app)


def test_impression_endpoint_returns_gzip():
    response = client.get(
        "/impression", params={"page_type": 1, "date": "2026-01-01", "hour": 10}
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/gzip"


def test_impression_endpoint_valid_csv():
    response = client.get(
        "/impression", params={"page_type": 2, "date": "2026-01-01", "hour": 5}
    )
    csv_text = gzip.decompress(response.content).decode("utf-8")
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    assert header == HEADER
    rows = list(reader)
    assert len(rows) >= 10000


def test_impression_endpoint_invalid_page_type():
    response = client.get(
        "/impression", params={"page_type": 5, "date": "2026-01-01", "hour": 10}
    )
    assert response.status_code == 422


def test_impression_endpoint_missing_params():
    response = client.get("/impression")
    assert response.status_code == 422


def test_impression_endpoint_content_disposition():
    response = client.get(
        "/impression", params={"page_type": 3, "date": "2026-03-09", "hour": 14}
    )
    assert "impressions_pt3_2026-03-09_h14.csv.gz" in response.headers.get(
        "content-disposition", ""
    )
