"""FastAPI service exposing an embedded DuckDB OLAP engine.

DuckDB runs in-process inside this web service: a single module-level
connection is opened on startup and every request queries it directly. This
demonstrates DuckDB as an application-level analytical engine -- no separate
database server, no network hop.
"""
import os
from contextlib import asynccontextmanager

import duckdb
from fastapi import Depends, FastAPI, Query

from app import queries
from app.db import get_connection, init_schema
from app.loader import load_impressions

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "impressions.duckdb")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

# Module-level embedded connection, initialised on startup.
_conn: duckdb.DuckDBPyConnection | None = None


def get_db() -> duckdb.DuckDBPyConnection:
    """Return the module-level DuckDB connection (override in tests)."""
    global _conn
    if _conn is None:
        _conn = get_connection(DUCKDB_PATH)
        init_schema(_conn)
    return _conn


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Open the connection and ensure the schema exists on startup."""
    get_db()
    yield


app = FastAPI(title="DuckDB Impression Analytics", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/load")
def load(
    page_type: int = Query(..., ge=1, le=3),
    date: str = Query(...),
    hour: int = Query(..., ge=0, le=23),
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> dict:
    rows_loaded = load_impressions(conn, API_BASE_URL, page_type, date, hour)
    return {
        "page_type": page_type,
        "date": date,
        "hour": hour,
        "rows_loaded": rows_loaded,
    }


@app.get("/funnel")
def funnel(conn: duckdb.DuckDBPyConnection = Depends(get_db)) -> list[dict]:
    return queries.funnel_analysis(conn)


@app.get("/page-type-summary")
def page_type_summary(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[dict]:
    return queries.page_type_summary(conn)


@app.get("/user-engagement")
def user_engagement(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[dict]:
    return queries.user_engagement(conn)


@app.get("/hourly-traffic")
def hourly_traffic(
    conn: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[dict]:
    return queries.hourly_traffic(conn)
