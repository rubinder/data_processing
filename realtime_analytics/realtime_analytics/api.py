"""FastAPI service serving conversation analytics out of ClickHouse.

Design rules, which are the reason the p95 target is reachable:

* **Every dashboard endpoint reads a materialized view, never the raw events
  table.** The raw table is reserved for the drill-down and ad-hoc endpoints,
  where the row count returned is inherently small.
* **Bounded windows.** Every endpoint caps its lookback. An unbounded range
  parameter is how a fast analytics API becomes a slow one in production.
* **Parameter binding.** All user input reaches ClickHouse as a bound
  ``{name:Type}`` parameter, never string-formatted into SQL. Path parameters
  are additionally shape-validated so a bad account id fails at the edge.
* **Self-instrumenting.** Every response carries the server-side query time,
  and ``/metrics`` exposes per-endpoint latency percentiles in Prometheus
  text format. An API that claims a p95 target should be able to report on
  it without an external load generator.

Run with::

    uvicorn realtime_analytics.api:app --host 0.0.0.0 --port 8500
"""
import os
import re
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

from fastapi import FastAPI, HTTPException, Path, Query, Request, Response

from realtime_analytics import queries
from realtime_analytics.db import get_backend

ACCOUNT_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
UUID_RE = re.compile(r"^[0-9a-fA-F-]{32,36}$")

EVENTS_TABLE = os.getenv("EVENTS_TABLE", "conversation_events")
MAX_DAYS = int(os.getenv("MAX_LOOKBACK_DAYS", "90"))
MAX_HOURS = int(os.getenv("MAX_LOOKBACK_HOURS", "720"))

#: Rolling window of recent request latencies per endpoint, for /metrics.
_LATENCIES: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
_COUNTS: dict[tuple[str, int], int] = defaultdict(int)


class State:
    backend = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    State.backend = get_backend()
    yield
    if State.backend:
        State.backend.close()


app = FastAPI(
    title="Conversation Analytics API",
    description=(
        "Sub-100ms analytics over AI agent conversation events, served from "
        "ClickHouse materialized views."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def track_latency(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started) * 1000
    route = request.scope.get("route")
    endpoint = getattr(route, "path", request.url.path)
    _LATENCIES[endpoint].append(elapsed_ms)
    _COUNTS[(endpoint, response.status_code)] += 1
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.2f}"
    return response


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(int(len(ordered) * fraction), len(ordered) - 1)
    return ordered[index]


def _validate_account(account_id: str) -> str:
    if not ACCOUNT_RE.match(account_id):
        raise HTTPException(status_code=400, detail="invalid account_id")
    return account_id


def _run(sql: str, params: dict) -> tuple[list[dict], float]:
    if State.backend is None:  # pragma: no cover - lifespan always sets it
        raise HTTPException(status_code=503, detail="backend unavailable")
    try:
        result = State.backend.query(sql, params)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"query failed: {exc}") from exc
    return result.rows, round(result.wall_s * 1000, 2)


def _day_window(days: int) -> tuple[date, date]:
    """Inclusive-exclusive day window ending tomorrow, so today is included."""
    end = date.today() + timedelta(days=1)
    return end - timedelta(days=days + 1), end


def _hour_window(hours: int) -> tuple[str, str]:
    end = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    start = end - timedelta(hours=hours)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)


@app.get("/health")
def health() -> dict:
    """Liveness plus a trivial round-trip to ClickHouse."""
    try:
        rows, elapsed_ms = _run("SELECT 1 AS ok", {})
        return {
            "status": "ok",
            "clickhouse": "reachable",
            "backend": State.backend.name,
            "query_ms": elapsed_ms,
            "rows": rows,
        }
    except HTTPException as exc:
        return {"status": "degraded", "clickhouse": "unreachable",
                "detail": exc.detail}


@app.get("/v1/accounts/{account_id}/summary")
def account_summary(
    account_id: str = Path(..., description="tenant identifier"),
    days: int = Query(7, ge=1, le=MAX_DAYS),
) -> dict:
    """Daily conversation volume, resolution/escalation rates, latency p50/p95/p99.

    Served from ``conversation_daily``: one row per (account, day), so the
    response size and the work done are both proportional to ``days``, not to
    the number of events.
    """
    _validate_account(account_id)
    start, end = _day_window(days)
    rows, elapsed_ms = _run(
        queries.API_CONVERSATION_SUMMARY,
        {"account_id": account_id, "start": start.isoformat(),
         "end": end.isoformat()},
    )
    return {
        "account_id": account_id,
        "days": days,
        "query_ms": elapsed_ms,
        "source": "materialized_view:conversation_daily",
        "results": rows,
    }


@app.get("/v1/accounts/{account_id}/agent-latency")
def agent_latency(
    account_id: str = Path(...),
    hours: int = Query(24, ge=1, le=MAX_HOURS),
) -> dict:
    """Agent response latency percentiles broken down by model and channel.

    This is the SLO view: ``slow_rate`` is the fraction of responses over the
    5s threshold, pre-computed by the view so the threshold costs nothing at
    read time.
    """
    _validate_account(account_id)
    start, end = _hour_window(hours)
    rows, elapsed_ms = _run(
        queries.API_AGENT_LATENCY,
        {"account_id": account_id, "start": start, "end": end},
    )
    return {
        "account_id": account_id,
        "hours": hours,
        "query_ms": elapsed_ms,
        "source": "materialized_view:agent_hourly",
        "results": rows,
    }


@app.get("/v1/accounts/{account_id}/hourly")
def hourly_volume(
    account_id: str = Path(...),
    hours: int = Query(24, ge=1, le=MAX_HOURS),
) -> dict:
    """Hourly agent response volume and p95 latency -- the time-series chart."""
    _validate_account(account_id)
    start, end = _hour_window(hours)
    rows, elapsed_ms = _run(
        queries.API_HOURLY_VOLUME,
        {"account_id": account_id, "start": start, "end": end},
    )
    return {
        "account_id": account_id,
        "hours": hours,
        "query_ms": elapsed_ms,
        "source": "materialized_view:agent_hourly",
        "results": rows,
    }


@app.get("/v1/accounts/{account_id}/intents")
def top_intents(
    account_id: str = Path(...),
    days: int = Query(7, ge=1, le=MAX_DAYS),
) -> dict:
    """Top intents by volume, with escalation counts.

    Deliberately reads the raw events table: intent is not in any rollup, so
    this is the honest cost of an un-pre-aggregated dimension. It stays fast
    because the sorting key prunes to one tenant and one time range -- which
    is exactly the v2 optimization from the tuning writeup.
    """
    _validate_account(account_id)
    start, end = _hour_window(days * 24)
    rows, elapsed_ms = _run(
        queries.API_TOP_INTENTS.replace("{events_table}", EVENTS_TABLE),
        {"account_id": account_id, "start": start, "end": end},
    )
    return {
        "account_id": account_id,
        "days": days,
        "query_ms": elapsed_ms,
        "source": f"raw:{EVENTS_TABLE}",
        "results": rows,
    }


@app.get("/v1/conversations/{conversation_id}")
def conversation_detail(conversation_id: str = Path(...)) -> dict:
    """Every event on one conversation, in order -- the support drill-down.

    Backed by the bloom filter skipping index on ``conversation_id``; without
    it this is a full scan.
    """
    if not UUID_RE.match(conversation_id):
        raise HTTPException(status_code=400, detail="invalid conversation_id")
    rows, elapsed_ms = _run(
        queries.API_CONVERSATION_DETAIL.replace("{events_table}", EVENTS_TABLE),
        {"conversation_id": conversation_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="conversation not found")
    return {
        "conversation_id": conversation_id,
        "query_ms": elapsed_ms,
        "events": rows,
    }


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus text exposition of per-endpoint request latency.

    Percentiles are computed over a rolling in-process window. That is fine
    for a single replica and a demo; a real deployment exports a histogram and
    lets Prometheus do the aggregation across replicas, because percentiles
    cannot be averaged after the fact.
    """
    lines = [
        "# HELP api_request_latency_ms Request latency by endpoint",
        "# TYPE api_request_latency_ms summary",
    ]
    for endpoint, samples in sorted(_LATENCIES.items()):
        values = list(samples)
        if not values:
            continue
        for label, fraction in (("0.5", 0.5), ("0.95", 0.95), ("0.99", 0.99)):
            lines.append(
                f'api_request_latency_ms{{endpoint="{endpoint}",'
                f'quantile="{label}"}} {_percentile(values, fraction):.3f}'
            )
        lines.append(
            f'api_request_latency_ms_count{{endpoint="{endpoint}"}} {len(values)}'
        )
    lines += [
        "# HELP api_requests_total Requests by endpoint and status",
        "# TYPE api_requests_total counter",
    ]
    for (endpoint, status), count in sorted(_COUNTS.items()):
        lines.append(
            f'api_requests_total{{endpoint="{endpoint}",status="{status}"}} {count}'
        )
    return Response("\n".join(lines) + "\n", media_type="text/plain")
