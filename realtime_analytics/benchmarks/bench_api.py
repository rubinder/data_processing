"""End-to-end API latency: does the service actually hold p95 < 100ms?

The ClickHouse benchmark measures queries. This measures what a dashboard
actually experiences -- HTTP handling, parameter validation, JSON
serialization, and the query -- because the SLO is on the endpoint, not on
the SELECT.

Two modes:

``embedded`` (default) builds a real ClickHouse in-process with ``chdb``,
loads generated events through the production schema and its materialized
views, and drives the FastAPI app with its TestClient. No Docker required,
which means the SLO check runs in CI.

``http`` points at an already-running service, which is the mode that also
measures the network and the container.

Usage::

    python benchmarks/bench_api.py --requests 300
    python benchmarks/bench_api.py --mode http --base-url http://localhost:8500
"""
import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from realtime_analytics.db import split_statements  # noqa: E402
from realtime_analytics.events import COLUMNS, generate_events  # noqa: E402

SQL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clickhouse"
)
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

#: The service-level objective this benchmark exists to verify.
P95_SLO_MS = 100.0


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def _escape(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def build_embedded(events: int, seed: int = 21):
    """Real embedded ClickHouse holding the production schema and events."""
    import tempfile

    from realtime_analytics.db import ChdbBackend

    backend = ChdbBackend(tempfile.mkdtemp(prefix="bench-api-"))
    with open(os.path.join(SQL_DIR, "10_production.sql")) as handle:
        for statement in split_statements(handle.read()):
            backend.command(statement)

    log(f"Loading {events:,} events through the materialized views")
    generated = list(
        generate_events(
            events,
            seed=seed,
            accounts=40,
            start_ts=datetime.now() - timedelta(days=25),
            window_hours=25 * 24,
        )
    )
    columns = ", ".join(COLUMNS)
    chunk = 20_000
    for start in range(0, len(generated), chunk):
        rows = ", ".join(
            "(" + ", ".join(_escape(v) for v in e.to_row()) + ")"
            for e in generated[start:start + chunk]
        )
        backend.command(
            f"INSERT INTO conversation_events ({columns}) VALUES {rows}"
        )
    for table in ("conversation_events", "conversation_daily",
                  "agent_hourly", "platform_daily"):
        backend.command(f"OPTIMIZE TABLE {table} FINAL")
    return backend


def embedded_client(backend):
    from fastapi.testclient import TestClient

    from realtime_analytics import api as api_module

    class Proxy:
        name = "chdb"

        def query(self, sql, params=None):
            return backend.query(sql, params)

        def command(self, sql, params=None):
            return backend.command(sql, params)

        def close(self):
            """No-op: the benchmark owns the session."""

    api_module.get_backend = lambda *a, **kw: Proxy()
    return TestClient(api_module.app)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(int(len(ordered) * fraction), len(ordered) - 1)
    return ordered[index]


def measure(get, path: str, count: int, warmup: int) -> dict:
    for _ in range(warmup):
        get(path)
    timings, statuses = [], set()
    for _ in range(count):
        started = time.perf_counter()
        response = get(path)
        timings.append((time.perf_counter() - started) * 1000)
        statuses.add(response.status_code)
    return {
        "path": path,
        "requests": count,
        "status_codes": sorted(statuses),
        "p50_ms": round(statistics.median(timings), 2),
        "p95_ms": round(percentile(timings, 0.95), 2),
        "p99_ms": round(percentile(timings, 0.99), 2),
        "max_ms": round(max(timings), 2),
        "mean_ms": round(statistics.fmean(timings), 2),
    }


def run(args) -> dict:
    if args.mode == "embedded":
        backend = build_embedded(args.events)
        client = embedded_client(backend)

        def get(path):
            return client.get(path)

        account = backend.query(
            "SELECT account_id, count() AS c FROM conversation_events "
            "GROUP BY account_id ORDER BY c DESC LIMIT 1"
        ).rows[0]["account_id"]
        conversation = backend.query(
            "SELECT toString(conversation_id) AS cid FROM conversation_events "
            "LIMIT 1"
        ).rows[0]["cid"]
    else:
        import requests

        session = requests.Session()

        def get(path):
            return session.get(args.base_url + path, timeout=30)

        account = args.account_id
        conversation = args.conversation_id
        if not account:
            raise SystemExit("--account-id is required in http mode")
        backend = None

    endpoints = [
        f"/v1/accounts/{account}/summary?days=7",
        f"/v1/accounts/{account}/summary?days=30",
        f"/v1/accounts/{account}/agent-latency?hours=168",
        f"/v1/accounts/{account}/hourly?hours=168",
        f"/v1/accounts/{account}/intents?days=7",
    ]
    if conversation:
        endpoints.append(f"/v1/conversations/{conversation}")

    results = []
    for path in endpoints:
        log(f"Measuring {path}")
        results.append(measure(get, path, args.requests, args.warmup))

    if backend is not None:
        backend.close()

    worst_p95 = max(r["p95_ms"] for r in results)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": args.mode,
        "events_loaded": args.events if args.mode == "embedded" else None,
        "requests_per_endpoint": args.requests,
        "slo_p95_ms": P95_SLO_MS,
        "worst_p95_ms": worst_p95,
        "slo_met": worst_p95 < P95_SLO_MS,
        "endpoints": results,
    }


def format_markdown(results: dict) -> str:
    verdict = "PASS" if results["slo_met"] else "FAIL"
    lines = [
        "# API latency",
        "",
        f"- generated: `{results['generated_at']}`",
        f"- mode: `{results['mode']}`",
        f"- requests per endpoint: `{results['requests_per_endpoint']}`",
        f"- SLO: p95 < {results['slo_p95_ms']}ms -> **{verdict}** "
        f"(worst p95 {results['worst_p95_ms']}ms)",
        "",
        "| endpoint | p50 ms | p95 ms | p99 ms | max ms |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in results["endpoints"]:
        lines.append(
            f"| `{row['path']}` | {row['p50_ms']} | {row['p95_ms']} | "
            f"{row['p99_ms']} | {row['max_ms']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["embedded", "http"], default="embedded")
    parser.add_argument("--base-url", default="http://localhost:8500")
    parser.add_argument("--events", type=int, default=400_000,
                        help="events to load in embedded mode")
    parser.add_argument("--requests", type=int, default=200,
                        help="timed requests per endpoint")
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--account-id", default=None, help="http mode only")
    parser.add_argument("--conversation-id", default=None, help="http mode only")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    results = run(args)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    prefix = args.out or os.path.join(RESULTS_DIR, "api_latency")
    with open(f"{prefix}.json", "w") as handle:
        json.dump(results, handle, indent=2, default=str)
    markdown = format_markdown(results)
    with open(f"{prefix}.md", "w") as handle:
        handle.write(markdown)

    print()
    print(markdown)
    if not results["slo_met"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
