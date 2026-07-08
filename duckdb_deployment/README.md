# DuckDB Deployment

Demonstrates **DuckDB as an embedded, application-level OLAP engine**. Instead
of a separate warehouse server (like the PostgreSQL used by `../dbt_deployment`),
an in-process DuckDB lives *inside* a small FastAPI service. The connection
object is the engine — there is no network hop between the app and the database,
and the analytics run in the same process that serves the HTTP requests.

The service ingests impression data from the web server API (the same
`GET /impression` gzip-CSV endpoint used elsewhere in the repo) and exposes the
four analytical models from `dbt_deployment` as JSON endpoints, re-implemented
in standard DuckDB SQL.

## The embedded pattern

- `app/db.py` — opens a file-backed (or `:memory:`) DuckDB connection and
  creates the `impressions` table.
- `app/loader.py` — pulls gzip-CSV from the web server API and loads rows into
  DuckDB (delete-then-insert per `page_type/date/hour` partition).
- `app/queries.py` — the four analytical queries as plain functions taking a
  DuckDB connection. The per-impression aggregation from
  `int_impressions_aggregated.sql` is inlined as a CTE.
- `app/main.py` — FastAPI app holding a single module-level connection,
  initialised on startup.

## Directory Structure

```
duckdb_deployment/
├── Dockerfile              # python:3.10-slim running uvicorn on :8100
├── docker-compose.yaml     # duckdb-app service on the shared network
├── deploy.sh               # up|down|restart|status|logs|load-data|query
├── pyproject.toml          # uv project (duckdb, fastapi, uvicorn, requests)
├── README.md
├── app/
│   ├── db.py               # connection + schema
│   ├── loader.py           # API pull -> DuckDB
│   ├── queries.py          # funnel / summary / engagement / hourly
│   └── main.py             # FastAPI app
└── tests/
    ├── conftest.py         # in-memory DuckDB + deterministic fixture
    ├── test_queries.py     # exact numeric assertions on each query
    └── test_api.py         # TestClient endpoint tests + mocked /load
```

## Quick Start (Docker)

```bash
# 1. Start the web server (in another terminal)
cd ../web_server_local && ./deploy.sh up

# 2. Build and start the DuckDB service
./deploy.sh up

# 3. Load impression data from the API into DuckDB
./deploy.sh load-data --page_type 1 --date 2026-01-01 --hour 10
./deploy.sh load-data --page_type 2 --date 2026-01-01 --hour 10
./deploy.sh load-data --page_type 3 --date 2026-01-01 --hour 10

# 4. Query the analytics endpoints
./deploy.sh query funnel
./deploy.sh query page-type-summary
./deploy.sh query user-engagement
./deploy.sh query hourly-traffic
```

## Running locally without Docker

```bash
uv venv
uv pip install -e ".[dev]"
uv run uvicorn app.main:app --port 8100
# then, in another shell:
curl -X POST "http://localhost:8100/load?page_type=1&date=2026-01-01&hour=10"
curl http://localhost:8100/funnel
```

## Commands

| Command | Description |
|---------|-------------|
| `./deploy.sh up` | Build and start the DuckDB FastAPI service |
| `./deploy.sh down` | Stop and remove the container |
| `./deploy.sh restart` | Restart the service |
| `./deploy.sh status` | Show container status |
| `./deploy.sh logs` | Tail container logs |
| `./deploy.sh load-data --page_type 1 --date 2026-01-01 --hour 10` | Load a partition |
| `./deploy.sh query funnel` | Curl the `/funnel` endpoint |

## Endpoints

| Method & Path | Description |
|---------------|-------------|
| `GET /health` | Liveness check |
| `POST /load?page_type=&date=&hour=` | Pull a partition from the API into DuckDB; returns rows loaded |
| `GET /funnel` | Funnel conversion by page type and event stage |
| `GET /page-type-summary` | Volume, users, funnel depth and conversion per page type |
| `GET /user-engagement` | Per-user impressions, funnel depth, most-engaged page |
| `GET /hourly-traffic` | Impression volume and engagement by date/hour/page type |

## Analytics parity with dbt

The four queries reproduce the logic of the dbt analysis models:

- **funnel_analysis** — distinct impressions per stage, `pct_of_total` and
  `pct_from_previous_stage` (LAG over the `a<b<c<d<e<f` ordering).
- **page_type_summary** — built on the inlined per-impression aggregation
  (`distinct_events`, `max_event_reached`, first/last second).
- **user_engagement** — Postgres `distinct on` is replaced with a DuckDB
  `QUALIFY row_number()` window to select each user's most-engaged page type.
- **hourly_traffic** — volume and `pct_reaching_d` grouped by date/hour/page.

## Tests

```bash
uv venv
uv pip install -e ".[dev]"
uv run pytest -q
```

Tests use an in-memory DuckDB seeded with a small deterministic dataset
(documented in `tests/conftest.py`) so every metric is asserted against a
hand-computed value.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PATH` | `impressions.duckdb` | Path to the DuckDB database file |
| `API_BASE_URL` | `http://localhost:8000` | Web server API URL |
