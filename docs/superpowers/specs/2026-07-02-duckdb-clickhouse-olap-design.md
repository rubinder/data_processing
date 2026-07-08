# DuckDB + ClickHouse OLAP Patterns — Design

Date: 2026-07-02

## Goal

Add two new analytical/ingestion patterns to the repo, each in its own top-level
directory mirroring `dbt_deployment` conventions:

- **`duckdb_deployment/`** — DuckDB as an *embedded, application-level* OLAP engine.
  An in-process DuckDB behind a small FastAPI service serving analytical endpoints.
- **`clickhouse_deployment/`** — ClickHouse as a *distributed* OLAP engine for
  concurrent queries across multiple nodes. A real multi-node cluster (2 shards +
  clickhouse-keeper), querying a `Distributed` table over `ReplicatedMergeTree`.

Both ingest the project's impression data and implement the same four analyses the
dbt project and Spark aggregation already establish, so the whole repo tells one
analytical story.

## Shared decisions

- **Ingestion source**: the web server API (`web_server_code` `/impression`
  endpoint, `csv.gz`), same pattern as `dbt_deployment/load_data.py`.
- **Analytical patterns** (mirror dbt `models/analysis/*`):
  1. `funnel_analysis` — impressions reaching each event stage a–f, per page_type,
     with pct-of-total and pct-from-previous-stage.
  2. `page_type_summary` — volume, unique users, avg/max funnel depth, pct reaching
     d/e/f, per page_type.
  3. `user_engagement` — per-user impressions, page types visited, funnel depth,
     most-engaged page type.
  4. `hourly_traffic` — impressions/users/funnel-depth per date+hour+page_type.
- **Schema** (matches `raw.impressions`): `user_id, impression_id, page_type,
  date, hour, min, second, event_type`.
- Both connect to the shared `data-processing-network` docker network.
- Each directory ships `Dockerfile`/compose, `deploy.sh` (up/down/restart/status/
  logs/load-data/query), `README.md`, `pyproject.toml` (uv), and passing tests.

## DuckDB deployment (simplest route)

- `duckdb_deployment/app/` — FastAPI app embedding DuckDB in-process.
  - `db.py` — connection factory (file-backed `impressions.duckdb` or `:memory:`),
    `init_schema()`.
  - `queries.py` — the four analyses as functions taking a `duckdb` connection and
    returning list-of-dicts. Pure DuckDB SQL.
  - `loader.py` — pull `csv.gz` from the API, insert into the `impressions` table
    (idempotent delete-by-partition then insert), reusing the dbt loader pattern.
  - `main.py` — FastAPI endpoints: `POST /load`, `GET /funnel`,
    `GET /page-type-summary`, `GET /user-engagement`, `GET /hourly-traffic`,
    `GET /health`.
- **Tests** (`tests/`, pytest): load a deterministic fixture into an in-memory
  DuckDB, assert each query's shape and key funnel numbers via FastAPI `TestClient`.
  Real DuckDB, no external services — genuinely runnable in CI.

## ClickHouse deployment

- `clickhouse_deployment/docker-compose.yaml` — `clickhouse-keeper`,
  `clickhouse-01`, `clickhouse-02` (2 shards, 1 replica each) on the shared network.
- `config/` — `cluster.xml` (remote_servers `impressions_cluster`, keeper), per-node
  `macros.xml` (shard/replica). Simplest working cluster.
- `init/schema.sql` — `impressions_local` (`ReplicatedMergeTree`) + `impressions`
  (`Distributed` over the cluster).
- `queries/*.sql` — the four analyses in ClickHouse SQL, querying the distributed
  `impressions` table. Table name parametrized so tests can point at a local table.
- `load_data.py` — pull `csv.gz` from the API, insert via `clickhouse-connect` into
  the distributed table.
- `deploy.sh` — up/down/restart/status/logs/load-data/query/schema.
- **Tests** (`tests/`, pytest): (a) loader parse/transform logic with mocked
  `requests`; (b) the four analytical queries executed against **embedded ClickHouse
  via `chdb`** on a fixture loaded into a local `MergeTree` table — real ClickHouse
  SQL semantics, no cluster required. Skip gracefully only if `chdb` cannot import.

## Testing gate

Both test suites must pass locally via `uv run pytest` before completion. No live
cluster/service required — DuckDB and chdb run in-process.

## Out of scope

No Airflow wiring, no AWS/Databricks deployment of these engines, no schema
registry. Focused on the two OLAP ingestion+analytics patterns and their tests.
