# ClickHouse Deployment

Deploys a real multi-node **ClickHouse** cluster locally with Docker and runs
the project's impression analytics on it. This module demonstrates ClickHouse
as a **distributed OLAP engine for concurrent queries across multiple nodes**:
two shards coordinated by `clickhouse-keeper`, a `Distributed` table layered
over `ReplicatedMergeTree`, and four analytical queries that fan out and run
concurrently on both nodes.

## Why this enables concurrent, distributed queries

- **Two shards** (`clickhouse-01`, `clickhouse-02`) each hold part of the data
  in a local `ReplicatedMergeTree` table (`impressions_local`).
- **clickhouse-keeper** provides ZooKeeper-compatible coordination so the
  replicated tables share consistent metadata and DDL (`ON CLUSTER`) applies to
  every node at once.
- The **`Distributed` table** (`impressions`) is a thin routing layer. A read
  against it fans the query out to both shards, which execute **in parallel**;
  each shard returns partial aggregation states that the initiator merges.
- Because `uniqExact`, `max`, `sum`, and `count` are mergeable aggregate
  functions, per-impression `GROUP BY` results stay correct even though rows
  are sharded with `rand()` — the partial states are merged by key on the
  initiator. This is what lets many analytical queries run concurrently across
  nodes while returning a single correct result.

## Directory Structure

```
clickhouse_deployment/
├── docker-compose.yaml       # keeper + clickhouse-01 + clickhouse-02
├── deploy.sh                 # Deployment / query management script
├── load_data.py              # Loads data from web server API into ClickHouse
├── pyproject.toml            # uv project (clickhouse-connect, requests, pytest; chdb for tests)
├── README.md
├── config/
│   ├── cluster.xml           # remote_servers (2 shards) + keeper connection
│   ├── keeper.xml            # single-node clickhouse-keeper config
│   ├── macros-01.xml         # shard/replica macros for node 01
│   └── macros-02.xml         # shard/replica macros for node 02
├── init/
│   └── schema.sql            # ReplicatedMergeTree local + Distributed table
├── queries/
│   ├── funnel_analysis.sql   # Event funnel conversion rates
│   ├── page_type_summary.sql # Page type statistics
│   ├── user_engagement.sql   # User-level engagement metrics
│   └── hourly_traffic.sql    # Hourly traffic patterns
└── tests/
    ├── conftest.py           # Embedded ClickHouse (chdb) fixture + dataset
    ├── test_queries.py       # Executes each query, asserts exact numbers
    └── test_loader.py        # Pure CSV fetch/parse tests (mocked requests)
```

## Prerequisites

- Docker and Docker Compose installed
- Web server running locally (see `../web_server_local/`)
- Shared Docker network (`data-processing-network`) — created automatically by `deploy.sh`

## Quick Start

```bash
# 1. Start the ClickHouse cluster (keeper + 2 shards)
./deploy.sh up

# 2. Create the distributed tables (ON CLUSTER)
./deploy.sh schema

# 3. Start the web server (in another terminal)
cd ../web_server_local && ./deploy.sh up

# 4. Load impression data from the API into the distributed table
./deploy.sh load-data --all

# 5. Run an analytical query across both shards
./deploy.sh query funnel_analysis
./deploy.sh query page_type_summary
./deploy.sh query user_engagement
./deploy.sh query hourly_traffic
```

## Commands

| Command | Description |
|---------|-------------|
| `./deploy.sh up` | Start keeper and both ClickHouse nodes, wait for health |
| `./deploy.sh down` | Stop and remove containers |
| `./deploy.sh restart` | Restart all services |
| `./deploy.sh status` | Show container status |
| `./deploy.sh logs` | Tail container logs |
| `./deploy.sh schema` | Apply `init/schema.sql` (creates the distributed tables) |
| `./deploy.sh load-data --all` | Load all page types for current date/hour |
| `./deploy.sh load-data --page_type 1 --date 2026-01-01 --hour 10` | Load specific slice |
| `./deploy.sh query <name>` | Run `queries/<name>.sql` on the cluster |

## Queries

Each query reproduces the logic of the matching dbt model in ClickHouse SQL,
reading from the `impressions` Distributed table. The table name is a `{table}`
template token so the same SQL runs against a local fixture table under test.

- **funnel_analysis**: distinct impressions reaching each event stage a→f per
  page type, with `pct_of_total` and per-stage conversion via `lagInFrame`.
- **page_type_summary**: volume, unique users, average/max funnel depth, average
  duration, and reach-to-d/e/f counts and percentages per page type.
- **user_engagement**: per-user impressions, page types visited, funnel depth,
  total events, first/last seen, and most-engaged page type (`row_number`).
- **hourly_traffic**: impression volume, unique users, funnel depth and reach-to-d
  by date / hour / page type.

## Connecting to ClickHouse

```bash
# HTTP interface
curl 'http://localhost:8123/?query=SELECT%20*%20FROM%20system.clusters'

# native client inside a node
docker compose exec clickhouse-01 clickhouse-client
```

Node 01: HTTP `localhost:8123`, native `localhost:9000`.
Node 02: HTTP `localhost:8124`, native `localhost:9001`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host (for load_data.py) |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | (empty) | ClickHouse password |
| `API_BASE_URL` | `http://localhost:8000` | Web server API URL |

## Running Tests

The query tests execute the real `queries/*.sql` against an **embedded
ClickHouse** (`chdb`) over a small deterministic dataset, so they genuinely
validate the SQL. The loader tests are pure and always run.

```bash
uv venv
uv pip install -e .
uv pip install chdb pytest
uv run pytest -q
```

If `chdb` cannot be installed on your platform, the query tests are skipped and
the loader tests still run.
