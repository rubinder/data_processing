# Polars Deployment — Design

Date: 2026-09-15

## Goal

Add `polars_deployment/`, a standalone module demonstrating **Polars as a
single-node, in-process DataFrame engine** over the repo's impression event
model. It is the batch counterpart to `duckdb_deployment` (embedded SQL OLAP):
the same four analyses, expressed in Polars' lazy expression API instead of
SQL, stored as hive-partitioned Parquet instead of a database file.

The module answers a question the repo does not yet answer: when a Spark job
fits on one machine, what does the single-node alternative look like, and what
does it cost in memory and time? Every number in the README comes from a
benchmark script in the module and is verified against the other engines'
answers before it is reported.

## Decisions

- **Ingestion**: the web server API (`GET /impression`, csv.gz), like every
  other module. Polars reads the gzip bytes directly with `read_csv`; there is
  no manual gunzip and no row loop.
- **Storage**: hive-partitioned Parquet under a data directory,
  `page_type=N/date=YYYY-MM-DD/hour=H/data.parquet`. One load replaces one
  partition directory, so loads are idempotent. `scan_parquet` with
  `hive_partitioning=True` recovers the partition columns and prunes files
  when a query filters on them.
- **Analyses**: the four dbt models (funnel, page-type summary, user
  engagement, hourly traffic) as functions `LazyFrame -> LazyFrame`, sharing a
  per-impression aggregation that mirrors `int_impressions_aggregated.sql`.
  Nothing is collected until the caller asks, so projection and predicate
  pushdown reach the Parquet reader.
- **Execution modes**: the same lazy plan is run on the in-memory engine and
  on the streaming engine (`collect(engine="streaming")`). The tests assert
  both produce identical results on the fixture dataset.
- **No service**. A batch engine is exercised through a CLI (`load`, `query`,
  `explain`, `partitions`) and run-to-completion Docker containers, not a
  long-running FastAPI process. The DuckDB module already covers the
  "embedded engine behind an API" shape.
- **Benchmark**: `benchmarks/bench_engines.py` generates a deterministic
  synthetic dataset at a chosen scale, then runs the four analyses as
  (a) eager (read everything, then compute), (b) lazy in-memory,
  (c) lazy streaming and (d) DuckDB over the same Parquet files, each in a
  fresh subprocess so peak RSS is measured per variant. Results are
  cross-checked for equality and written to `benchmarks/results/`.
- **Dependencies**: `polars` and `requests` at runtime; `duckdb` only in the
  `bench` extra; `pytest` in `dev`. Python 3.10, uv, PEP 8, like the rest
  of the repo.

## Layout

```
polars_deployment/
├── Dockerfile, docker-compose.yaml, deploy.sh, pyproject.toml, README.md
├── polars_deployment/
│   ├── schema.py      # column list, dtypes, partition columns
│   ├── store.py       # ParquetStore: write_partition / scan / partitions
│   ├── loader.py      # API -> DataFrame -> partition
│   ├── analyses.py    # aggregated + the four analyses, LazyFrame in/out
│   └── cli.py         # load | query | explain | partitions
├── benchmarks/
│   ├── synth.py       # deterministic synthetic partitions at scale
│   ├── bench_engines.py
│   └── results/       # latest.json, latest.md
└── tests/
    ├── conftest.py    # the DuckDB module's fixture dataset, as Parquet
    ├── test_analyses.py, test_store.py, test_loader.py, test_cli.py
```

## Testing

Real Polars, no services. The fixture is the hand-derived dataset from
`duckdb_deployment/tests/conftest.py`, so the expected numbers are already
documented and the two engines can be checked against each other. Loader
tests mock `requests.get` with real gzip CSV bytes.
