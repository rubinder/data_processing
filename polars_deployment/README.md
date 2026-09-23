# Polars Deployment

Demonstrates **Polars as a single-node, in-process DataFrame engine**. It is
the batch counterpart to [`../duckdb_deployment`](../duckdb_deployment/):
where that module embeds a SQL OLAP engine behind an API, this one runs the
same four analyses as lazy Polars expression pipelines over hive-partitioned
Parquet, with no server, no service and no SQL.

It answers a question the rest of the repo leaves open. `spark_applications`
runs these aggregations on a cluster. When the data fits on one machine, what
does the single-node alternative cost, and does Polars' lazy and streaming
machinery actually buy anything over "read it all, then compute"? The
[benchmark](#benchmark-one-machine-four-strategies) below measures that, and
its answer is not uniformly in Polars' favour.

## The single-node pattern

- `polars_deployment/schema.py` - the impression schema with the integer
  columns narrowed to Int8. Polars keeps whole columns in memory, so `hour`
  costs one byte per row instead of eight.
- `polars_deployment/store.py` - `ParquetStore`: one directory per
  `(page_type, date, hour)` partition in hive layout,
  `page_type=1/date=2026-01-01/hour=10/data.parquet`. Writing a partition
  replaces its directory, so loads are idempotent. `scan()` is a single
  `scan_parquet` over the tree that recovers the partition columns from the
  paths and prunes whole files when a query filters on them.
- `polars_deployment/loader.py` - pulls the gzip CSV from the web server API
  and hands the bytes straight to `read_csv`, which detects the gzip header
  and applies the schema. There is no gunzip step and no row loop.
- `polars_deployment/analyses.py` - the four dbt analyses (funnel, page-type
  summary, user engagement, hourly traffic) as functions
  `LazyFrame -> LazyFrame`, sharing a per-impression aggregation that mirrors
  `int_impressions_aggregated.sql`. Nothing runs until `collect()`, so the
  optimizer sees the plan from the Parquet scan to the final sort.
- `polars_deployment/cli.py` - `load | query | explain | partitions`. The
  same plan can be collected on the in-memory engine or the streaming engine.

The numbers are held to the DuckDB module's numbers. The test fixture is the
same hand-derived dataset as `duckdb_deployment/tests/conftest.py`, rounding
is set to half-away-from-zero to match DuckDB and PostgreSQL, and the
benchmark refuses to report a speedup until every engine's answer matches.

## Directory structure

```
polars_deployment/
├── Dockerfile                 # python:3.10-slim, run-to-completion CLI
├── docker-compose.yaml        # polars-app on the shared network, data volume
├── deploy.sh                  # build|load-data|query|explain|partitions|benchmark|shell|clean
├── pyproject.toml             # uv project (polars, requests; dev: pytest; bench: duckdb, pyarrow)
├── README.md
├── polars_deployment/
│   ├── schema.py              # columns, dtypes, partition layout
│   ├── store.py               # ParquetStore: write_partition / scan / partitions
│   ├── loader.py              # API csv.gz -> typed DataFrame -> partition
│   ├── analyses.py            # aggregated + the four analyses, lazy
│   └── cli.py                 # argparse entry point
├── benchmarks/
│   ├── synth.py               # deterministic synthetic partitions at scale
│   ├── bench_engines.py       # eager vs lazy vs streaming vs DuckDB
│   └── results/               # latest.json, latest.md
└── tests/
    ├── conftest.py            # the DuckDB module's fixture dataset, as Parquet
    ├── test_analyses.py       # exact numbers on both engines, engines agree
    ├── test_store.py          # layout, idempotent writes, pruning, projection
    ├── test_loader.py         # gzip CSV parsing, mocked API, idempotent loads
    └── test_cli.py            # query/explain/partitions output
```

## Quick start (Docker)

```bash
# 1. Start the web server (in another terminal)
cd ../web_server_local && ./deploy.sh up

# 2. Build the image
./deploy.sh build

# 3. Load impression partitions from the API into the Parquet store
./deploy.sh load-data --page_type 1 --date 2026-01-01 --hour 10
./deploy.sh load-data --page_type 2 --date 2026-01-01 --hour 10
./deploy.sh load-data --page_type 3 --date 2026-01-01 --hour 10

# 4. Run the analyses
./deploy.sh partitions
./deploy.sh query funnel
./deploy.sh query page-type-summary --format json
./deploy.sh query user-engagement --engine streaming
./deploy.sh query hourly-traffic --page_type 1

# 5. See what the optimizer did with a filtered query
./deploy.sh explain hourly-traffic --page_type 1 --unoptimized
```

The container runs to completion: every command is one CLI invocation over
the Parquet store on the `polars-data` volume. There is nothing to keep up.

## Running locally without Docker

```bash
uv sync --extra dev --extra bench
uv run python -m polars_deployment.cli load --page_type 1 --date 2026-01-01 --hour 10
uv run python -m polars_deployment.cli query funnel
uv run pytest
```

`POLARS_DATA_DIR` (default `./data/impressions`) and `API_BASE_URL` (default
`http://localhost:8000`) configure the store and the web server.

## Commands

| Command | Description |
|---|---|
| `./deploy.sh build` | Build the image |
| `./deploy.sh load-data --page_type N --date D --hour H` | Fetch one partition from the API and (re)write it |
| `./deploy.sh query NAME [--engine in-memory\|streaming] [--format table\|json] [--page_type N] [--date D] [--hour H]` | Run an analysis: `funnel`, `page-type-summary`, `user-engagement`, `hourly-traffic` |
| `./deploy.sh explain NAME [--unoptimized] [filters]` | Print the optimized (and optionally the raw) plan |
| `./deploy.sh partitions` | List the partitions in the store |
| `./deploy.sh benchmark [--days N] [--repeat N] [--regenerate]` | Run the engine benchmark inside the container |
| `./deploy.sh shell` | Shell in the container |
| `./deploy.sh clean` | Remove the container and the data volume |

## What the lazy API does with a query

`explain hourly-traffic --page_type 1 --unoptimized` on the benchmark
dataset (144 partition files) prints the plan as written and the plan as run.
Trimmed to the parts that changed:

```
== unoptimized ==
AGGREGATE [n_unique(impression_id), n_unique(user_id), mean(distinct_events), ...]
  BY [event_date, hour, page_type]
  FROM
  AGGREGATE [len(), n_unique(event_type), min(second), max(second),
             min(first_event_at), max(last_event_at), max(event_type)]
    BY [user_id, impression_id, page_type, date, hour]
    FROM
    FILTER col("page_type") == 1
    FROM
      Parquet SCAN [.../page_type=1/date=2026-01-01/hour=0/data.parquet, ... 143 other sources]
      PROJECT */8 COLUMNS
      ESTIMATED ROWS: 8086417

== optimized ==
AGGREGATE [n_unique(impression_id), n_unique(user_id), mean(distinct_events), ...]
  BY [event_date, hour, page_type]
  FROM
  AGGREGATE [n_unique(event_type).alias(distinct_events), max(event_type).alias(max_event_reached)]
    BY [user_id, impression_id, page_type, date, hour]
    FROM
    Parquet SCAN [.../page_type=1/date=2026-01-01/hour=0/data.parquet, ... 47 other sources]
    PROJECT 6/8 COLUMNS
    SELECTION: col("page_type") == 1
    ESTIMATED ROWS: 2695488
```

Three things happened without the analysis code knowing about them:

- **Partition pruning.** The filter became a `SELECTION` on the scan and the
  hive predicate cut the file list from 144 to 48. This is the same
  mechanism as the Spark partition-pruning case in
  `spark_applications/debugging`; there, losing it was the bug.
- **Projection pushdown.** The scan reads 6 of 8 columns; `min` and
  `second` are never decoded because nothing downstream needs them.
- **Aggregate pruning.** The shared per-impression aggregation computes seven
  things; hourly traffic uses two. The other five, including the two
  timestamp expressions, were removed from the plan entirely. The shared
  helper costs nothing it does not use.

The same plan can be run on two engines. `collect()` uses the default
in-memory engine, which materialises each intermediate in RAM.
`collect(engine="streaming")` runs the pipeline in batches, which bounds peak
memory by the batch size plus whatever state the operators must hold. The
tests assert both give identical results; the benchmark shows what they cost.

## Benchmark: one machine, four strategies

`benchmarks/bench_engines.py` generates a deterministic synthetic dataset
with the web server's funnel shape and runs six workloads under four
strategies, every `(strategy, workload)` pair in its own subprocess so peak
RSS is measured per pair:

| strategy | what it is |
|---|---|
| `eager` | `read_parquet` everything into one DataFrame, then compute. The pandas-shaped workflow: the whole table is resident before the first aggregation, and a `page_type` filter is applied after the read. |
| `lazy` | `scan_parquet` and the lazy plan, collected on the in-memory engine. |
| `streaming` | the same plan collected with `engine="streaming"`. |
| `duckdb` | the SQL from `duckdb_deployment/app/queries.py`, executed by DuckDB over the same Parquet files with `read_parquet(..., hive_partitioning=true)`. |

Every output is verified against `lazy` before a number is reported.

Dataset: 8,161,390 event rows in 144 partition files, 92.5 MB of zstd
Parquet (2 days x 24 hours x 3 page types, 20,000 impressions per hour).
Machine: 8-core Apple Silicon laptop, Python 3.10, Polars 1.44.2,
DuckDB 1.5.5. Median of 3 runs; full tables in
[`benchmarks/results/latest.md`](benchmarks/results/latest.md).

| workload | eager | lazy | streaming | duckdb |
|---|---|---|---|---|
| funnel | 0.24 s / 1517 MB | 0.23 s / 1340 MB | 0.45 s / 2378 MB | 0.30 s / 1447 MB |
| page-type-summary | 0.66 s / 2449 MB | 0.69 s / 2473 MB | 1.42 s / 3595 MB | 0.77 s / 2701 MB |
| user-engagement | 0.72 s / 2638 MB | 0.73 s / 2655 MB | 1.32 s / 3371 MB | 0.93 s / 2916 MB |
| hourly-traffic | 0.70 s / 2435 MB | 0.72 s / 2477 MB | 1.49 s / 3453 MB | 0.75 s / 2674 MB |
| hourly-traffic, page_type=1 only | 0.23 s / 1163 MB | 0.20 s / 879 MB | 0.27 s / 1369 MB | 0.23 s / 1207 MB |
| filter to stages d+ and write one Parquet file | 0.16 s / 1062 MB | 0.15 s / 614 MB | 0.14 s / 502 MB | 0.22 s / 1019 MB |

### What the numbers say

**Eight million rows is not a cluster problem.** Every analysis finishes in
under a second on a laptop, in Polars or DuckDB, and the two engines are
within 30% of each other throughout. The Spark aggregation job over the same
model spends longer than that starting a session.

**On the full analyses, lazy buys almost nothing over eager.** The four
analyses touch nearly every column and every row, so there is nothing for
projection or predicate pushdown to remove: eager and lazy land within a few
percent on both time and memory. Memory is dominated by the per-impression
aggregation, one group per impression with two 36-character string keys,
which is about 2.4 GB of state regardless of how the input was read.

**Lazy wins when the query is narrower than the table.** With the
`page_type = 1` filter, the lazy scan reads 48 files instead of 144 and
peaks at 879 MB; eager reads everything and then filters, peaking at
1163 MB. The narrower the query relative to the store, the larger this gap,
and it is the reason to write the analyses against `scan()` rather than
`read_parquet()` even when today's data fits.

**The streaming engine is the wrong tool for these aggregates.** On the four
analyses it is about twice as slow and uses 40% more peak memory than the
in-memory engine. This is the expected shape, not a tuning failure: the
per-impression group-by has a state size proportional to the input, so
batching the input does not bound anything, and the streaming engine pays
for partitioned hash tables and a merge on top. Streaming bounds memory when
the operators between scan and sink hold little state.

**The streaming engine is the right tool for the ETL-shaped pass.** Filter
the raw events to stages d and beyond and `sink_parquet` them: streaming is
the fastest of the four and peaks at 502 MB, half of eager's 1062 MB,
because no batch is ever held longer than it takes to write it. That is the
shape of a compaction, a re-partition or a format conversion, and it is
where `sink_*` belongs.

**DuckDB and Polars are peers here, not rivals.** Same files, same answers,
similar cost. The choice between them in this repo is about interface: SQL
behind a service (`duckdb_deployment`) versus expressions in a batch job
(this module). Both fit in the process that needs them.

### When this replaces the Spark job

Use this shape when the working set fits in one machine's memory with room
for the group-by state (a rule of thumb from the table above: budget about
300 bytes per event row for these analyses), the job runs on a schedule
rather than serving concurrent users, and the inputs are already Parquet or
CSV on a filesystem or object store Polars can scan. Stay on Spark when the
data does not fit, when the join side cannot be broadcast, or when the
platform already runs there and the cost of a second runtime outweighs the
savings. `spark_applications/DECISIONS.md` covers the cluster side of that
trade.

### Reproducing

```bash
uv sync --extra dev --extra bench
uv run python benchmarks/bench_engines.py --days 2 --repeat 3
# or larger:
uv run python benchmarks/bench_engines.py --days 7 --repeat 3 --regenerate
# or inside the container:
./deploy.sh benchmark --days 2
```

The first run generates `benchmarks/data/` (gitignored). The DuckDB variant
is skipped, with a message, if the `duckdb` package or `../duckdb_deployment`
is missing.

## Tests

```bash
uv run pytest
```

29 tests against real Polars, no services. The four analyses are asserted
to the same hand-derived numbers as the DuckDB module on both execution
engines, and the engines are asserted equal to each other. Store tests cover
the hive layout, idempotent partition replacement, file pruning and
projection pushdown by inspecting the optimized plan. Loader tests mock the
API with real gzip CSV bytes.
