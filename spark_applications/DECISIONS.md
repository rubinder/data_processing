# Engineering Decisions

Incident-style writeups of non-obvious changes: what was wrong, why it
mattered, and what was done. New entries go at the top.

---

## #7 — Debuggability (worked cases, plan analysis)

**Area:** `spark_applications/debugging/`, `DEBUGGING.md`

### Symptoms / risk

The repo demonstrated *correct* pipelines but nothing about diagnosing them
when they misbehave. The performance decisions in #2 (dynamic partition
overwrite, explicit schemas, `_compact`, AQE) were asserted in prose with no
reproducible evidence, and the two open #2 follow-ups — wiring salting into a
real hot key, and before/after shuffle metrics — had no vehicle short of a
cluster run.

### Resolution

Seven runnable cases over the impression model, each reproducing a pathology,
capturing the plan evidence that identifies it, applying a fix, and capturing
the plan again: skewed join, driver OOM, lost partition pruning, small-files
explosion, `AMBIGUOUS_REFERENCE`, `PythonException` from a UDF, and Python UDF
to pandas UDF.

`debugging/explain_tools.py` is the reusable core: `capture_plan` /
`capture_final_plan`, plus parsers for join strategies, exchange counts and
their partitioning schemes, `PartitionFilters` / `PushedFilters`, Python eval
operators, AQE shuffle reads, and runtime scan metrics. The parsers take plan
*text*, so they unit-test against captured fixtures with no SparkSession.

Three findings that contradict common advice, each pinned by a test:

- **Casts do not break partition pruning** in Spark 3.5.
  `(cast(hour#656 as int) = 10)` appears inside `PartitionFilters`, as do
  `to_date()` and `substring()`. UDFs break it; casts do not. This validates
  `aggregation.py`'s `hour.cast("int")`, which the received wisdom would have
  had us contort.
- **AQE did not split the skewed partition** — it reported
  `AQEShuffleRead coalesced` only, because the partitions sit under
  `skewedPartitionThresholdInBytes` (256MB). AQE alone is not a skew strategy,
  which qualifies the "Skew — AQE + adaptive skew-join enabled" row in #2.
- **The `_compact` repartition is free** in the common case. Measured
  `Exchange` count is unchanged (1 → 1): Spark collapses the redundant
  repartition, so the fix changes an existing shuffle's scheme from
  `RoundRobinPartitioning` to `hashpartitioning` rather than adding a shuffle.

Also documented: `explain()` shows the *initial* plan, so AQE rewrites are
invisible in it — and the final plan attaches only to the DataFrame object you
executed, so `count()` and `noop` writes leave it `isFinalPlan=false` while
`collect()` populates it. This is the usual reason AQE looks inert.

### Measured

| Case | Broken | Fixed |
| ---- | ------ | ----- |
| 01 skewed join | `SortMergeJoin`, 3 exchanges | `BroadcastHashJoin`, 1 exchange |
| 02 driver collect | 2,000 rows to driver | 1 row, constant |
| 03 pruning | 72 files / 9 partitions | 24 files / 3 partitions |
| 04 small files | 432 files (16/dir) | 27 files (1/dir) |
| 06 trace noise | 141 lines / 104 Scala frames | 2 lines that matter |
| 07 pandas UDF | `BatchEvalPython`, 0.99s | `ArrowEvalPython`, 0.47s |

### Verification

`tests/test_debugging_explain_tools.py` (26 tests, no Spark) and
`tests/test_debugging_cases.py` (30 tests, real Spark). The case tests assert
the *claim* of each case — the plan changes in the stated way and both paths
return identical results — so a future Spark version that changes this
behaviour fails the suite rather than leaving a quietly false writeup. Full
suite: 81 passed.

`pandas` / `pyarrow` / `setuptools` added to `pyproject.toml` for the case 07
Arrow path (pyspark 3.5's version check imports `distutils`, absent on Python
3.12+).

### Follow-ups (tracked in Tasks.md)

- Wire the case 01 broadcast/salting conclusion into `aggregation.py`'s real
  hot-key path.
- Re-measure cases 01 and 07 on a real cluster, where per-row and network
  costs are not understated by `local[*]`.

---

## #4 — Orchestration maturity (Airflow)

**Area:** `airflow_deployment/dags/impression_pipeline.py`

The two existing DAGs run hello-world Spark with `schedule=None` and
`catchup=False` — no real scheduling, backfill, or failure story. Added
`impression_pipeline`, which wires the actual `api_pull` → `aggregation` jobs:

- `schedule="@hourly"` + `catchup=True` + `max_active_runs=3` → missing hours
  backfill automatically, with bounded concurrency.
- `--date`/`--hour` derived from the logical date; combined with the jobs'
  dynamic partition overwrite (#2) this makes every run idempotent, so
  retries/backfills are safe to repeat.
- Retries with exponential backoff, a per-task SLA, and an `on_failure_callback`
  alert hook.
- `api_pull` is dynamically mapped over page_types rather than three copied
  tasks.

Verified by `py_compile` (Airflow isn't installed in the dev sandbox; the DAG
runs in the airflow_deployment container). Follow-ups: Datasets / data-aware
scheduling, a documented reprocessing runbook.

---

## #5 — Streaming / CDC consumer (connect the deployed pieces)

**Area:** `flink_applications/flink_applications/cdc_impressions.py`,
`cdc_sql.py`

Debezium published `cdc.impressions.*` topics but nothing consumed them — the
streaming stack was three disconnected docker-composes. Added a PyFlink job
that consumes `cdc.impressions.events` and computes a tumbling-window count per
page_type, demonstrating the streaming concerns the hello-world batch job
can't:

- **Exactly-once** via `enable_checkpointing(..., EXACTLY_ONCE)`.
- **Event-time + watermarks** off the Debezium source timestamp
  (`TO_TIMESTAMP_LTZ(__source_ts_ms, 3)`, 15s bounded out-of-orderness).
- **CDC semantics**: deletes (`__op = 'd'`) excluded from counts.

The pure SQL builders live in `cdc_sql.py` (no `pyflink` import) so they're
unit-testable without a Flink runtime — `tests/test_cdc_sql.py`. The full job
needs the Flink + Kafka containers to run. Follow-up: a schema registry on the
Debezium → Flink path, and an upsert-kafka / Delta sink instead of the print
sink.

---

## #6 — Observability (structured logging + metrics)

**Area:** `utils/observability.py`, `api_pull.py`, `aggregation.py`

`print()` statements carry no structure and are invisible to log aggregation.
Added stdlib-only structured logging: `log_event` / `log_metrics` emit
single-line JSON (indexable fields: job, stage, row counts, bytes), plus a
`timed` context manager. `api_pull` now emits `rows_read` / `rows_written` /
`rows_quarantined` metrics and structured lifecycle events; `aggregation` emits
start/complete events (without reintroducing the count() actions removed in
#2). Pure formatter tested in `tests/test_observability.py`. Follow-up:
OpenLineage across Spark → S3 → Glue → Athena → dbt.

---

## #3 — Idempotency, reliability & data quality (partial)

**Area:** `api_pull.py`, `utils/quality.py`

### Symptoms / risk

- The API pull was a single `requests.get(timeout=300)` — any transient blip
  or 5xx failed the whole job with no retry.
- Nothing guaranteed rows weren't silently dropped between read and write.
- The stack delivers CDC at-least-once, but there was no dedup, so a redelivered
  record would double-count downstream.

### Resolution

- **Retry + backoff.** `fetch_impression_data` now retries transient
  `RequestException`s with exponential backoff (`backoff_base * 2**attempt`).
  The pull is a safe GET and the downstream write is idempotent (issue #2), so
  retrying cannot double-apply data. `sleep` is injectable so tests don't wait.
- **Reconciliation.** After the quality split the job asserts
  `total == written + quarantined` via `reconcile_counts` — every row read is
  accounted for, nothing silently dropped.
- **Deduplication.** `quality.deduplicate(df, keys, order_by)` keeps the latest
  row per key (windowed `row_number`), making downstream processing idempotent
  under redelivery.

### Verification

- `test_api_pull.py::test_fetch_retries_then_succeeds` /
  `test_fetch_raises_after_max_attempts`.
- `test_quality.py::test_deduplicate_keeps_latest_per_key`; reconciliation is
  covered by the `reconcile_counts` tests and exercised in the job.

### Follow-ups (tracked in Tasks.md)

- Transactional coupling of raw-file landing and table write so a partial
  failure cannot orphan a raw file (the deterministic `job_id` + dynamic
  overwrite already make re-runs safe, but the two writes aren't atomic).

---

## #1 — Schema-contract enforcement & quarantine (partial)

**Area:** `utils/schema.py`, `utils/quality.py`, `utils/storage.py`,
`api_pull.py`; dbt `macros/safe_casts.sql`, `models/staging/`

### Symptoms / risk

Two ways schema drift / bad data corrupted things silently:

1. **Spark side:** the read enforced no contract beyond the column list. A
   row with a non-numeric `hour`, a missing column, or an extra column would
   either parse to garbage or (with the explicit schema from #2) silently
   null out — and still get written to the table, polluting every downstream
   aggregation.
2. **dbt side:** `stg_impressions` did `date::date` and `make_timestamp(...)`
   directly. Postgres has no `TRY_CAST`, so a *single* malformed date aborts
   the entire model run — one bad row takes down the whole pipeline.

### Resolution

- **Quarantine path (Spark).** Reads now run in PERMISSIVE mode with a
  `_corrupt_record` column (`schema_with_corrupt_column`). `split_on_contract`
  (in `utils/quality.py`) partitions the batch into conforming rows and
  rejects (corrupt marker present, or a required column null). Conforming rows
  are written to the table; rejects are appended to a `quarantine/` location
  via the new `StorageAdapter.write_quarantine`. Bad data is now *contained
  and inspectable* instead of silently corrupting the table or dropped.
- **Reconciliation.** `reconcile_counts` asserts no rows vanish between stages
  (raw landed vs. written + quarantined), with a configurable tolerance.
- **dbt safe casts.** `macros/safe_casts.sql` adds `safe_to_date` and
  `safe_event_timestamp`, which validate (regex / range checks) before casting
  and yield NULL on bad input. `stg_impressions` uses them, and a
  `severity: warn` not_null test on `event_date` surfaces malformed source
  dates without blocking the run.

### Verification

`tests/test_quality.py`:
- `test_split_quarantines_malformed_rows` — a row with `hour = "NOPE"` is routed
  to quarantine; the two valid rows pass through; the corrupt column is dropped
  from the clean output.
- `reconcile_counts` tests cover exact match, within-tolerance, drift (raises),
  and the zero-expected edge.

### Follow-ups (tracked in Tasks.md)

- Schema Registry (Avro/Protobuf) for the Debezium → Kafka → Flink path with an
  explicit compatibility mode, and a demo of surviving a source `ALTER TABLE`.
  This needs the Kafka/Flink infra standing and is not unit-testable here.
- Delta column-mapping + an explicit `mergeSchema`/`overwriteSchema` policy
  (the Databricks quarantine write already uses `mergeSchema=true`).

---

## #2 — ETL performance & correctness in the impression pipeline

**Area:** `spark_applications/api_pull.py`, `aggregation.py`,
`utils/storage.py`, `utils/session.py`, `utils/schema.py`

### Symptoms / risk

The original happy-path pipeline had four issues that only surface at scale or
on reprocessing — exactly when you can least afford them:

1. **Data loss on reprocess.** `write_partitioned` used
   `mode("overwrite")` with a *static* partition spec, which deletes the entire
   table before writing. Re-running a single `(page_type, date, hour)` — a
   routine backfill or late-data correction — destroyed every other partition.
   This is a silent correctness bug, not just a performance one.
2. **The pipeline was not distributed.** `api_pull` pulled the gzip into the
   driver, `gzip.decompress`-ed it on the driver, wrote a second decompressed
   copy to disk, then let Spark read that. None of the heavy lifting was
   distributed; Spark was pure overhead and the job couldn't scale past a file
   that fits in driver memory.
3. **`inferSchema=True` everywhere.** This costs an extra full pass over the
   data purely to guess types, and the guess is non-deterministic across
   files (an all-null or all-int column infers differently), so an input
   shift silently corrupts downstream types.
4. **`df.count()` used as progress logging.** Each `.count()` is a full job;
   counting before a write forces the whole lineage to recompute. `aggregation`
   did this twice (once on the filtered input, once on the output).

Two secondary issues: hour-grain partitioning with no pre-write repartition
causes the small-files explosion (tasks x partitions fragments), and the
aggregation filter compared `hour` (a partition value) without normalizing its
type.

### Resolution

| Issue | Fix |
| ----- | --- |
| Destructive overwrite | `spark.sql.sources.partitionOverwriteMode=dynamic` (session + per-write) for parquet; `partitionOverwriteMode=dynamic` write option for Delta. Overwrite is now scoped to the partitions present in the DataFrame. |
| Driver decompress | `read_csv` reads the `.gz` directly; Spark decompresses and parses in a distributed manner. The driver only lands the raw bytes (an unavoidable single-endpoint HTTP fetch). |
| Type inference | Explicit `IMPRESSION_SCHEMA` (`utils/schema.py`); single read pass and an enforced column contract. Partition column type inference disabled for deterministic typing. |
| count() actions | Removed; replaced with a single terminal log line. |
| Small files | `_compact()` repartitions by the partition keys before writing → one file per partition directory. |
| hour typing | Aggregation casts `hour` to int in the partition filter so pruning works regardless of read-back type. |
| Skew | AQE + adaptive skew-join enabled at session level (`session.py`). Wiring the explicit `salted_join` into the aggregation hot key remains tracked under #2 in `Tasks.md`. |

### Before / after (api_pull read + write)

```python
# BEFORE — driver-side decompress, second file write, inferSchema, count()
decompressed = gzip.decompress(raw_content)          # on the driver
storage.save_raw_file(decompressed, decompressed_path)
df = storage.read_csv(spark, decompressed_path)      # inferSchema=True
print(f"Read {df.count()} rows")                     # full job
df.write.mode("overwrite").partitionBy(...).parquet(...)  # nukes whole table

# AFTER — distributed gz read, explicit schema, idempotent partition write
df = storage.read_csv(spark, raw_path, schema=IMPRESSION_SCHEMA)
storage.write_partitioned(df, table_path, ["page_type", "date", "hour"])
# dynamic overwrite + repartition-by-key compaction inside the adapter
```

### Verification

`tests/test_storage.py`:
- `test_partition_overwrite_is_non_destructive` — reprocessing `page_type=1`
  leaves `page_type=2` intact (would have failed under the old static
  overwrite).
- `test_partitioned_write_compacts_to_one_file_per_partition` — one data file
  per partition directory.

`tests/test_api_pull.py::test_read_gzip_csv_directly_with_schema` — the `.gz`
is read directly and the schema matches the contract (typed, not inferred).

### Follow-ups (tracked in Tasks.md)

- Wire `salted_join` into the aggregation hot key; capture before/after shuffle
  metrics.
- Malformed-record handling (FAILFAST / quarantine) belongs to backlog #1
  (schema evolution), not here.
