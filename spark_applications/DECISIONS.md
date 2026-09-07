# Engineering Decisions

Incident-style writeups of non-obvious changes: what was wrong, why it
mattered, and what was done. New entries go at the top.

---

## #8 — FinOps (AWS deployment)

**Area:** `aws_deployment/cloudformation/main.yaml`, `glue/etl_job.py`,
`aws_deployment/FINOPS.md`, `utils/session.py` (parquet codec)

### Symptoms / risk

The template had no cost controls: untiered S3 with no expiry on scratch
prefixes, no Athena guardrail (an unfiltered `SELECT *` was billed in full), a
Glue 4.0 job with fixed workers, no timeout and `append` semantics, and an
always-on EMR cluster of on-demand nodes with no scaling or idle termination.
Nothing was tagged, so Cost Explorer could not attribute any of it. The Glue
script also re-introduced everything #2 removed on the Spark side:
`inferSchema`, `append`, no compaction, a `count()` for logging.

### Resolution

- **S3 lifecycle.** `raw/` to STANDARD_IA at 30 days and GLACIER_IR at 90;
  `processed/` to Intelligent-Tiering; `quarantine/` expires at 90 days and
  `athena-results/` at 7; incomplete multiparts aborted; noncurrent versions
  bounded.
- **Athena workgroup** with `BytesScannedCutoffPerQuery` (parameter, 10 GiB
  default), `ProcessedBytes` published to CloudWatch, engine v3 pinned.
- **Glue job** on Glue 5.0 (Spark 3.5), G.1X with auto-scaling (so
  `NumberOfWorkers` is a ceiling), 60-minute timeout, one retry (safe because
  the write is idempotent), and one `--conf` carrying zstd + dynamic partition
  overwrite. `etl_job.py` mirrors #2 and #1: explicit schema, PERMISSIVE read
  with a quarantine write, dynamic overwrite, repartition-by-keys, zstd, one
  terminal JSON log line.
- **EMR** keeps master/core on demand, adds a Spot task group, a managed
  scaling policy (everything above the core floor is Spot task capacity) and a
  one-hour idle auto-termination; `spark-defaults` mirrors `utils/session.py`.
- **Tags** (`Project`, `Environment`, `CostCenter`) on every taggable
  resource.
- **Parquet codec zstd** everywhere (`utils/session.py`, Glue, EMR): smaller
  files are cheaper to store *and* cheaper to scan in Athena.

`FINOPS.md` documents each lever, the cost formulas (Athena = bytes scanned x
$/TB with a 10 MiB floor; Glue = DPU-seconds / 3600 x $/DPU-hour; EMR =
instance-hours x (EC2 + uplift)), how to measure before/after, the trade-offs,
and a real-run checklist. No savings are claimed; nothing has been measured
against a real bill yet.

### Verification

`cfn-lint` clean (0 errors, 0 warnings; the pre-existing unused-`VpcId`
warning is fixed by the new Batch security group). `py_compile` on the Glue
script. A local zstd parquet write/read round-trip.

**Deployed 2026-09-06** (account-level validation of what lint cannot see).
Two failures on the first attempts, both pre-existing in the template and
both now fixed:

1. `DataCrawler` failed with `Unable to validate s3 target ... 404`. Glue
   validates the crawler's S3 target at create time, and the crawler named
   the bucket by its reconstructed string, so CloudFormation saw no
   dependency and created it first. The path is now `!Sub
   "s3://${DataLandingBucket}/"`. That closed a dependency cycle (bucket ->
   S3 notification -> Lambda permission -> Lambda -> state machine ->
   crawler -> bucket), broken by having the state machine address the
   crawler by its fixed name instead of `!Ref`.
2. `EMRCluster` terminated with `Service role ... has insufficient EC2
   permissions ... ec2:CreateSecurityGroup`. `AmazonEMRServicePolicy_v2`
   only permits provisioning on VPCs/subnets/security groups tagged
   `for-use-with-amazon-emr-managed-policies=true`, and the template does not
   own the network it is given. An inline `EmrEc2Provisioning` policy grants
   the legacy role's EC2/CloudWatch/PassRole actions; the comment in the
   template says how to scope it back down once the network is owned.

Also found while deploying: `.env.example`'s placeholder access keys, copied
verbatim, shadow the CLI profile (`InvalidAccessKeyId`), so `deploy.sh` now
ignores placeholder values; and boto3 needs `botocore[crt]` to read the
credentials the `aws login` provider issues. EMR 7.x defaults to Python 3.9,
below the project's 3.10 baseline, so the cluster gets a bootstrap action
(`emr/bootstrap_python311.sh`) and `spark.pyspark.python` pointing at 3.11.

Three more, found by running the pipeline on the deployed stack:

3. **Batch on Fargate could not pull its image**
   (`ResourceInitializationError ... GetAuthorizationToken ... i/o timeout`).
   The given VPC has no NAT, so a Fargate task needs
   `NetworkConfiguration.AssignPublicIp: ENABLED` on the job definition.
4. **The Glue script crashed on `--JOB_RUN_ID: conflicting option string`.**
   `getResolvedOptions` pre-registers the Glue-injected options
   (`JOB_NAME`, `JOB_ID`, `JOB_RUN_ID`, `TempDir`); requesting `JOB_RUN_ID`
   again is an error. Only `run_id` is requested now.
5. **The trigger Lambda named executions from the S3 key alone.** Re-landing
   the same object (a retry, a backfill, a corrected file) failed with
   `ExecutionAlreadyExists` and the pipeline silently never ran. The name now
   carries the S3 event sequencer, which is unique per object change.
6. **The Batch encoding check rejected every real file.** It ran `chardet` on
   the *compressed* bytes of the gzip the API serves, got `None`, and exited
   1 ("File is not valid UTF-8"). It now decompresses a sample first.
7. **`docker buildx ... -t "$REPO:latest"` in zsh pushed nothing**: zsh reads
   `:l` as a lowercase modifier, so the image went to a repository that does
   not exist and the build still exited 0. `${REPO}:latest` is required.

After these, an object landing in `raw/` drove the whole pipeline
unattended: Lambda -> Step Function -> Batch encoding check (Fargate) ->
Glue crawler -> Glue ETL, `ExecutionSucceeded`, with the processed zstd
parquet and the quarantine rows in the processed bucket.

What the deployment confirmed (all previously marked unverified): `AWS::NoValue`
inside EMR `ConfigurationProperties` and inside Glue `DefaultArguments` drops
the key cleanly (the live cluster and job show no OpenLineage keys); Glue 5.0
honours the folded `--conf` string (the output files are `*.zstd.parquet`);
the Athena workgroup enforces the 10 GiB cutoff on engine v3; lifecycle rules
are attached; the EMR auto-termination policy is 3600s; the bootstrap
installed Python 3.11 with pandas 2.2.3 / pyarrow 16.1 while the default
stayed 3.9. The Glue job run on a 68,369-row test file: 68,367 rows written
as one zstd parquet file in `processed/page_type=1/date=2026-09-06/hour=10/`,
the two deliberately malformed rows in `quarantine/<run_id>/`, 75s
execution, single JSON log line with the counts.

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


### Update 2026-09-04 — Datasets, quality checks, runbook

`impression_pipeline` now declares `outlets` on shared `Dataset` objects
(`dags/datasets.py`: raw, processed, aggregated). A new dataset-triggered DAG,
`impression_quality_checks`, runs once per successful aggregation with two
independent tasks: **freshness** (newest `completed` status file younger than
`FRESHNESS_SLA_MINUTES`) and **volume** (this hour's `rows_written` from the
raw manifest vs the median of the same hour on previous days, plus a
quarantine-ratio ceiling). The checks are pure functions in
`dags/quality_checks.py` (14 tests, no Airflow import) and read the local
storage layout through a shared bind mount (`PIPELINE_DATA_DIR`). Datasets
were chosen over a second cron because the consumer then runs when data has
actually landed and a backfill of one hour re-triggers exactly that hour
downstream; their 2.x limit (table-level URI, no partition) is worked around
by reading the triggering run's logical date. `sla=None` is set on the mapped
pull task because Airflow 2.x rejects SLAs on mapped tasks at parse time; the
freshness check enforces it instead. `RUNBOOK.md` documents reprocessing one
hour, a range, one page_type, quarantine spikes, SLA misses, dataset
re-triggering and a post-reprocess checklist. Verified by `py_compile` on all
DAG files and the quality-check tests; Airflow itself is not installed in the
dev sandbox.

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


### Update 2026-09-04 — Schema Registry, Avro, upsert-kafka sink

- **Sink.** The `print` sink is replaced by an `upsert-kafka` table keyed by
  `(page_type, window_start, window_end)`: each revision the windowed
  aggregate emits becomes an upsert on that key and a retraction a tombstone,
  so a compacted topic holds exactly one current row per window and is
  consumable by anything (Flink, ClickHouse ReplacingMergeTree, ksqlDB).
  `CDC_SINK=print` keeps the debug path. The Flink image now actually ships
  `flink-sql-connector-kafka` and `flink-sql-avro-confluent-registry` (sha1
  checked); the base image has neither, so the previous job could not have
  planned a `'connector' = 'kafka'` table.
- **Schema Registry.** `debezium_deployment` adds Confluent Schema Registry
  and switches the connector to the Avro converter (Connect is built from a
  derived image because `debezium/connect:2.5` does not ship the Confluent
  converter, verified by listing the plugin dir). Compatibility is set
  explicitly to `BACKWARD` per subject at `register`: a consumer on the newest
  schema can read everything already in the topic, so consumers upgrade first.
- **Surviving `ALTER TABLE`.** `source_ddl(fmt="avro-confluent")` derives the
  Avro reader schema from the DDL, so a column added at the source is ignored
  and a column dropped or renamed resolves to `NULL` rather than failing.
  `./deploy.sh evolve` was run live: ADD / RENAME / DROP took the subject from
  version 1 to 4 with the connector task still RUNNING, and
  `evolve-incompatible` got the expected 409
  (`READER_FIELD_MISSING_DEFAULT_VALUE`). Written up with the captured output
  in `debezium_deployment/SCHEMA_EVOLUTION.md`, including a Debezium quirk
  found on the way (nullability comes from the live catalog, not the WAL
  position, so rapid DDL shows up as optional one version early).
- Verified: 12 pure SQL-builder tests, compose validation, the live registry
  run above. The Flink job's behaviour across the four versions follows from
  Avro resolution and was not re-run end to end in this session.

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


### Update 2026-09-04 — OpenLineage

One variable, `OPENLINEAGE_URL`, turns lineage on across every layer and
leaves every job untouched when unset:

- **Spark** (`utils/session.py::openlineage_conf`): the OpenLineage Spark
  listener (`io.openlineage:openlineage-spark_2.12:1.53.0`, verified on Maven
  Central) with http transport and a namespace; reports each read/write path
  (`file`/`s3://bucket` namespaces) with schema and output-statistics facets.
- **Airflow**: `apache-airflow-providers-openlineage` (2.20.x, Airflow >=
  2.11) in the image; gated by `AIRFLOW__OPENLINEAGE__DISABLED`. The
  `SparkSubmitOperator` extractor passes the parent-run facet so Spark runs
  nest under their task.
- **Glue / EMR**: the same listener via the template's `OpenLineageUrl`
  parameter (`--extra-jars` from the deployment bucket for Glue,
  `spark.jars.packages` for EMR).
- **dbt**: `openlineage-dbt`'s `dbt-ol` wrapper, switched in by
  `dbt_deployment/deploy.sh` when the URL is set.
- **Athena**: no native integration; `lineage_deployment/LINEAGE.md` gives
  the `openlineage-python` pattern for emitting from the Step Function /
  Lambda and names datasets by Glue catalog table so the graph joins.
- **Backend**: `lineage_deployment/` runs Marquez (API + UI) on the shared
  network; `deploy.sh smoke` posts a synthetic run to prove the endpoint.

Verified: pure config tests in `tests/test_session.py`, compose validation for
`lineage_deployment`, Marquez smoke event. Not verified: an end-to-end graph
with all emitters live at once.

---

## #3 — Idempotency, reliability & data quality

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


### Update 2026-09-04 — Transactional landing, volume checks

- **Transactional coupling** (`utils/landing.py`). The raw gzip is staged
  under `<table>/_staging/<job_id>/`, the table is written from the staged
  copy, and only then is the file moved to its partition path (atomic rename
  locally, single-object PUT on S3) and a `_manifest.json` published last
  with row counts and the sha256 of the bytes. A failure anywhere inside
  deletes the staged file and leaves the previous successful landing
  untouched; a stale staging left by a crash is discarded on the next entry.
  Invariant for consumers: a manifest exists only if raw file and table
  partition were written from the same bytes. The residual windows (crash
  between table write and promote, or between promote and manifest) are
  repaired by the idempotent re-run that already existed. Storage adapters
  gained five small primitives (exists / move / delete / write manifest / read
  manifest) for local, S3 and DBFS so the transaction logic lives in one place.
- **Volume / anomaly checks** (`quality.check_volume`,
  `check_quarantine_ratio`). Before the table write the batch is compared to
  the median `rows_written` of the same (page_type, hour) on the previous
  seven days, read from those days' manifests. Median, not mean, so an earlier
  outage does not mask a repeat; zero rows is always an anomaly. A volume
  anomaly is logged as a warning by default (`VOLUME_CHECK_MODE=fail` aborts
  the landing); a quarantine share above 1% always aborts, because that means
  the source changed shape and the rest of the batch is not to be trusted.
- **Freshness SLAs** are enforced by the orchestrator, which owns the clock:
  `impression_quality_checks.check_freshness` (Airflow, see #4) and dbt
  `source freshness` on `raw.impressions.loaded_at` (see #1). The Spark side
  supplies the timestamps (`landed_at`, `committed_at` in the manifest).

Verification: `tests/test_landing.py` (commit, abort, stale staging, re-run,
no clobber of the previous landing), `tests/test_api_pull.py` end-to-end
through `main()` with the API mocked (lands raw + table + manifest together;
a failing table write leaves no orphan and marks the status failed; a 50%
quarantine share aborts; a volume collapse warns by default and fails in
`fail` mode), `tests/test_quality.py` for the check functions.

---

## #1 — Schema-contract enforcement & quarantine

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


### Update 2026-09-04 — dbt contracts and source freshness; registry moved to #5

- **Model contracts.** Every dbt model (`stg_impressions`,
  `int_impressions_aggregated`, the four analysis tables) now has
  `contract: enforced: true` with a `data_type` per column. dbt checks the
  compiled SQL's columns against the contract before building, so a retyped or
  dropped column fails the run with a diff instead of silently changing the
  dashboards' tables. Verified against a real PostgreSQL: `dbt run` builds all
  six models with contracts on.
- **Source freshness.** `raw.impressions` gains `loaded_at TIMESTAMPTZ`,
  stamped once per (page_type, date, hour) batch by `load_data.py`, declared
  as the source's `loaded_at_field` with `warn_after 2h / error_after 24h`
  (hourly pipeline plus a retry window). `./deploy.sh source-freshness`
  passes on a freshly loaded table. `init_db.sql` only runs on a fresh
  volume, so `migrations/001_add_loaded_at.sql` and `./deploy.sh migrate`
  bring existing databases forward.
- Rows whose source date fails the safe cast are excluded from
  `int_impressions_aggregated` (they were surfacing as a NULL `event_date`
  partition and failing `hourly_traffic`'s not_null test); the warn-level
  test on `stg_impressions` still reports them.
- The Schema Registry follow-ups are done and written up under #5 and in
  `debezium_deployment/SCHEMA_EVOLUTION.md`.

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


### Update 2026-09-04 — Salting wired in, thresholds tuned

- `aggregation.py` gains the pipeline's hot-key path as an explicit,
  optional user-dimension enrichment (`--user-dimension`,
  `--join-strategy broadcast|salted`) implementing case 01's order of attack:
  **broadcast** by default with a `broadcast()` hint (so the plan does not
  depend on Spark's size estimate, which is what produced the sort-merge join
  on the skewed key in the first place), **AQE** at the session level, and
  **salted** only on request via a new `salted_join()` helper that is a
  drop-in for an equi-join (no salt columns or duplicated key in the output;
  inner and left).
- `utils/session.py` thresholds: `autoBroadcastJoinThreshold` and its AQE
  twin raised from 10MB to 64MB (Spark's estimate for a filtered/aggregated
  side is routinely off by 10x; 64MB estimated is ~200-250MB as an in-memory
  hash relation, still an order of magnitude below anything that should be
  salted); `skewJoin.skewedPartitionThresholdInBytes` and
  `advisoryPartitionSizeInBytes` lowered from 256MB to 64MB so AQE actually
  splits mid-sized skew (case 01 measured that it split nothing at the
  default).
- **Measured plan shapes** (`debugging/production_metrics.py`, in
  `DEBUGGING.md`): the plain join under case-01 conditions is a
  `SortMergeJoin` with *both* sides hashed on `user_id` and, because `user_id`
  prefixes the aggregation key, the aggregation inherits the skewed
  partitioning too (2 exchanges, all skewed). Broadcast leaves only the
  aggregation's own exchanges on the full, unskewed key. Salted shuffles twice
  on `salted_key`. Tests pin all three plans and that the strategies agree on
  the result.
- **Measured on EMR 7.13 (2026-09-06)** with `debugging/cluster_measure.py`
  (numbers in DEBUGGING.md, "Cluster re-measurement"). At 20M rows the
  production plain join had one task 677x the median with 189MB of spill
  (60s); the hinted broadcast ran evenly (20.5s); salted ran evenly and
  fastest (15.0s). Case 01's 10MB threshold did *not* broadcast on EMR (the
  operator was `ShuffledHashJoin` in every leg, EMR's `preferSortMergeJoin`
  being off), which is the strongest argument yet for the `broadcast()`
  hint the pipeline uses. AQE coalesced but never split: the hot partition
  was 24.7MB, under even the lowered 64MB threshold at this row count.
