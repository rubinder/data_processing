Complete the following under different folders
    - [x] AWS Deployment under aws_deployment folder,
        - [x] Cloudformation template that has ,IAM roles, EMR code, S3 bucket for data arriving, S3 event for triggering a Lambda which triggers a step function, AWS Glue which catalogs the table schema, Step Function code which runs an AWS Batch job to check the encoding of the file then a subsequent Glue Crawler to check the schema, then a Glue ETL job to ETL the data in parquet and in Athena, DynamoDB table infrastructure that has an id key
        - [x] shell script to zip the code mentioned above upload it to AWS S3  and it runs python script which creates the resources in the deployment zip
    - [x] Airflow Deployment  under airflow_deployment
        - [x] Dockerfile containing all the necessary components for an airflow infrastructure deployment
        - [x] Runs a docker-compose command in a shell script to deploy it
        - [x] Connects to AWS mentioned above
        - [x] Connects to Spark Cluster mentioned below
        - [x] It is configured with 2 jobs
            - [x] One is a local spark job, the code that is ran is a "hello world" example
            - [x] One is a AWS Spark job , the code that is ran is a "hello world" example
    - [x] Local Spark Deployment under local_spark_deployment
        - [x] Dockerfile containing spark cluster deployment and it connects with the Airflow Cluster mentioned above
    - [x] Spark Applications under spark_applications
        - [x] A Spark job that runs "hello world"
        - [x] A Spark job using a salt on an ID column is a common technique to mitigate data skew (or "hot keys") during wide transformations like joins or aggregations, where a specific ID value is overrepresented. By appending a random number ("salt") to the key, the skewed data is distributed across multiple partitions,  Step 1: Add a Salt Column Create a new column with a random integer (e.g., 0-9) and combine it with your ID column. Step 2: Handle the "Other" Side (For Joins) If joining with a smaller, unskewed DataFrame, that DataFrame must be exploded to match the new salted keys., Step 3: Remove the Salt After the aggregation or join, remove the salt columns to restore the original data structure.
        - [x] A Spark job that pull from an API (which varies but is related to the code under web_server_code) and updates a table / DynamoDB / delta table of pull status depending on mode (local vs AWS vs Databricks) and saves the data pull from the API to s3 or local depending on the mode and then reads the file and saves it to a table (delta table for databricks mode, athena for AWS and parquet for local) and partitions it based on page_type, date, hour
        - [x] A Spark job that reads data for a given date and hour aggregates data based on user_id/impression_id/page_type
        - [x] unit tests are under a folder /tests folder that are not packaged with the application code
    -[x] Databricks Deployment
        - [x] Contains sample workflow configuration that runs a 3 tasks
        - [x] Shell script to deploy workflow configuration to Databricks account
    -[x] Under web_server_code
        - [x] FastAPI web server that serves a csv gzip file at the path of /impression, the parameters for the request should be page_type, date, hour. The file generated should reflect the parameters in the request for page_type, date, hour. The contains a header of user_id, impression_id, page_type, date, hour, min, second, event_type. user_id is a uuid that represents user activity, there should be a minimum of 1,000 user_id in a file. The impression_id is a uuid that should be the same across user_id/page_type/event_type/date/hour/min combinations. event_type is an enumeration of a, b, c, d, e, f . The enumeration is in alphabetical and chronological order, the file should only generate an event_type for the same combination of impression_id/user_id/page_type/date/hour/min if previous letters have come up, i.e. if the event_type f occurred then event type a,b,c,d,e also occurred for the same impression_id/user_id/page_type/date/hour/min at earlier seconds. Page type is an enumeration of 1,2,3. The file should have 10000 to 100000 impressions. For page_type 1 about 10% of impressions reach event_type d 0% reach event_type e 0% reach event_type f, For page_type 2  about 30% of impressions reach event_type d about 10% reach event_type e 0% reach event_type f, For page_type 3 about 50% of impressions reach event_type d about 20% reach event_type e about 10% reach event_type f
    -[x] Under web_server_local
        - [x] Should deploy the code under web_server_code locally
    - [x] Under web_server_aws
        - [x] Should deploy the code under web_server_code to AWS using a shell script
    - [x] Add a README.md to each directory describing how to run scripts, launch Dockerfiles, and what each directory does
    - [x] dbt Deployment under dbt_deployment
        - [x] Dockerfile and docker-compose.yaml deploying PostgreSQL 15 and dbt-postgres locally with Docker
        - [x] Shell script (deploy.sh) to manage lifecycle (up, down, restart, status, logs, run, seed, test, load-data)
        - [x] init_db.sql to create raw schema and impressions table
        - [x] load_data.py script to pull impression data from web server API into PostgreSQL
        - [x] dbt project with staging model (stg_impressions) that cleans raw data
        - [x] Intermediate model (int_impressions_aggregated) mirroring the Spark aggregation job
        - [x] Analysis model: funnel_analysis - conversion rates by page type through event stages a-f
        - [x] Analysis model: page_type_summary - high-level stats per page type (volume, users, funnel depth, conversion rates)
        - [x] Analysis model: user_engagement - user-level metrics (impressions, funnel depth, most engaged page type)
        - [x] Analysis model: hourly_traffic - hourly traffic patterns by page type
        - [x] Schema tests validating not_null, accepted_values, and uniqueness constraints
        - [x] Connects to shared data-processing-network
        - [x] README.md describing how to deploy and run
    - [x] Flink Applications under flink_applications
        - [x] PyFlink hello world batch job using Table API
        - [x] pyproject.toml with apache-flink 1.18.1 dependency
        - [x] unit tests under /tests folder
    - [x] Flink Deployment under flink_deployment
        - [x] Dockerfile with Flink 1.18.1 and PyFlink installed
        - [x] docker-compose.yaml with JobManager and TaskManager, connects to data-processing-network
        - [x] docker-compose.local.yaml for standalone mode
        - [x] deploy.sh script (up, down, restart, status, logs, submit)
        - [x] README.md describing how to deploy and run
    - [x] Debezium CDC Deployment under debezium_deployment
        - [x] docker-compose.yaml with Zookeeper, Kafka, PostgreSQL (wal_level=logical), Debezium Connect 2.5, Kafka UI
        - [x] docker-compose.local.yaml for standalone mode
        - [x] init_db.sql with impressions schema (events, users, pull_status tables) with seed data
        - [x] connectors/postgres-source.json Debezium connector config capturing impressions schema
        - [x] sample_changes.sql with sample inserts, updates, deletes for generating CDC events
        - [x] deploy.sh script (up, down, restart, status, logs, register, connectors, topics, consume, exec-sql)
        - [x] Connects to shared data-processing-network
        - [x] README.md describing how to deploy and run
    - [x] DuckDB Deployment under duckdb_deployment (embedded, application-level OLAP)
        - [x] FastAPI service embedding in-process DuckDB
        - [x] Loader pulls impression csv.gz from the web server API into a DuckDB table
        - [x] Analytical endpoints mirroring the dbt analyses: funnel_analysis, page_type_summary, user_engagement, hourly_traffic
        - [x] Dockerfile, docker-compose.yaml (external data-processing-network), deploy.sh (up|down|restart|status|logs|load-data|query)
        - [x] uv pyproject.toml, README.md
        - [x] pytest tests: real in-process DuckDB queries + FastAPI TestClient (11 passing)
    - [x] ClickHouse Deployment under clickhouse_deployment (distributed OLAP, concurrent queries across nodes)
        - [x] Multi-node cluster: 2 shards + clickhouse-keeper, Distributed table over ReplicatedMergeTree
        - [x] cluster.xml / keeper.xml / macros config, init/schema.sql (ON CLUSTER)
        - [x] clickhouse-connect loader pulls impression csv.gz from the web server API
        - [x] Four analytical SQL queries mirroring the dbt analyses (templated table name)
        - [x] docker-compose.yaml (external data-processing-network), deploy.sh (up|down|restart|status|logs|schema|load-data|query), uv pyproject.toml, README.md
        - [x] pytest tests: analytical SQL executed against embedded ClickHouse (chdb) + mocked-request loader tests (6 passing)

    - [x] Real-time Analytics under realtime_analytics (Kafka + PyFlink + ClickHouse + FastAPI)
        - [x] AI agent conversation event model (conversation_started, message_sent, agent_response, resolution, escalation) across chat/voice/email/sms/api channels, multi-tenant by account_id
        - [x] Kafka producer keyed by conversation_id (per-conversation ordering) with deliberate late-event injection for watermark testing
        - [x] Simple Python Kafka -> ClickHouse consumer: batched inserts, flush deadline, offsets committed after insert (at-least-once)
        - [x] PyFlink streaming job: Kafka source -> 1-minute event-time tumbling rollup -> Kafka, verified running on the cluster (1,321 windows emitted and ingested, all minute-aligned, counters internally consistent)
        - [x] ClickHouse Kafka table engine + materialized views for ingest (20_kafka_ingest.sql raw path, 21_flink_rollup_ingest.sql for the Flink rollup); chosen because flink-connector-jdbc ships no ClickHouse dialect at any released version, so a JDBC sink cannot be built without custom Java
        - [x] Full stack validated end-to-end on Docker: 106,106 events Kafka -> ClickHouse with exact materialized-view reconciliation, API p95 8.47ms over HTTP against the 100ms SLO
        - [x] Documented Flink tuning: parallelism vs partition count, watermark/out-of-orderness, checkpoint interval + min-pause + unaligned checkpoints, RocksDB vs heap state, mini-batch and two-phase aggregation for hot keys, source idle timeout
        - [x] Staged ClickHouse schemas measured one variable at a time: 00 naive, 01 types+codecs, 02 sorting key, 03 partitioning, 04 skipping indexes, 05 materialized views, 06 projections alternative
        - [x] Production schema (10_production.sql): ReplacingMergeTree for at-least-once dedup, monthly partitioning, bloom filters, TTL 90d raw / 730d aggregates, three materialized views
        - [x] benchmarks/bench_clickhouse.py: interleaved warm-cache rounds, engine-reported rows_read/bytes_read, every stage verified to match the naive baseline before a speedup is reported
        - [x] benchmarks/bench_partitioning.py: partition granularity (none/monthly/weekly/daily) measured for pruning benefit vs per-part cost
        - [x] benchmarks/bench_api.py: end-to-end API p50/p95/p99 against the 100ms SLO, runnable without Docker
        - [x] FastAPI service reading materialized views, bound query parameters, capped lookback windows, Prometheus /metrics
        - [x] docker-compose (Kafka KRaft, ClickHouse, Flink JM/TM, API), Dockerfile, Dockerfile.flink with connector JARs, deploy.sh, .env.example, uv pyproject
        - [x] pytest suite against real embedded ClickHouse (chdb): schema-variant equality, index pruning, MV correctness, ReplacingMergeTree dedup, API behaviour, Flink SQL/tuning
        - [x] Self-contained README documenting the measured tuning journey, ClickHouse vs Redshift reasoning, partitioning and materialized view strategy, SLOs/runbook, and the negative results

    - [x] Iceberg Deployment under iceberg_deployment (Apache Iceberg table format on Spark 3.5)
        - [x] session.py with two catalogs: filesystem (no services, used by tests) and REST-on-S3/MinIO (production shape); pins PYSPARK_PYTHON to sys.executable so venv driver/worker versions cannot diverge
        - [x] Iceberg table over the impression event model with hidden partitioning (days(event_ts), page_type) and format-version 2 merge-on-read
        - [x] schema_evolution.py: documented compatibility policy (safe: add/rename/drop/reorder/widen; rejected: narrowing, nullable->required, unrelated type change) plus partition-spec evolution
        - [x] time_travel.py: snapshot listing, VERSION/TIMESTAMP AS OF, rollback_to_snapshot, cherrypick for write-audit-publish
        - [x] upserts.py: MERGE INTO composed from the live schema (survives renames), idempotent replay, row-level DELETE
        - [x] maintenance.py: rewrite_data_files compaction, rewrite_manifests, expire_snapshots with a retain floor, remove_orphan_files
        - [x] demo.py six-step printed walkthrough; docker-compose (Iceberg REST + MinIO), deploy.sh, README, uv pyproject
        - [x] 22 pytest tests against real Iceberg tables: rename preserves field ID and data, drop-then-re-add does not resurrect values, narrowing rejected, evolution rewrites zero data files, partition specs coexist, rollback restores state, MERGE replay idempotent, compaction reduces files while preserving records

---

## Staff-Level Hardening Backlog

The work above demonstrates *breadth* (11 deployment targets). The items below
exist to demonstrate *depth of judgment under change, scale, and failure* —
schema evolution, ETL performance, idempotency, and reliability. Each item
should ship with a short written narrative of the issue and its resolution
(a `DECISIONS.md` / incident-style writeup) rather than just code.

### #1 — Schema evolution (done, see spark_applications/DECISIONS.md and debezium_deployment/SCHEMA_EVOLUTION.md)
- [x] Replace `inferSchema=True` with explicit `StructType` schemas for all CSV reads (`utils/schema.py`, done under #2)
- [x] Add a quarantine / dead-letter path for records that do not conform to the contract (`utils/quality.py`, `write_quarantine`)
- [x] dbt: use safe casts (`macros/safe_casts.sql`) so one malformed row doesn't fail the model; warn-level not_null on `event_date`
- [x] Introduce a Schema Registry (Avro/Protobuf) for the Debezium → Kafka path with an explicit compatibility mode — Confluent Schema Registry + Avro converter in `debezium_deployment/` (Connect built from a derived image; upstream ships no Confluent converter), `BACKWARD` set per subject at register; `deploy.sh schemas|compat|evolve|evolve-incompatible`
- [x] Wire Debezium → Schema Registry → Flink and demonstrate surviving a source `ALTER TABLE` — `cdc_sql.source_ddl(fmt="avro-confluent")`; `./deploy.sh evolve` run live 2026-09-04: ADD/RENAME/DROP took the subject from v1 to v4 with the connector RUNNING, `evolve-incompatible` got the 409 (`debezium_deployment/SCHEMA_EVOLUTION.md`). Flink job behaviour across versions follows from Avro resolution; not re-run end to end
- [x] Adopt Delta/Iceberg with column mapping + a deliberate schema-evolution policy for evolving tables — done in `iceberg_deployment/` (field-ID based evolution, documented safe/unsafe operation policy, 22 tests asserting old data still reads correctly after rename/drop/re-add)
- [x] dbt: model contracts on all six models + `dbt source freshness` on `raw.impressions.loaded_at` (warn 2h / error 24h); `migrations/001_add_loaded_at.sql` + `deploy.sh migrate`; verified against PostgreSQL: run 6/6, test 20 pass + 1 expected warn, freshness pass

### #2 — ETL performance & correctness (done locally; cluster re-measurement outstanding — see spark_applications/DECISIONS.md)
- [x] `api_pull.py`: stop decompressing in the driver; let Spark read gzip CSV directly (distributed)
- [x] Use explicit schema instead of `inferSchema` (double scan + non-deterministic types) — `utils/schema.py`
- [x] Replace full-table `mode("overwrite")` with idempotent partition replacement (`partitionOverwriteMode=dynamic`) so reprocessing one hour does not nuke history
- [x] Remove `df.count()` progress-logging actions that force recomputation
- [x] Add `repartition` before partitioned writes to avoid the small-files explosion at hour grain (`_compact`)
- [x] `aggregation.py`: filter on partition columns for pruning + fix `hour` string/int typing
- [x] Enable AQE / adaptive skew-join at the session level (`utils/session.py`)
- [x] Worked debugging cases with before/after query plans and measured evidence — `spark_applications/debugging/` + `DEBUGGING.md` (see DECISIONS.md #7). Covers skew/join-strategy, driver OOM, partition pruning, small files, `AMBIGUOUS_REFERENCE`, `PythonException`, and Python UDF → pandas UDF. 56 tests.
- [x] Reusable plan-analysis helpers (`debugging/explain_tools.py`): captures the post-AQE *final* plan that `explain()` does not show; parses join strategies, exchange counts + partitioning schemes, `PartitionFilters`/`PushedFilters`, Python-eval operators, runtime scan metrics
- [x] Wire salting into the real hot-key path in `aggregation.py` (`--user-dimension`, `--join-strategy broadcast|salted`; broadcast-hinted by default, `salted_join()` helper) and tune thresholds in `utils/session.py` (broadcast 10MB → 64MB, AQE skew threshold 256MB → 64MB); plan shapes measured in `debugging/production_metrics.py`, written up in DEBUGGING.md
- [x] Re-measure debugging cases 01 and 07 on a real cluster — done 2026-09-06 on the stack's EMR 7.13 cluster with `debugging/cluster_measure.py` (REST-API task metrics); case 01: 210x shuffle-read skew, 10MB threshold did not broadcast on EMR, AQE coalesced but did not split; case 07: native 8x the Python UDF, pandas UDF 1.6x. DEBUGGING.md "Cluster re-measurement"
- [x] Before/after metrics for the production jobs — plan-level locally (`production_metrics.py`) and runtime on EMR at 20M rows: plain join 59.7s with a 677x straggler and 189MB spill, broadcast 20.5s even, salted 15.0s even (DEBUGGING.md)

### #3 — Idempotency, reliability, data quality (done, see spark_applications/DECISIONS.md)
- [x] Retry + backoff on the API pull (`fetch_impression_data`)
- [x] Deduplication for at-least-once CDC delivery (`quality.deduplicate`)
- [x] Row-count reconciliation read → written + quarantined (`quality.reconcile_counts`, wired into `api_pull`)
- [x] Transactionally couple raw-file write and table write — `utils/landing.py` (stage → write table → promote → publish `_manifest.json` last; abort deletes the staged file); storage adapters gained exists/move/delete/manifest primitives for local, S3, DBFS; end-to-end tests through `api_pull.main`
- [x] Volume/anomaly checks; freshness SLAs — `quality.check_volume` (median of same hour on previous days, from manifests; warn by default, `VOLUME_CHECK_MODE=fail`), `check_quarantine_ratio` (>1% aborts); freshness enforced by Airflow `impression_quality_checks.check_freshness` and dbt `source freshness`

### #4 — Orchestration maturity (Airflow) (done, see spark_applications/DECISIONS.md and airflow_deployment/RUNBOOK.md)
- [x] Real backfillable DAG (`dags/impression_pipeline.py`): hourly schedule, catchup, idempotent date/hour params, retries+backoff, SLA, failure callback
- [x] Dynamic task mapping over page_types
- [x] Datasets / data-aware scheduling (`dags/datasets.py`, outlets on the pipeline, dataset-triggered `impression_quality_checks` with freshness + volume tasks, 14 tests); `airflow_deployment/RUNBOOK.md` for reprocessing

### #5 — Streaming / CDC (connect the deployed pieces) (done, see spark_applications/DECISIONS.md)
- [x] Flink job that consumes the Debezium topics (`flink_applications/cdc_impressions.py`)
- [x] Checkpointing (exactly-once), event-time watermarks, CDC delete handling, windowed aggregation
- [x] Schema registry on the Debezium → Flink path (see #1) and an `upsert-kafka` sink keyed by (page_type, window) instead of the print sink; Flink image now ships the Kafka + Avro-registry jars

### #6 — Observability, lineage, FinOps (done, see spark_applications/DECISIONS.md #6/#8, lineage_deployment/LINEAGE.md, aws_deployment/FINOPS.md)
- [x] Replace `print()` with structured logging; emit metrics (rows in/out, bytes) — `utils/observability.py`
- [x] OpenLineage across Spark → S3 → Glue → Athena → dbt — Spark listener in `utils/session.py`, Airflow provider, Glue/EMR template parameter, `dbt-ol`, Marquez in `lineage_deployment/`; Athena documented as manual emission (`lineage_deployment/LINEAGE.md`). All gated on `OPENLINEAGE_URL`
- [x] Cost story — `aws_deployment/FINOPS.md` + template changes: zstd parquet everywhere, S3 lifecycle tiers/expiry, Athena workgroup with bytes-scanned cutoff, Glue 5.0 auto-scaling with timeout, EMR Spot task group + managed scaling + idle termination, cost-allocation tags; cfn-lint clean. Nothing measured against a real bill yet

### Discovered 2026-09-04 (not yet done)
- [x] Run the cluster protocol (cases 01, 07, production before/after) — done, see above
- [x] Deploy the CloudFormation stack and verify what cfn-lint cannot — deployed 2026-09-06 (`data-processing-pipeline`, account 194611079924, us-east-1). `AWS::NoValue` in EMR properties and Glue arguments drops keys cleanly; Glue 5.0 honours the folded `--conf` (zstd output); Athena cutoff enforced. Seven pre-existing defects found and fixed on the way (DECISIONS.md #8). Full S3 → Lambda → Step Function → Batch → Crawler → Glue run: `ExecutionSucceeded`. The OpenLineage jar on Glue remains untested (lineage was off for the deployment)
- [ ] Run the Flink CDC job end to end against the evolved topic (versions 1-4) and record the NULL-resolution behaviour `SCHEMA_EVOLUTION.md` derives from Avro rules
- [ ] `impression_quality_checks` reads the local layout only; add boto3 readers for DynamoDB status + S3 manifests so it works in `SPARK_MODE=aws`
- [ ] Emit Athena lineage from the Step Function / Lambda (`LINEAGE.md` has the `openlineage-python` pattern) — currently the only manual hop
- [ ] `flink_applications/tests/test_hello_world.py` asserts on captured stdout, but the Table API `print()` writes from the JVM and pytest's capsys does not see it; make the test assert on a collected result instead
- [ ] The Glue crawler catalogues the whole landing bucket as one table named after the bucket with `partition_0`/`partition_1` for the `raw/impressions` prefix; point it at `s3://<landing>/raw/impressions/` (and the ETL output at `processed/`) so the table is named and partitioned meaningfully
- [ ] EMR: the stack's `spark.jars.packages` OpenLineage line duplicates the `datazone-openlineage-spark` jar EMR 7.13 already ships on the classpath (see `lineage_deployment/LINEAGE.md`); drop the Maven line on EMR or pin to the shipped version, then test lineage on Glue/EMR with `OPENLINEAGE_URL` set
- [ ] Deployed stack left running after verification: the EMR cluster self-terminates after 1h idle (`AutoTerminationPolicy`); everything else idles at ~$0. `aws cloudformation delete-stack --stack-name data-processing-pipeline` (empty the landing/processed buckets first) removes it
