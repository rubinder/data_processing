# data_processing

A multi-platform data engineering repository: the same simulated event data,
processed through batch, streaming, CDC, and OLAP stacks, each deployable
locally with Docker and (where relevant) to AWS or Databricks.

---

## Featured: real-time analytics with ClickHouse and Flink

**[`realtime_analytics/`](realtime_analytics/) — Kafka → PyFlink → ClickHouse
pipeline for AI agent conversation events, with a documented ClickHouse tuning
journey taking the tenant dashboard query from 181 ms to 2.4 ms (75×) and the
platform-wide query from 286 ms to 2.0 ms (143×) — reading 91 rows instead of
12,000,000 — and a FastAPI service serving every endpoint at a p95 of 1.2 ms
against a 100 ms target.**

Every number is produced by a benchmark script in the repo, verified to return
identical results at every stage, and reproducible on a laptop with no server
required. The writeup covers what did **not** work as much as what did:
a skipping index that pruned nothing, an aggregate projection the planner never
selected, a materialized view that was slower than the scan it replaced until
its index granularity was fixed, and a partitioning scheme the measurements
forced a rewrite of.

→ **[Read the tuning writeup](realtime_analytics/README.md)** ·
**[How each optimization works](realtime_analytics/OPTIMIZATIONS.md)**

---

## Modules

| Directory | What it is |
| --- | --- |
| [`realtime_analytics/`](realtime_analytics/) | **Kafka + PyFlink + ClickHouse** real-time analytics for AI agent conversation events, with a measured schema-tuning writeup and a sub-100 ms FastAPI serving layer |
| [`spark_applications/`](spark_applications/) | PySpark jobs: API ingestion with transactional raw landing and manifests, volume/anomaly checks, partitioned writes, aggregation with a broadcast-first / salted hot-key join, seven worked debugging cases, plus `DECISIONS.md` and `DEBUGGING.md` |
| [`flink_applications/`](flink_applications/) | PyFlink jobs, including a Debezium CDC consumer (Avro via Schema Registry, exactly-once, event-time watermarks) writing to an upsert-kafka sink |
| [`airflow_deployment/`](airflow_deployment/) | Airflow 2 via Docker: backfillable hourly DAG, dynamic task mapping, Dataset-triggered freshness/volume checks, OpenLineage provider, and a reprocessing `RUNBOOK.md` |
| [`aws_deployment/`](aws_deployment/) | CloudFormation: EMR (Spot + managed scaling), S3 lifecycle tiers, Lambda, Step Functions, Glue 5.0, Athena workgroup with a scanned-bytes cutoff, DynamoDB; cost narrative in `FINOPS.md` |
| [`databricks_deployment/`](databricks_deployment/) | Databricks workflow configuration and deployment script |
| [`local_spark_deployment/`](local_spark_deployment/) | Local Spark cluster wired to the Airflow deployment |
| [`flink_deployment/`](flink_deployment/) | Local Flink JobManager/TaskManager cluster |
| [`debezium_deployment/`](debezium_deployment/) | Postgres → Debezium → Kafka CDC stack with Confluent Schema Registry (BACKWARD compatibility) and a measured `ALTER TABLE` walkthrough in `SCHEMA_EVOLUTION.md` |
| [`dbt_deployment/`](dbt_deployment/) | dbt + PostgreSQL: staging, intermediate, and four analytical models with enforced contracts, tests, and source freshness |
| [`clickhouse_deployment/`](clickhouse_deployment/) | Distributed ClickHouse: 2 shards + keeper, Distributed over ReplicatedMergeTree |
| [`iceberg_deployment/`](iceberg_deployment/) | **Apache Iceberg** lakehouse tables on Spark 3.5: field-ID schema evolution, partition evolution, time travel and rollback, MERGE upserts, and compaction/expiry maintenance — 22 tests against real tables |
| [`duckdb_deployment/`](duckdb_deployment/) | DuckDB as an embedded, application-level OLAP engine behind FastAPI |
| [`lineage_deployment/`](lineage_deployment/) | Marquez (OpenLineage backend + UI) and `LINEAGE.md`, the lineage story across Spark, S3, Glue, Athena and dbt |
| [`web_server_code/`](web_server_code/) | FastAPI service generating the simulated impression data everything else consumes |
| [`web_server_local/`](web_server_local/), [`web_server_aws/`](web_server_aws/) | Local and AWS deployments of that service |

## Conventions

- Python 3.10, PEP 8, dependencies managed with `uv`
- Spark 3.5, Airflow 2.11.1, Flink 1.18.1, Databricks 17.3
- Each module has its own `README.md`, `deploy.sh`, and tests
- Containers share an external Docker network, `data-processing-network`
- Lineage is opt-in everywhere through one variable, `OPENLINEAGE_URL`

See [`Planning.md`](Planning.md) for the layout rationale and
[`Tasks.md`](Tasks.md) for status.
