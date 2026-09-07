# Airflow Deployment

Deploys Apache Airflow 2.11.1 locally using Docker. The deployment includes a webserver, scheduler, and PostgreSQL metadata database.

## Architecture

- **Dockerfile**: Builds a custom Airflow image with Java 17, PySpark 3.5.4, and providers for Apache Spark and AWS.
- **docker-compose.yaml**: Orchestrates the following services:
  - `postgres` - PostgreSQL 15 metadata database
  - `airflow-init` - One-time database migration and admin user creation
  - `airflow-webserver` - Airflow web UI (port 8080)
  - `airflow-scheduler` - DAG scheduler
- **dags/**: Contains two DAG workflows:
  - `hello_world_local_spark.py` - Runs a hello world Spark job on the local Spark cluster
  - `hello_world_emr_spark.py` - Runs a hello world Spark job on AWS EMR
- **dags/spark_scripts/**: Contains the Spark scripts executed by the DAGs

## Prerequisites

- Docker and Docker Compose
- A `.env` file in the project root with required environment variables

## How to Run

```bash
# Start all Airflow services
./deploy.sh up

# Stop all services
./deploy.sh down

# Restart all services
./deploy.sh restart

# Check service status
./deploy.sh status

# Tail logs
./deploy.sh logs
```

Once running, access the Airflow UI at http://localhost:8080 (default credentials: admin/admin).

## Network

Creates a shared Docker network `data-processing-network` that the local Spark cluster and web server connect to.

## Data-aware scheduling and quality checks

`dags/datasets.py` defines the `Dataset` URIs the pipeline produces
(`raw/impressions`, `processed/impressions`, `output/impressions_aggregated`).
`impression_pipeline` declares them as task `outlets`;
`impression_quality_checks` is scheduled on the aggregated dataset and runs
once per successful aggregation with two independent tasks:

| task | check | env |
| --- | --- | --- |
| `check_freshness` | newest `completed` status file younger than the SLA | `FRESHNESS_SLA_MINUTES` (90) |
| `check_volume` | this hour's `rows_written` vs the median of the same hour on previous days; quarantine ratio ceiling | `VOLUME_BASELINE_DAYS` (7), `VOLUME_MIN_RATIO` (0.5), `VOLUME_MAX_RATIO` (2.0), `MAX_QUARANTINE_RATIO` (0.01) |

Both read the local storage layout the Spark jobs write (status files and
raw manifests) under `PIPELINE_DATA_DIR=/opt/airflow/data`, which
`docker-compose.yaml` bind-mounts from `../data`. In `SPARK_MODE=aws` the
tasks skip. The pure check functions are in `dags/quality_checks.py` and
tested without Airflow:

```bash
uv run --no-project --with pytest pytest tests -q
```

A downstream consumer (for example a dbt refresh) subscribes the same way:

```python
from datasets import IMPRESSIONS_AGGREGATED

with DAG("dbt_refresh", schedule=[IMPRESSIONS_AGGREGATED], ...):
    BashOperator(task_id="dbt_build", bash_command="dbt build ...")
```

Reprocessing procedures (one hour, a range, one page_type, quarantine spikes,
SLA misses, dataset re-triggering) are in `RUNBOOK.md`.

## OpenLineage

The image includes `apache-airflow-providers-openlineage`. It is off by
default; set in `../.env`:

```
OPENLINEAGE_DISABLED=false
OPENLINEAGE_URL=http://marquez-api:5000
OPENLINEAGE_NAMESPACE=data_processing
```

and start Marquez with `../lineage_deployment/deploy.sh up`. Every task run
is then reported, and `SparkSubmitOperator` passes the parent-run facet so the
Spark job's own lineage (emitted by the listener configured in
`spark_applications/utils/session.py`) nests under the Airflow task. See
`../lineage_deployment/LINEAGE.md`.
