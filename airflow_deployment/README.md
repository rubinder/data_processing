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
