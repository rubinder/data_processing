# Flink Deployment

Local Apache Flink cluster deployment using Docker.

## Prerequisites

- Docker and Docker Compose
- For shared network mode: the `data-processing-network` Docker network (created by `airflow_deployment`)

## Quick Start

```bash
# Standalone mode (no external network dependency)
./deploy.sh up local

# Shared network mode (connects to Airflow network)
./deploy.sh up
```

## Commands

| Command   | Description                                      |
|-----------|--------------------------------------------------|
| `up`      | Build and start Flink JobManager + TaskManager   |
| `down`    | Stop and remove all Flink services               |
| `restart` | Restart all Flink services                       |
| `status`  | Show service status                              |
| `logs`    | Tail logs from all services                      |
| `submit`  | Submit a PyFlink job to the cluster              |

## Submitting a Job

```bash
./deploy.sh submit /opt/flink-apps/hello_world.py
```

## Web UI

Flink Web UI is available at [http://localhost:8082](http://localhost:8082) when the cluster is running.

## Architecture

- **JobManager**: Coordinates job execution, exposed on port 8082 (Web UI)
- **TaskManager**: Executes tasks, configured with 2 task slots and 1728m memory
- Application code from `../flink_applications/flink_applications` is mounted at `/opt/flink-apps`

## CDC job: source format and sink

`flink_applications/cdc_impressions.py` reads the Debezium topic and writes
per-page_type windowed counts. Both ends are configurable through environment
variables that `deploy.sh submit` forwards into `flink run`:

| variable | default | meaning |
| --- | --- | --- |
| `CDC_FORMAT` | `avro-confluent` | Source value format. `avro-confluent` reads Confluent-wire Avro and resolves writer schemas from `SCHEMA_REGISTRY_URL`; `json` matches the JSON-converter connector (`connectors/postgres-source-json.json`). |
| `SCHEMA_REGISTRY_URL` | `http://debezium-schema-registry:8081` | Confluent Schema Registry (see `debezium_deployment`). |
| `CDC_SINK` | `upsert-kafka` | `upsert-kafka` writes to `CDC_COUNTS_TOPIC` keyed by `(page_type, window_start, window_end)`; `print` streams the changelog to the client for debugging. |
| `CDC_COUNTS_TOPIC` | `cdc.impressions.page_type_counts` | Output topic. |

```bash
./deploy.sh up local                          # joins debezium-local-network
./deploy.sh submit cdc_impressions.py local   # avro-confluent -> upsert-kafka
CDC_SINK=print ./deploy.sh submit cdc_impressions.py local
./deploy.sh jobs
../debezium_deployment/deploy.sh consume cdc.impressions.page_type_counts local
```

The image installs `flink-sql-connector-kafka` and
`flink-sql-avro-confluent-registry` into `/opt/flink/lib` (the base image ships
neither; without them a `'connector' = 'kafka'` table fails at plan time). Jar
coordinates and sha1 checks are in the `Dockerfile`.

Why the sink is `upsert-kafka` and how the job survives a source
`ALTER TABLE` are written up in `debezium_deployment/SCHEMA_EVOLUTION.md`.
