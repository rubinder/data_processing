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
