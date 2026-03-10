# Web Server Local

Deploys the FastAPI web server (from `web_server_code`) locally using Docker.

## Architecture

- **Dockerfile**: Builds a Python 3.10-slim image, installs uv, copies the web server code, and runs uvicorn on port 8000.
- **docker-compose.yaml**: Runs the web server container, exposing port 8000 with a health check against `/docs`.

## Prerequisites

- Docker and Docker Compose
- The `data-processing-network` Docker network must exist (created by running `airflow_deployment/deploy.sh up` first)

## How to Run

```bash
# Start the web server
./deploy.sh up

# Stop the web server
./deploy.sh down

# Restart the web server
./deploy.sh restart

# Check service status
./deploy.sh status

# Tail logs
./deploy.sh logs
```

Once running:
- Web server available at http://localhost:8000
- API docs at http://localhost:8000/docs

## Network

Connects to the shared `data-processing-network` Docker network, allowing Spark applications and Airflow to access the web server at `http://web-server:8000`.
