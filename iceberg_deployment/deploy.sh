#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    cat <<'EOF'
Usage: ./deploy.sh <command>

Lifecycle (REST catalog on S3/MinIO):
  up          Start the Iceberg REST catalog and MinIO
  down        Stop and remove containers
  status      Show container status
  logs [svc]  Tail logs

Demos (local filesystem catalog -- no Docker required):
  demo        Run the full walkthrough: evolve schema, time travel, upsert, compact
  test        Run the pytest suite against real Iceberg tables

Endpoints once up:
  REST catalog   http://localhost:8181
  MinIO console  http://localhost:9101  (minioadmin / minioadmin)
EOF
    exit 1
}

ensure_venv() {
    if [ ! -x "$SCRIPT_DIR/.venv/bin/python" ]; then
        echo "Creating local virtualenv with uv..."
        uv venv --python 3.10
        uv pip install -e ".[test]"
    fi
}

case "${1:-}" in
    up)
        docker compose up -d
        echo "REST catalog: http://localhost:8181   MinIO: http://localhost:9101"
        echo "Point Spark at it with:"
        echo "  export ICEBERG_CATALOG_TYPE=rest"
        ;;
    down)   docker compose down ;;
    status) docker compose ps ;;
    logs)   shift; docker compose logs -f "$@" ;;
    demo)
        ensure_venv
        "$SCRIPT_DIR/.venv/bin/python" -m iceberg_deployment.demo "${@:2}"
        ;;
    test)
        ensure_venv
        "$SCRIPT_DIR/.venv/bin/python" -m pytest "${@:2}"
        ;;
    *) usage ;;
esac
