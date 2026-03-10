#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  up       Build and start Spark master + worker"
    echo "  down     Stop and remove all Spark services"
    echo "  restart  Restart all Spark services"
    echo "  status   Show service status"
    echo "  logs     Tail logs from all services"
    echo ""
    echo "Requires the data-processing-network Docker network"
    echo "(created by airflow_deployment)."
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

case "$1" in
    up)
        echo "Starting local Spark cluster..."
        docker compose up --build -d
        echo "Spark master UI available at http://localhost:8081"
        ;;
    down)
        echo "Stopping local Spark cluster..."
        docker compose down
        echo "Spark cluster stopped."
        ;;
    restart)
        echo "Restarting local Spark cluster..."
        docker compose down
        docker compose up --build -d
        echo "Spark cluster restarted at http://localhost:8081"
        ;;
    status)
        docker compose ps
        ;;
    logs)
        docker compose logs -f
        ;;
    *)
        usage
        ;;
esac
