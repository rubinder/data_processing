#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  up       Build and start all Airflow services"
    echo "  down     Stop and remove all services"
    echo "  restart  Restart all services"
    echo "  status   Show service status"
    echo "  logs     Tail logs from all services"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

case "$1" in
    up)
        echo "Starting Airflow deployment..."
        docker compose up --build -d
        echo "Airflow is starting at http://localhost:8080"
        ;;
    down)
        echo "Stopping Airflow deployment..."
        docker compose down
        echo "Airflow stopped."
        ;;
    restart)
        echo "Restarting Airflow deployment..."
        docker compose down
        docker compose up --build -d
        echo "Airflow restarted at http://localhost:8080"
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
