#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  up       Build and start the web server"
    echo "  down     Stop and remove the web server"
    echo "  restart  Restart the web server"
    echo "  status   Show service status"
    echo "  logs     Tail logs from the web server"
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
        echo "Starting web server..."
        docker compose up --build -d
        echo "Web server available at http://localhost:8000"
        echo "API docs at http://localhost:8000/docs"
        ;;
    down)
        echo "Stopping web server..."
        docker compose down
        echo "Web server stopped."
        ;;
    restart)
        echo "Restarting web server..."
        docker compose down
        docker compose up --build -d
        echo "Web server restarted at http://localhost:8000"
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
