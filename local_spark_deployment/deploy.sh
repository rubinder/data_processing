#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs} [local]"
    echo ""
    echo "Commands:"
    echo "  up       Build and start Spark master + worker"
    echo "  down     Stop and remove all Spark services"
    echo "  restart  Restart all Spark services"
    echo "  status   Show service status"
    echo "  logs     Tail logs from all services"
    echo ""
    echo "Options:"
    echo "  local    Use a standalone network instead of the shared"
    echo "           data-processing-network (no Airflow dependency)"
    echo ""
    echo "By default, requires the data-processing-network Docker network"
    echo "(created by airflow_deployment). Pass 'local' to run standalone."
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

# Check for 'local' flag in any argument position
for arg in "$@"; do
    if [ "$arg" = "local" ]; then
        COMPOSE_CMD="docker compose -f docker-compose.yaml -f docker-compose.local.yaml"
        echo "Running in standalone mode (no data-processing-network required)"
        break
    fi
done

# Get the command (first non-'local' argument)
CMD=""
for arg in "$@"; do
    if [ "$arg" != "local" ]; then
        CMD="$arg"
        break
    fi
done

case "$CMD" in
    up)
        echo "Starting local Spark cluster..."
        ${COMPOSE_CMD} up --build -d
        echo "Spark master UI available at http://localhost:8081"
        ;;
    down)
        echo "Stopping local Spark cluster..."
        ${COMPOSE_CMD} down
        echo "Spark cluster stopped."
        ;;
    restart)
        echo "Restarting local Spark cluster..."
        ${COMPOSE_CMD} down
        ${COMPOSE_CMD} up --build -d
        echo "Spark cluster restarted at http://localhost:8081"
        ;;
    status)
        ${COMPOSE_CMD} ps
        ;;
    logs)
        ${COMPOSE_CMD} logs -f
        ;;
    *)
        usage
        ;;
esac
