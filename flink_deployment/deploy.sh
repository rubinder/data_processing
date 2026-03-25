#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|submit} [local]"
    echo ""
    echo "Commands:"
    echo "  up       Build and start Flink JobManager + TaskManager"
    echo "  down     Stop and remove all Flink services"
    echo "  restart  Restart all Flink services"
    echo "  status   Show service status"
    echo "  logs     Tail logs from all services"
    echo "  submit   Submit a PyFlink job (pass job path as next arg)"
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

# Collect non-'local' arguments
ARGS=()
for arg in "$@"; do
    if [ "$arg" != "local" ]; then
        ARGS+=("$arg")
    fi
done

CMD="${ARGS[0]:-}"

case "$CMD" in
    up)
        echo "Starting local Flink cluster..."
        ${COMPOSE_CMD} up --build -d
        echo "Flink Web UI available at http://localhost:8082"
        ;;
    down)
        echo "Stopping local Flink cluster..."
        ${COMPOSE_CMD} down
        echo "Flink cluster stopped."
        ;;
    restart)
        echo "Restarting local Flink cluster..."
        ${COMPOSE_CMD} down
        ${COMPOSE_CMD} up --build -d
        echo "Flink cluster restarted at http://localhost:8082"
        ;;
    status)
        ${COMPOSE_CMD} ps
        ;;
    logs)
        ${COMPOSE_CMD} logs -f
        ;;
    submit)
        JOB_PATH="${ARGS[1]:-}"
        if [ -z "$JOB_PATH" ]; then
            echo "Error: provide a job path, e.g.: $0 submit /opt/flink-apps/hello_world.py"
            exit 1
        fi
        echo "Submitting PyFlink job: $JOB_PATH"
        docker exec flink-jobmanager flink run -py "$JOB_PATH"
        ;;
    *)
        usage
        ;;
esac
