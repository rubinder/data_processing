#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"
LOCAL=false
# Same external network debezium_deployment uses in its `local` mode.
LOCAL_NETWORK="debezium-local-network"
# Where docker-compose.yaml mounts ../flink_applications/flink_applications.
APPS_ROOT="/opt/flink-apps"
APPS_PKG="${APPS_ROOT}/flink_applications"
# Env vars forwarded from the host into `flink run` when set (read by
# cdc_impressions.py at plan time).
JOB_ENV_VARS="KAFKA_BOOTSTRAP CDC_EVENTS_TOPIC CDC_COUNTS_TOPIC CDC_CONSUMER_GROUP CDC_FORMAT SCHEMA_REGISTRY_URL CDC_SINK CHECKPOINT_INTERVAL_MS"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|submit|jobs|cancel} [args] [local]"
    echo ""
    echo "Commands:"
    echo "  up             Build and start Flink JobManager + TaskManager"
    echo "  down           Stop and remove all Flink services"
    echo "  restart        Restart all Flink services"
    echo "  status         Show service status"
    echo "  logs           Tail logs from all services"
    echo "  submit <job>   Submit a PyFlink job. <job> is a file name under"
    echo "                 flink_applications/ (e.g. cdc_impressions.py) or an"
    echo "                 absolute path inside the container."
    echo "  jobs           List running jobs"
    echo "  cancel <id>    Cancel a job by id"
    echo ""
    echo "Job environment (forwarded to submit when set on the host):"
    echo "  KAFKA_BOOTSTRAP      default debezium-kafka:9092"
    echo "  CDC_EVENTS_TOPIC     default cdc.impressions.events"
    echo "  CDC_COUNTS_TOPIC     default cdc.impressions.page_type_counts (upsert-kafka sink)"
    echo "  CDC_FORMAT           avro-confluent (default) | json"
    echo "  SCHEMA_REGISTRY_URL  default http://debezium-schema-registry:8081"
    echo "  CDC_SINK             upsert-kafka (default) | print (debug; blocks, prints to client)"
    echo ""
    echo "Options:"
    echo "  local    Join ${LOCAL_NETWORK} (debezium_deployment's standalone network)"
    echo "           instead of the shared data-processing-network (no Airflow dependency)"
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
        LOCAL=true
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

# docker-compose.local.yaml declares the network as external so this project
# and debezium_deployment can share it; whichever comes up first creates it.
ensure_local_network() {
    if [ "$LOCAL" = true ]; then
        if ! docker network inspect "$LOCAL_NETWORK" > /dev/null 2>&1; then
            echo "Creating Docker network ${LOCAL_NETWORK}..."
            docker network create "$LOCAL_NETWORK" > /dev/null
        fi
    fi
}

remove_local_network() {
    if [ "$LOCAL" = true ]; then
        # Fails harmlessly while debezium_deployment is still attached.
        docker network rm "$LOCAL_NETWORK" > /dev/null 2>&1 || true
    fi
}

case "$CMD" in
    up)
        ensure_local_network
        echo "Starting local Flink cluster..."
        ${COMPOSE_CMD} up --build -d
        echo "Flink Web UI available at http://localhost:8082"
        ;;
    down)
        echo "Stopping local Flink cluster..."
        ${COMPOSE_CMD} down
        remove_local_network
        echo "Flink cluster stopped."
        ;;
    restart)
        ensure_local_network
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
        JOB="${ARGS[1]:-}"
        if [ -z "$JOB" ]; then
            echo "Error: provide a job, e.g.: $0 submit cdc_impressions.py"
            exit 1
        fi
        case "$JOB" in
            /*) JOB_PATH="$JOB" ;;
            *)  JOB_PATH="${APPS_PKG}/${JOB}" ;;
        esac
        ENV_FLAGS=()
        for VAR in $JOB_ENV_VARS; do
            if [ -n "${!VAR:-}" ]; then
                ENV_FLAGS+=(-e "${VAR}=${!VAR}")
            fi
        done
        echo "Submitting PyFlink job: $JOB_PATH"
        if [ ${#ENV_FLAGS[@]} -gt 0 ]; then
            echo "  with ${ENV_FLAGS[*]}"
        fi
        # --pyFiles puts /opt/flink-apps on the driver's PYTHONPATH so the job
        # can import the flink_applications package it lives in.
        docker exec "${ENV_FLAGS[@]+"${ENV_FLAGS[@]}"}" flink-jobmanager \
            flink run --detached --pyFiles "$APPS_ROOT" -py "$JOB_PATH"
        ;;
    jobs)
        docker exec flink-jobmanager flink list -r
        ;;
    cancel)
        JOB_ID="${ARGS[1]:-}"
        if [ -z "$JOB_ID" ]; then
            echo "Error: provide a job id (see: $0 jobs)"
            exit 1
        fi
        docker exec flink-jobmanager flink cancel "$JOB_ID"
        ;;
    *)
        usage
        ;;
esac
