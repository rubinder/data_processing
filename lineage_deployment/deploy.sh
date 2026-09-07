#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"
API_URL="http://localhost:5005"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|namespaces|jobs|smoke} [local]"
    echo ""
    echo "Commands:"
    echo "  up          Start Marquez (API + UI + PostgreSQL)"
    echo "  down        Stop and remove the services (keeps the volume)"
    echo "  restart     Restart the services"
    echo "  status      Show service status and API health"
    echo "  logs        Tail logs (optionally pass a service name)"
    echo "  namespaces  List OpenLineage namespaces seen so far"
    echo "  jobs        List jobs in a namespace (default data_processing)"
    echo "  smoke       POST a synthetic START+COMPLETE run to prove the endpoint works"
    echo ""
    echo "Options:"
    echo "  local       Use a standalone network instead of the shared"
    echo "              data-processing-network (no Airflow dependency)"
    echo ""
    echo "Emitters point at http://marquez-api:5000 (inside Docker) or"
    echo "http://localhost:5005 (from the host) via OPENLINEAGE_URL."
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

for arg in "$@"; do
    if [ "$arg" = "local" ]; then
        COMPOSE_CMD="docker compose -f docker-compose.yaml -f docker-compose.local.yaml"
        echo "Running in standalone mode (no data-processing-network required)"
        break
    fi
done

ARGS=()
for arg in "$@"; do
    if [ "$arg" != "local" ]; then
        ARGS+=("$arg")
    fi
done
CMD="${ARGS[0]:-}"

wait_for_api() {
    local count=0
    while [ $count -lt 30 ]; do
        if curl -sf "${API_URL}/api/v1/namespaces" > /dev/null 2>&1; then
            return 0
        fi
        count=$((count + 1))
        sleep 3
    done
    echo "Error: Marquez API did not become ready."
    return 1
}

case "$CMD" in
    up)
        echo "Starting Marquez..."
        ${COMPOSE_CMD} up -d
        echo ""
        echo "  Marquez UI:   http://localhost:3000"
        echo "  Lineage API:  ${API_URL}/api/v1/lineage"
        echo ""
        echo "Point emitters at it with OPENLINEAGE_URL=http://marquez-api:5000"
        echo "(inside Docker) or OPENLINEAGE_URL=${API_URL} (host). See LINEAGE.md."
        ;;
    down)
        ${COMPOSE_CMD} down
        ;;
    restart)
        ${COMPOSE_CMD} down
        ${COMPOSE_CMD} up -d
        ;;
    status)
        ${COMPOSE_CMD} ps
        if curl -sf "${API_URL}/api/v1/namespaces" > /dev/null 2>&1; then
            echo "Marquez API: UP"
        else
            echo "Marquez API: NOT READY"
        fi
        ;;
    logs)
        SERVICE="${ARGS[1]:-}"
        if [ -n "$SERVICE" ]; then
            ${COMPOSE_CMD} logs -f "$SERVICE"
        else
            ${COMPOSE_CMD} logs -f
        fi
        ;;
    namespaces)
        wait_for_api
        curl -s "${API_URL}/api/v1/namespaces" | python3 -m json.tool
        ;;
    jobs)
        NS="${ARGS[1]:-data_processing}"
        wait_for_api
        curl -s "${API_URL}/api/v1/namespaces/${NS}/jobs" | python3 -m json.tool
        ;;
    smoke)
        wait_for_api
        RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
        NOW=$(python3 -c "from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat())")
        for EVENT in START COMPLETE; do
            curl -s -o /dev/null -w "%{http_code} ${EVENT}\n" -X POST \
                -H "Content-Type: application/json" \
                "${API_URL}/api/v1/lineage" \
                -d "{
                  \"eventType\": \"${EVENT}\",
                  \"eventTime\": \"${NOW}\",
                  \"run\": {\"runId\": \"${RUN_ID}\"},
                  \"job\": {\"namespace\": \"data_processing\", \"name\": \"smoke_test\"},
                  \"inputs\": [{\"namespace\": \"file\", \"name\": \"/data/raw/impressions\"}],
                  \"outputs\": [{\"namespace\": \"file\", \"name\": \"/data/processed/impressions\"}],
                  \"producer\": \"lineage_deployment/deploy.sh\",
                  \"schemaURL\": \"https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent\"
                }"
        done
        echo "Open http://localhost:3000 -> namespace data_processing -> job smoke_test"
        ;;
    *)
        usage
        ;;
esac
