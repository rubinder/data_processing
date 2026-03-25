#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"
CONNECT_URL="http://localhost:8083"

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|register|connectors|topics|consume|exec-sql} [local]"
    echo ""
    echo "Commands:"
    echo "  up          Build and start Zookeeper, Kafka, PostgreSQL, Debezium Connect, Kafka UI"
    echo "  down        Stop and remove all services"
    echo "  restart     Restart all services"
    echo "  status      Show service status"
    echo "  logs        Tail logs from all services (or pass service name)"
    echo "  register    Register the PostgreSQL source connector with Debezium Connect"
    echo "  connectors  List registered connectors and their status"
    echo "  topics      List Kafka topics"
    echo "  consume     Consume messages from a topic (pass topic name as next arg)"
    echo "  exec-sql    Execute a SQL file against the source database (pass file path)"
    echo ""
    echo "Options:"
    echo "  local       Use a standalone network instead of the shared"
    echo "              data-processing-network (no Airflow dependency)"
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

wait_for_connect() {
    echo "Waiting for Debezium Connect to be ready..."
    local retries=30
    local count=0
    while [ $count -lt $retries ]; do
        if curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
            echo "Debezium Connect is ready."
            return 0
        fi
        count=$((count + 1))
        echo "  Attempt $count/$retries - Connect not ready yet..."
        sleep 5
    done
    echo "Error: Debezium Connect did not become ready in time."
    return 1
}

case "$CMD" in
    up)
        echo "Starting Debezium CDC stack..."
        ${COMPOSE_CMD} up --build -d
        echo ""
        echo "Services starting:"
        echo "  Kafka UI:          http://localhost:8084"
        echo "  Debezium Connect:  http://localhost:8083"
        echo "  PostgreSQL:        localhost:5434 (cdc_user/cdc_pass/cdc_source)"
        echo ""
        echo "Next steps:"
        echo "  1. Wait for services to be healthy: $0 status"
        echo "  2. Register the connector: $0 register"
        echo "  3. Apply sample changes: $0 exec-sql sample_changes.sql"
        echo "  4. View topics: $0 topics"
        echo "  5. Consume events: $0 consume cdc.impressions.events"
        ;;
    down)
        echo "Stopping Debezium CDC stack..."
        ${COMPOSE_CMD} down -v
        echo "Debezium CDC stack stopped and volumes removed."
        ;;
    restart)
        echo "Restarting Debezium CDC stack..."
        ${COMPOSE_CMD} down
        ${COMPOSE_CMD} up --build -d
        echo "Debezium CDC stack restarted."
        ;;
    status)
        ${COMPOSE_CMD} ps
        echo ""
        echo "Checking Debezium Connect status..."
        if curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
            echo "Connect API: UP"
            CONNECTORS=$(curl -s "${CONNECT_URL}/connectors")
            echo "Registered connectors: ${CONNECTORS}"
        else
            echo "Connect API: NOT READY (services may still be starting)"
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
    register)
        wait_for_connect
        echo "Registering PostgreSQL source connector..."
        RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d @connectors/postgres-source.json \
            "${CONNECT_URL}/connectors")
        if [ "$RESPONSE" = "201" ]; then
            echo "Connector registered successfully."
            echo "CDC topics will be created as changes occur:"
            echo "  - cdc.impressions.events"
            echo "  - cdc.impressions.users"
            echo "  - cdc.impressions.pull_status"
        elif [ "$RESPONSE" = "409" ]; then
            echo "Connector already exists. Use 'connectors' to check status."
        else
            echo "Failed to register connector (HTTP $RESPONSE)."
            curl -s -X POST \
                -H "Content-Type: application/json" \
                -d @connectors/postgres-source.json \
                "${CONNECT_URL}/connectors" | python3 -m json.tool
            exit 1
        fi
        ;;
    connectors)
        echo "Registered connectors:"
        curl -s "${CONNECT_URL}/connectors" | python3 -m json.tool
        echo ""
        echo "Connector details:"
        for CONN in $(curl -s "${CONNECT_URL}/connectors" | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" 2>/dev/null); do
            echo "--- ${CONN} ---"
            curl -s "${CONNECT_URL}/connectors/${CONN}/status" | python3 -m json.tool
            echo ""
        done
        ;;
    topics)
        echo "Kafka topics:"
        docker exec debezium-kafka kafka-topics --bootstrap-server localhost:9092 --list
        ;;
    consume)
        TOPIC="${ARGS[1]:-}"
        if [ -z "$TOPIC" ]; then
            echo "Error: provide a topic name, e.g.: $0 consume cdc.impressions.events"
            exit 1
        fi
        echo "Consuming from topic: $TOPIC (Ctrl+C to stop)"
        docker exec debezium-kafka kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic "$TOPIC" \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" | "
        ;;
    exec-sql)
        SQL_FILE="${ARGS[1]:-}"
        if [ -z "$SQL_FILE" ]; then
            echo "Error: provide a SQL file path, e.g.: $0 exec-sql sample_changes.sql"
            exit 1
        fi
        if [ ! -f "$SQL_FILE" ]; then
            echo "Error: file not found: $SQL_FILE"
            exit 1
        fi
        echo "Executing SQL file: $SQL_FILE"
        docker exec -i debezium-postgres psql -U cdc_user -d cdc_source < "$SQL_FILE"
        echo "SQL executed. CDC events should appear in Kafka topics shortly."
        ;;
    *)
        usage
        ;;
esac
