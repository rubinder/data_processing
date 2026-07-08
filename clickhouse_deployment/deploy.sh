#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|schema|load-data|query}"
    echo ""
    echo "Commands:"
    echo "  up          Start clickhouse-keeper and the two ClickHouse nodes"
    echo "  down        Stop and remove containers"
    echo "  restart     Restart all services"
    echo "  status      Show container status"
    echo "  logs        Tail container logs"
    echo "  schema      Apply init/schema.sql (creates the distributed tables)"
    echo "  load-data   Load data from the web server API into ClickHouse"
    echo "              Options: --all or --page_type N --date YYYY-MM-DD --hour H"
    echo "  query NAME  Run queries/NAME.sql (e.g. funnel_analysis) on the cluster"
    exit 1
}

wait_for_clickhouse() {
    echo "Waiting for ClickHouse nodes to be ready..."
    for i in $(seq 1 30); do
        if docker compose exec -T clickhouse-01 clickhouse-client --query "SELECT 1" \
                >/dev/null 2>&1 && \
           docker compose exec -T clickhouse-02 clickhouse-client --query "SELECT 1" \
                >/dev/null 2>&1; then
            echo "ClickHouse cluster is ready."
            return 0
        fi
        sleep 2
    done
    echo "Timed out waiting for ClickHouse."
    exit 1
}

case "${1:-}" in
    up)
        echo "Starting ClickHouse cluster (keeper + 2 shards)..."
        docker compose up -d
        wait_for_clickhouse
        echo ""
        echo "ClickHouse cluster is up:"
        echo "  node-01: HTTP localhost:8123  native localhost:9000"
        echo "  node-02: HTTP localhost:8124  native localhost:9001"
        echo ""
        echo "Next steps:"
        echo "  1. Apply the schema: $0 schema"
        echo "  2. Start the web server: cd ../web_server_local && ./deploy.sh up"
        echo "  3. Load data: $0 load-data --all"
        echo "  4. Run a query: $0 query funnel_analysis"
        ;;
    down)
        echo "Stopping ClickHouse cluster..."
        docker compose down
        ;;
    restart)
        echo "Restarting ClickHouse cluster..."
        docker compose down
        docker compose up -d
        wait_for_clickhouse
        echo "ClickHouse cluster restarted."
        ;;
    status)
        docker compose ps
        ;;
    logs)
        docker compose logs -f
        ;;
    schema)
        echo "Applying init/schema.sql on cluster impressions_cluster..."
        docker compose exec -T clickhouse-01 clickhouse-client --multiquery \
            < "$SCRIPT_DIR/init/schema.sql"
        echo "Schema applied."
        ;;
    load-data)
        shift
        echo "Loading data from web server API into ClickHouse..."
        if command -v python3 &>/dev/null; then
            pip install --quiet clickhouse-connect requests 2>/dev/null || true
            python3 "$SCRIPT_DIR/load_data.py" "$@"
        else
            echo "Error: python3 is required to load data"
            exit 1
        fi
        ;;
    query)
        name="${2:-}"
        if [ -z "$name" ] || [ ! -f "$SCRIPT_DIR/queries/$name.sql" ]; then
            echo "Error: provide a query name matching queries/<name>.sql"
            echo "Available:"
            ls "$SCRIPT_DIR/queries" | sed 's/\.sql$//' | sed 's/^/  /'
            exit 1
        fi
        echo "Running query '$name' on the distributed impressions table..."
        sed 's/{table}/impressions/g' "$SCRIPT_DIR/queries/$name.sql" | \
            docker compose exec -T clickhouse-01 clickhouse-client \
                --multiquery --format PrettyCompact
        ;;
    *)
        usage
        ;;
esac
