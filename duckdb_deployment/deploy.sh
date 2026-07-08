#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|load-data|query}"
    echo ""
    echo "Commands:"
    echo "  up          Build and start the embedded-DuckDB FastAPI service"
    echo "  down        Stop and remove the container"
    echo "  restart     Restart the service"
    echo "  status      Show container status"
    echo "  logs        Tail container logs"
    echo "  load-data   Load data from the web server API into DuckDB"
    echo "              Options: --page_type N --date YYYY-MM-DD --hour H"
    echo "  query NAME  Query a running endpoint via curl. NAME is one of:"
    echo "              funnel | page-type-summary | user-engagement |"
    echo "              hourly-traffic | health"
    exit 1
}

APP_URL="${DUCKDB_APP_URL:-http://localhost:8100}"

case "${1:-}" in
    up)
        echo "Building and starting the DuckDB service..."
        docker compose up -d --build duckdb-app
        echo "DuckDB analytics service available at $APP_URL"
        echo ""
        echo "Next steps:"
        echo "  1. Start the web server: cd ../web_server_local && ./deploy.sh up"
        echo "  2. Load data: $0 load-data --page_type 1 --date 2026-01-01 --hour 10"
        echo "  3. Query:     $0 query funnel"
        ;;
    down)
        echo "Stopping the DuckDB service..."
        docker compose down
        ;;
    restart)
        echo "Restarting the DuckDB service..."
        docker compose down
        docker compose up -d --build duckdb-app
        echo "DuckDB service restarted."
        ;;
    status)
        docker compose ps
        ;;
    logs)
        docker compose logs -f
        ;;
    load-data)
        shift
        # Parse --page_type / --date / --hour into POST /load query params.
        page_type=""; date=""; hour=""
        while [ $# -gt 0 ]; do
            case "$1" in
                --page_type) page_type="$2"; shift 2;;
                --date) date="$2"; shift 2;;
                --hour) hour="$2"; shift 2;;
                *) echo "Unknown option: $1"; usage;;
            esac
        done
        if [ -z "$page_type" ] || [ -z "$date" ] || [ -z "$hour" ]; then
            echo "Error: --page_type, --date and --hour are all required"
            exit 1
        fi
        echo "Loading data via $APP_URL/load ..."
        curl -sS -X POST \
            "$APP_URL/load?page_type=$page_type&date=$date&hour=$hour"
        echo ""
        ;;
    query)
        shift
        name="${1:-}"
        if [ -z "$name" ]; then
            echo "Error: query requires an endpoint name"
            usage
        fi
        echo "GET $APP_URL/$name"
        curl -sS "$APP_URL/$name"
        echo ""
        ;;
    *)
        usage
        ;;
esac
