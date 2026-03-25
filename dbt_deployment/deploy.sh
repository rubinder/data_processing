#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|run|seed|test|load-data}"
    echo ""
    echo "Commands:"
    echo "  up          Build and start PostgreSQL and dbt containers"
    echo "  down        Stop and remove containers"
    echo "  restart     Restart all services"
    echo "  status      Show container status"
    echo "  logs        Tail container logs"
    echo "  run         Execute dbt run (build models)"
    echo "  seed        Execute dbt seed (load seed data)"
    echo "  test        Execute dbt test (run tests)"
    echo "  load-data   Load data from web server API into PostgreSQL"
    echo "              Options: --all or --page_type N --date YYYY-MM-DD --hour H"
    exit 1
}

case "${1:-}" in
    up)
        echo "Building and starting dbt services..."
        docker compose up -d dbt-postgres
        echo "Waiting for PostgreSQL to be ready..."
        sleep 5
        docker compose build dbt
        echo "dbt services are up. PostgreSQL available at localhost:5433"
        echo ""
        echo "Next steps:"
        echo "  1. Start the web server: cd ../web_server_local && ./deploy.sh up"
        echo "  2. Load data: $0 load-data --all"
        echo "  3. Run dbt: $0 run"
        ;;
    down)
        echo "Stopping dbt services..."
        docker compose down
        ;;
    restart)
        echo "Restarting dbt services..."
        docker compose down
        docker compose up -d dbt-postgres
        sleep 5
        docker compose build dbt
        echo "dbt services restarted."
        ;;
    status)
        docker compose ps
        ;;
    logs)
        docker compose logs -f
        ;;
    run)
        echo "Running dbt models..."
        docker compose run --rm dbt sh -c "dbt deps && dbt run"
        ;;
    seed)
        echo "Loading dbt seeds..."
        docker compose run --rm dbt dbt seed
        ;;
    test)
        echo "Running dbt tests..."
        docker compose run --rm dbt dbt test
        ;;
    load-data)
        shift
        echo "Loading data from web server API..."
        if command -v python3 &>/dev/null; then
            pip install --quiet psycopg2-binary requests 2>/dev/null || true
            python3 "$SCRIPT_DIR/load_data.py" "$@"
        else
            echo "Error: python3 is required to load data"
            exit 1
        fi
        ;;
    *)
        usage
        ;;
esac
