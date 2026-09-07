#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    echo "Usage: $0 {up|down|restart|status|logs|migrate|run|seed|test|source-freshness|docs|load-data}"
    echo ""
    echo "Commands:"
    echo "  up                Build and start PostgreSQL and dbt containers (applies migrations)"
    echo "  down              Stop and remove containers"
    echo "  restart           Restart all services"
    echo "  status            Show container status"
    echo "  logs              Tail container logs"
    echo "  migrate           Apply migrations/*.sql to the raw schema (idempotent)"
    echo "  run               Execute dbt run (build models; contracts enforced)"
    echo "  seed              Execute dbt seed (load seed data)"
    echo "  test              Execute dbt test (run tests)"
    echo "  source-freshness  Execute dbt source freshness (raw.impressions.loaded_at SLA)"
    echo "  docs              Generate and serve dbt docs at http://localhost:8580"
    echo "  load-data         Load data from web server API into PostgreSQL"
    echo "                    Options: --all or --page_type N --date YYYY-MM-DD --hour H"
    echo ""
    echo "Lineage: when OPENLINEAGE_URL is set (e.g. http://marquez-api:5000, see"
    echo "  ../lineage_deployment) dbt commands run through the dbt-ol wrapper and"
    echo "  emit OpenLineage events for every model, source and test."
    exit 1
}

# dbt-ol (openlineage-dbt) wraps dbt and posts run events + the manifest
# lineage to OPENLINEAGE_URL. Without the URL the plain dbt binary is used,
# so lineage is strictly opt-in and needs no running Marquez.
DBT_BIN="dbt"
LINEAGE_ENV=()
if [ -n "${OPENLINEAGE_URL:-}" ]; then
    DBT_BIN="dbt-ol"
    LINEAGE_ENV=(-e "OPENLINEAGE_URL=${OPENLINEAGE_URL}"
                 -e "OPENLINEAGE_NAMESPACE=${OPENLINEAGE_NAMESPACE:-data_processing}")
fi

run_dbt() {
    # Runs a dbt (or dbt-ol) command line inside the dbt container.
    docker compose run --rm "${LINEAGE_ENV[@]+"${LINEAGE_ENV[@]}"}" --entrypoint "" dbt \
        sh -c "$DBT_BIN deps && $DBT_BIN $*"
}

apply_migrations() {
    # init_db.sql only runs on a fresh volume; migrations bring existing
    # databases up to the current raw schema. Each file must be idempotent.
    for f in "$SCRIPT_DIR"/migrations/*.sql; do
        [ -e "$f" ] || continue
        echo "Applying $(basename "$f")..."
        docker compose exec -T dbt-postgres psql -q -U dbt -d data_processing < "$f"
    done
}

case "${1:-}" in
    up)
        echo "Building and starting dbt services..."
        docker compose up -d dbt-postgres
        echo "Waiting for PostgreSQL to be ready..."
        sleep 5
        docker compose build dbt
        apply_migrations
        echo "dbt services are up. PostgreSQL available at localhost:5433"
        echo ""
        echo "Next steps:"
        echo "  1. Start the web server: cd ../web_server_local && ./deploy.sh up"
        echo "  2. Load data: $0 load-data --all"
        echo "  3. Run dbt: $0 run"
        echo "  4. Check freshness: $0 source-freshness"
        ;;
    migrate)
        apply_migrations
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
        run_dbt run
        ;;
    seed)
        echo "Loading dbt seeds..."
        run_dbt seed
        ;;
    test)
        echo "Running dbt tests..."
        run_dbt test
        ;;
    source-freshness)
        echo "Checking source freshness (raw.impressions.loaded_at)..."
        run_dbt source freshness
        ;;
    docs)
        echo "Generating and serving dbt docs at http://localhost:8580 ..."
        echo "Press Ctrl+C to stop the docs server."
        docker compose run --rm --entrypoint "" -p 8580:8580 dbt sh -c "dbt deps && dbt docs generate && dbt docs serve --port 8580 --host 0.0.0.0 --no-browser"
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
